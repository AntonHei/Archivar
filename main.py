import internetarchive as ia
import os
import threading
import re

# Credentials
import internetarchive.exceptions
import requests.exceptions

#
# Archive.org Credentials
#

ia_username = ""
ia_password = ""

#
# Directory Related
#

book_ia_directory_prefix = "books/"

#
# Search Related
#

essential_query_ia = " AND mediatype:(texts) AND -collection:(inlibrary)"  # inlibrary is the biggest collection for book borrowing

#
# Other
#

maxFilesizeMB = 35

# Which Format to download
formatsToScrape = [
    ".pdf"
]

# Which language to scrape for
languagesToScrape = [
    "English"
]

# Titles to Scrape for; Every Title Search will get its own thread
titlesToScrape = [
    "Subject",
]

#
# Runtime
#

ia_session = None


def search(search_query, fields=[], maxPerPage=1, page=1):
    languagesJoin = ') OR languageSorter:('.join(languagesToScrape)
    languageQuery = " AND (languageSorter:(" + languagesJoin + "))"

    query = search_query + essential_query_ia + languageQuery
    log("Searching Query: \"" + query + "\"")
    results = ia.search_items(query, fields, sorts=["-week"], params=dict(page=page, rows=maxPerPage))
    return results


def getItem(identifier):
    result = ia.get_item(identifier)
    return result


def prepare():
    global ia_session

    log("Setting up Internet Archive Interface")
    # Internet Archive Credentials
    try:
        ia.configure(ia_username, ia_password)
    except internetarchive.exceptions.AuthenticationError:
        log(message="Could not verify Internet Archive Login: Check Credentials", lprefix="ERROR")

    ia_session = internetarchive.get_session()
    ia_session.mount_http_adapter()


def log(message, fprefix="Archivar", lprefix=""):
    if lprefix == "":
        print("[" + fprefix + "]" + " " + message)
    else:
        print("[" + fprefix + "]" + " " + "[" + lprefix + "]" + " " + message)


def getFormatsToScrape(files):
    targetFileFormats = []

    for file in files:

        fileName = file["name"]
        fileSizeMB = None
        if "size" in file:
            fileFormat = file["format"]
            fileSize = file["size"]
            fileSizeMB = round(float(fileSize) / (1024 ** 2), 2)
        else:
            continue

        for formatToScrape in formatsToScrape:
            if fileName.lower().endswith(formatToScrape):
                if fileSizeMB <= maxFilesizeMB:
                    targetFileFormats.append(fileFormat)
                    log("Found File: " + fileFormat + " [Filesize: " + str(fileSizeMB) + "MB]")
                else:
                    log("Found File: " + fileFormat + " [Filesize: " + str(fileSizeMB) + "MB] - Filesize over: " + str(
                        maxFilesizeMB) + "MB; Continuing")

    return targetFileFormats


def safeStrip(string, divider=""):
    string = string.strip().replace(" ", divider).replace(":", "").replace(".", "").replace(",", "").replace("'",
                                                                                                             "").replace(
        "\"", "")
    string = string[:255]  # Truncate Long Names
    string = re.sub(r"[^a-zA-Z0-9-_]+", '', string)
    return string


def downloadResults(results):
    for result in results:
        log("Found: " + result["title"] + "[" + result["identifier"] + "]")

        item = getItem(result["identifier"])
        safeTitle = safeStrip(item.item_metadata["metadata"]["title"], "_")

        collections = item.item_metadata["metadata"]["collection"]
        collectionsString = str(collections) + "/"
        if not isinstance(collections, str):
            collectionsString = ""
            for collection in collections:
                collectionsString += str(collection) + "/"

        targetDir = book_ia_directory_prefix + safeTitle + "/"

        if os.path.exists(targetDir) is True:
            log("Book already scraped; Continuing")
            continue
        else:
            needDir = True

        files = item.item_metadata["files"]
        targetFileFormats = getFormatsToScrape(files)

        if len(targetFileFormats) <= 0:
            log("No Files found")
            continue

        if needDir is True:
            os.makedirs(targetDir, exist_ok=True)

        log("Now downloading [TargetDir: \"" + targetDir + "\", Formats: \"" + str(targetFileFormats) + "\"]")

        try:
            download = item.download(verbose=True, no_directory=True, destdir=targetDir, formats=targetFileFormats)
        except NotADirectoryError:
            targetDir = book_ia_directory_prefix + result["identifier"] + "/"
            download = item.download(verbose=True, no_directory=True, destdir=targetDir, formats=targetFileFormats)
        except requests.exceptions.HTTPError:
            log("HTTP Error occured while trying to download: " + result["identifier"])


def startScrape(query, threadIndex, dryRun = False):
    log("[Thread: " + str(threadIndex) + "] Started Scraping")

    for run in range(10):
        log("[Thread: " + str(threadIndex) + "] Starting Run " + str(run))

        count = 50
        page = 1 + run
        results = search(query, ["title", "identifier"], count, page)

        if dryRun == True:
            for result in results:
                log("Found: " + result["title"] + "[" + result["identifier"] + "]")
        else:
            downloadResults(results)

        log("[Thread: " + str(threadIndex) + "] Run " + str(run) + " complete")


def getSearchQuery(value, field="title", exclude=False):
    if exclude is False:
        return field + ":(" + value + ")"
    else:
        return "-" + field + ":(" + value + ")"


if __name__ == '__main__':
    needDir = False

    log("Initiating Startup Procedure")
    prepare()

    threads = []
    for threadIndex in range(len(titlesToScrape)):
        curTitle = titlesToScrape[threadIndex]
        query = getSearchQuery(curTitle)
        log("[Thread: " + str(threadIndex) + "] Searching Title: " + curTitle)
        curThread = threading.Thread(target=startScrape, args=[query, threadIndex, False])
        threads.append(curThread)

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()
