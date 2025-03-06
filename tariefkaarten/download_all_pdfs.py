# example command: python download_all_pdfs.py [url] [path to folder]

import os
import urllib.request
import re
import sys

pathName = "~/Desktop/pdfs/"

if len(sys.argv) == 1:
    url = "https://uwaterloo.ca/scholar/ahilal/classes/ece-457a-cooperative-and-adaptive-algorithms/materials/schedule"
elif len(sys.argv) == 2:
    url = sys.argv[1]
else:
    url = sys.argv[1]
    pathName = sys.argv[2]

path = os.path.expanduser(pathName)

print("Downloading PDFs from " + url + " and saving to " + pathName)

if not os.path.exists(path):
    os.makedirs(path)

with urllib.request.urlopen(url) as website:
    html = website.read().decode("utf-8")

links = re.findall(r'"((http|ftp)s?://.*?)"', html)
pdflinks = []

for link in links:
    if link[0].endswith(".pdf") and link[0] not in pdflinks:
        pdflinks.append(link[0])
        fileName = os.path.basename(link[0])
        print("Downloading " + fileName + "...")
        filePath = os.path.join(path, fileName)
        urllib.request.urlretrieve(link[0], filePath)
