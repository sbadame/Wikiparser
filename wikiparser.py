#!/usr/bin/env python

# Does the work of turning wikipedia into a bunch of files
# This script takes a loooong while to run, but it gets the job done and 
# appears to be mostly I/O bound so don't blame python :)

import xml.parsers.expat
import unicodedata
import os.path
import signal
import threading
import thread
import time
import sys
from UserString import MutableString
from BeautifulSoup import BeautifulSoup

OUTPUT_DIRECTORY = "wikipedia"
ARTICLE_COUNT = 2000
storedata = False
currentArticle = MutableString()
filedata = []
stop = False
outputlock = thread.allocate_lock()
p = None
entrycount = 0

# Every xml tag is an element.
# If the xml tag's name (the name argument) is "title"
# then we are starting a new article and we can expect
# a text element to come next that contains the data
def startelement(name, attrs):
    global storedata
    storedata = (name == "text")
    if storedata:
        global entrycount, filedata, currentArticle
        entrycount += 1
        filetext = asciify(unicode(currentArticle))
        if filetext:
            filedata.append(filetext)
        currentArticle = MutableString()

# Start parsing the data that we are getting
def getdata(text):
    if storedata:
        text = asciify(text)
        if text and not text.startswith("#REDIRECT"):
            global currentArticle
            currentArticle += text
            if stop or entrycount >= ARTICLE_COUNT: #2 entries per article
                writeout()

# Removing all of the wiki syntax garbage that we don't care about
# Syntax grabbed from: http://en.wikipedia.org/wiki/Help:Wiki_markup
def removesyntax(text):
   text = ''.join(BeautifulSoup("<html><body>" + text + "</body></html>").findAll(text=True))
   import re
   text = re.sub("\{\{(.+?)\}\}", "", text) #usually metadata
   text = re.sub("\[\[[^\[]+\|([^\]]+)\]\]", r'\1', text) #links, save the anchor text
   text = re.sub("\[\[([^\]]+)\]\]", r'\1', text) #links, save the anchor text
   text = text.replace("====","") # Bolded words
   text = text.replace("===","") # Bolded words
   text = text.replace("==","") # Bolded words
   text = text.replace("\'\'\'","") # Bolded words
   text = text.replace("\'\'", "") # Italic words
   text = text.replace("----", "") # Horizontal line
   text = re.sub(r"&[a-z]+;", "", text) #HTML garbage
   return text

# Handling converting the unicode data into ascii
def asciify(text):
    text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore')
    return removesyntax(text).replace("\n","").replace("\r","").strip()

# Actually do the IO of writing out the file to new a place
filecount = 0
def writeout():
    global filedata, entrycount, filecount
    filename = "%s/%d.txt" % (OUTPUT_DIRECTORY, filecount)
    file = open(filename, "w")
    file.write("\n".join(filedata))
    file.close()
    filecount += 1
    with outputlock:
        print "\rNew file: " + filename
    filedata = []
    entrycount = 0
    if stop:
        exit()

#Signal handling
def gotsignal(signum, stack):
    global stop
    stop = True
signal.signal(signal.SIGINT, gotsignal)

#Show the progress every 5 seconds as we make our way through
def showprogress():
    while p == None: time.sleep(0.1)
    while not(stop):
        with outputlock:
            print "%5.2f Complete\r" % (100*(p.CurrentByteIndex/float(30565654976))),
        sys.stdout.flush()
        time.sleep(5)

threading.Thread(target=showprogress,name="progress thread").start()

from datetime import datetime
print("Started at: %s" % datetime.now())
p = xml.parsers.expat.ParserCreate()
p.StartElementHandler = startelement
p.CharacterDataHandler = getdata
p.ParseFile(open("/home/sandro/Documents/enwiki-latest-pages-articles.xml", "rb"))
print("Finished at: %s" % datetime.now())
