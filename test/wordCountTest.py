import re
from collections import defaultdict
from os import listdir
from os.path import isfile, join

def test(inputfilepath,outputpath):
    expected = getExpected(inputfilepath)
    actual = getActual(outputpath)
    if len(expected.keys()) == len(actual.keys()):
        for key in expected.keys():
            if (key not in actual.keys()) or (actual[key] != expected[key]):
                print("Word Count Testcase Failed")
                return
        print("Word Count Testcase Passed")
    else:
        print("Word Count Testcase Failed")

def getExpected(inputfilepath):
    f = open(inputfilepath)
    data = f.read()
    f.close()
    data = data.lower()
    data = re.sub(r'[^\w\s]', '', data)
    words = data.split(" ")
    wordcount = defaultdict(int)
    for word in words:
        word = word.strip()
        if len(word)>0:
            wordcount[word] += 1
    return wordcount

def getActual(outputdir):
    files = listdir(outputdir)
    wordcount = defaultdict(int)
    for file in files:
        if isfile(join(outputdir, file)):
            f = open(join(outputdir, file), 'r')
            data = f.read()
            f.close()
            for line in data.split("\n"):
                line = line.strip()
                if len(line)>0:
                    word, count = line.split(" ")
                    wordcount[word] += int(count)
    return wordcount

