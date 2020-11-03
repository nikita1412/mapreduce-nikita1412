from os import listdir
from os.path import isfile, join
from collections import Counter, defaultdict

def test(inputfilepath, outputpath):
    expected = getExpected(inputfilepath)
    actual = getActual(outputpath)
    if len(expected.keys()) == len(actual.keys()):
        for key in expected.keys():
            if (key not in actual.keys()) or (actual[key] != expected[key]):
                print("URL Frequency Testcase Failed")
                # return
        print("URL Frequency Testcase Passed")
    else:
        print("URL Frequency Testcase Failed")



def getExpected(inputfilepath):
    f = open(inputfilepath, 'r',encoding='utf-8')
    data = f.read()
    f.close()
    data = data.split("\n")
    return Counter(data)

def getActual(outputdir):
    files = listdir(outputdir)
    urls = defaultdict(int)
    for file in files:
        if isfile(join(outputdir, file)):
            f = open(join(outputdir, file), 'r')
            data = f.read()
            f.close()
            for line in data.split("\n"):
                line = line.strip()
                if len(line)>0:
                    url, count = line.split(" ")
                    urls[url] += int(count)
    return urls