from os import listdir
from os.path import isfile, join
import re

def test(inputfilepath,outputpath, pattern):
    expected = getExpected(inputfilepath, pattern)
    actual = getActual(outputpath)
    if expected == actual:
        print("Distributed Grep Testcase Passed")
    else:
        print("Distributed Grep Testcase Failed")



def getExpected(inputfilepath, pattern):
    f = open(inputfilepath, 'r',encoding='utf-8')
    data = f.read()
    f.close()
    data = data.lower()
    return len(re.findall(pattern,data))

def getActual(outpudir):
    files = listdir(outpudir)
    count = 0
    for file in files:
        if isfile(join(outpudir, file)):
            f = open(join(outpudir, file))
            data = f.read()
            f.close()
            matched = int(data.split(" ")[1])
            count += matched
    return count