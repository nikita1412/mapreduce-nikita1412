import distributedGrepTest
import wordCountTest
import  URLFrequencyTest
import json
f = open("wordcount.json", 'r')
worcountconfig = f.read()
f.close()
worcountconfig = json.loads(worcountconfig)
wordCountTest.test(worcountconfig["inputfile"], worcountconfig["outputdir"])

f = open("distributedgrep.json", 'r')
distributedgrepconfig = f.read()
f.close()
distributedgrepconfig = json.loads(distributedgrepconfig)
distributedGrepTest.test(distributedgrepconfig["inputfile"], distributedgrepconfig["outputdir"], "distributed")

f = open("urlfrequency.json", 'r')
urlfrequencyconfig = f.read()
f.close()
urlfrequencyconfig = json.loads(urlfrequencyconfig)
URLFrequencyTest.test(urlfrequencyconfig["inputfile"], urlfrequencyconfig["outputdir"])