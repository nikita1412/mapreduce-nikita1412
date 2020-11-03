# MapReduce Implementation

The current implementation supports mutliple workers for MapReduce with fault tolerance. The test cases which it supports are:

  - Word Count: Count the words in the given text file
  - Distributed Grep: Find a specific pattern in the text file
  - URL Frequency count: Count the URL access frequency

### System Specifications

* Windows - 64 bit system
* JAVA (JDK 14)
* JAVA_HOME: path variable set in the environment variables.
* The path of the JDK should be added in the gradle.properties file
* Gradle 6.7 - the environment path variable also needs to be set (link in design doc)

### How to run the tests?

Run the createjar.bat file. This will build the project using gradle, run it and then run all the tests (defined in the "test" folder). If the tests are passed/failed, a message will be displayed in the CLI that the respective test case is passed/failed.

The input, output and intermediate file locations are specified in the config files, which are wordcount.json, distributedgrep.json and urlfrequency.json respectively. The reason for having separate config files is because the outputs are dumped into different output folders, since we are running multiple tests and not just one and the output of all tests need to be displayed.

If you want to give an input file other than the one provided in the project directory, then please change the inputfile path in the respective config file.

We faced some issues with different versions of Java. We have used jdk 14.0.2. If there are any issues faced with building gradle, the jdk file can be downloaded using the link provided in the design doc.

## How the tests work:
The tests are started by running startTests.py present in the *test* folder.

Note: The tests need to be run from the root directory of the project, and not from the *test* folder, since the config files are present in the root directory.

This file then runs *wordCountTest.py*, *distributedGrepTest.py* and *URLFrequencyTest.py*. Both of these test files take the output of our MapReduce program as "actual output" and the actual results that are used for verification as "expected output ". These expected results are computed by the respective test python files. In this way, we are checking whether our distributed implementation of the tasks returns the same results as a non-distributed, single process implementation of the task.

### Architecture
We have implemented a MapReduce interface which has the following function declarations:

* The *mapper* function of each worker takes in a row from the partition of the data that is assigned to that worker and gives out a list of key value pairs, which are then written to intermediate files
* The *reducer* function takes in the values from the intermediate file as list for a particular key and gives out the final result in string format
* The *getResourceSeparator* function is used to obtain the separator that is used to separate rows in the input for each test case

The execution is started with the creation of a master process. The master refers to the config file that is provided, and takes in the desired locations for the input file, output file and intermediate file, along with the value of N which defines the number of workers. The master then takes an object of the test case, which implements the MapReduce interface that our library defines, serializes that object and places it in a file. The worker processes are then created with unique IDs. A socket/port is initialized to facilitate communication between the master and all the workers, and the master constantly listens for messages from the workers. The file containing the serialized object, the communication port, and the file paths are passed to all of the workers. The workers are all initialised to perform the map phase first, and a global barrier is implemented. In this phase, the input files are taken as input and the intermediate files are considered as output files. The workers access the serialized object in read-only mode, deserialize it and then access the user-defined map function using that object. This is followed by the execution of the task specified in the UDF. Upon completion, each worker sends a message to the master on the designated port stating that it has completed its task. The master acknowledges, and maintains a tally to ensure global synchronicity. The first phase ends once all the workers have sent a message to the master stating their tasks are completed.

The second phase is then started, and workers are created and initialised for the reduce phase. The same process described above is followed again, with the workers acting as reducers now. The intermediate files are considered as the input files for this phase, and the output files are the final output files.



License
----
MIT
