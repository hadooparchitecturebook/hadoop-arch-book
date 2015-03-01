To run this example:

1. First run "python src/datagen.py" this will generate two data files: foo_file and bar_file, corresponding to the "foo" and "bar" tables in the example.

2. Create 2 directories in HDFS one for the "foo" data set and the other for "bar". Place the generated files in the corresponding directories.
I created /user/gshapira/ctest/foo and /user/gshapira/ctest/bar

3. Build the jar for this project using "mvn clean package" the result should be target/examples.cascading-0.0.1-SNAPSHOT.jar

4. In the hadoop cluster, execute the jar:
hadoop jar examples.cascading-0.0.1-SNAPSHOT.jar <foo directory> <bar directory> <output directory> <lower limit on foo value> <lower limit on foo + bar value> <number of reducers>
I used:
hadoop jar examples.cascading-0.0.1-SNAPSHOT.jar ctest/foo ctest/bar ctest/output2 50 150 2