This project is a Corpus Calculator which is used to calculate the probability of sentences from a sample of corpus.
Further details of the implementation can be found in the Report along with the problem statement in HW2.pdf 

Details on setting up and running the MapReduce jobs on the pseudo-distributed node on a local system have been discuss below

Following are the instructions that I followed to run the MapReduce job 

------------------------------------

On setting up hadoop on the local system, 
first, the DFS needed to be started. 
hadoop-*/bin/start-all.sh

Then we need to copy the input file into the HDFS
hadoop-*/bin/hadoop dfs -copyFromLocal hadoop-*/YourInputFileLocation input

To view this file 
hadoop-*/bin/hadoop dfs -ls

To run the job 
hadoop-*/bin/hadoop jar code.jar CorpusCaculator input output

To view the file on errorless execution
hadoop-*/bin/hadoop dfs -ls output

To check the actual content 
hadoop-*/bin/hadoop dfs -cat output/*

To copy the files to your local disk and check/modify
hadoop-*/bin/hadoop dfs -copyToLocal output YourOutputPath


--------------------------------------------
Running the MapReduce job on the EMR, follow the instructions as provided in class and the discussion slides

-First the files need to be imported into the S3. This includes the jar file and the input file in the S3 bucket
-Moving to the EMR console, first we start a cluster.
-In the cluster, adding a new configuration as a new JAR, we specify the directory in the S3 bucket with the input and output arguments. 
