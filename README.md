# Nasa log anlytics - Pyspark and docker

**Description**

This is a PySpark application which determines the top-n most frequent visitors and urls for each day of the Nasa log trace. This PySpark application along with pytest test cases is packaged as an image with Dockerized Spark and Hadoop. Dockerfile has been configured in a way that it downloads log file from ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz and places it in /src/input_dir. you can find the structure of this project below with respective comments.

Dockerfile(used) is commited here so that anyone can make use of it as a template and enhance/update it very easily as per their use case or requirements.


### Project Structure:

```
├─ README.md
├─ requirements.txt     # has list of packages required for Spark application
├─ sparkJob_submit.sh   # A runnable Spark submit shell wrapper
├─ src                  # Main Application dir
│  ├─ input_dir         # Docker image build will download the input log file & writes here
│  ├─ nasa_log_analysis.py        # Pyspark application code
│  ├─ output_dir                  # Base output dir for Spark application
│  │  ├─ badRecordsOutPutFile     # Outdir for storing null or unparsable records in CSV
│  │  ├─ frequent_urls            # to-n frequent urls for each day
│  │  ├─ frequent_visitors        # to-n frequent visitors/hosts for each day
│  │  └─ frequent_visitors_urls   # frequent urls and visotors for each day as per assumption.
├─ testcases_submit.sh  # shell wrapper to trigger test cases
├─ tests
│  ├─ setup.py  # setup file to setup spark for pytest
│  ├─ test_nasa_log.py  # test cases application
│  ├─ __init__.py

```

### Docker Image includes: 

         - Spark 3.0.3
         - Hadoop 3.2.0
         - openjdk8
         - python 3.6

### Prerequisite:
   Hope you have docker installed on your system (if not, please install from [Docker site](https://docs.docker.com/desktop/windows/install/) ) to follow the below steps and make pyspark docker container running in your local system.

## How To {steps to build image & run container}:

Please execute below steps in your local working directory.

> Pull image from docker hub:

      docker pull nyamani/nasa_log_pyspark:latest

> Run docker image 

      docker run --rm -it --name nasa_log_container nyamani/nasa_log_pyspark:latest
   This will fire up the container using the image in interactive mode. Also, downloads and set up saprk using spark-3.0.3-bin-hadoop3.2.tgz while satrting up. 
   

## Running test cases:

Directory '/app/tests/' is the test suite it has all unittests for this application.
To run test cases, you can log in to container and execute this script using "sh" or you can execute dircetly using the below command. It will generate a 'test_results.log' in /app/tests/ directory.

      docker exec nasa_log_container dos2unix /app/testcases_submit.sh
      
      docker exec nasa_log_container sh -x /app/testcases_submit.sh

## Running Pyspark Application:

   Use spark_submit.sh in main /app directory to submit the spark job.
   Since it's just a stand alone cluster on local machine I haven't used much resources in terms of number of executots and cores. you can adjust them as per your local system configuarations.

**nasa_log_conf:**
   Most of this PySpark application has been parameterized to accept the chnages dynamically through application configuarion file 'nasa_log_conf'. Application level configurations are like Application name, top-N , input dir path, output Dir Path, input file format , output file format, number of output files etc.

   if you want to chnage any configuartion, you are welcome to change and play with it.

   **spark submit:**

   spark-submit --num-executors 1 --executor-cores 2 \
   --executor-memory 1g --driver-memory 1g  \
   --properties-file /app/configs/nasa_log_conf.properties \
   /app/src/nasa_log_analysis.py

   #### Use the below command to trigger the spark job:

      docker exec nasa_log_container dos2unix /app/sparkJob_submit.sh
      
      docker exec nasa_log_container sh -x /app/sparkJob_submit.sh
			or
      docker exec (containerid) -x /app/sparkJob_submit.sh


## Pysaprk App:
 nasa_log_analysis.py is the main application file and it has a function almost for every transformation we need needed for Nasa log analytics. Each function is well documented with what input it takes and returns. 
 Quick tip: You can use help(<functionName>) to know more about each function.

 #### Here is the high level abstaract:

   - **NASA_access_log_Jul95.gz:** input file in compressed format. 

   - **readInputFile() :** function reads the input file from provided path and returns DF with one 'value' column and one record for each line in file.

   - **columnExtract() :** Takes a base DF (returned from prev. function) and returns a df with multiple columns as per the regex written. Due to time constraint, i'm not explaining more about Regex here but it was kind of clearly stated in Function. Regex also can be moved to configuration file to provide flixbility with reqyirement chnages.

  -  **route_parsed_dataframes() :** This takes Extracted DF as input and returns two DF (good, bad). Basically we can't assume everythig is alright! so using this function we can confirm if there any records with nulls or empty spaces. One can further investigate are they actual nulls, empty spaces or happened due to regex transformation. in our case, there are ~20K records with nulls in one columns. There was One records with empty spaces in almost all the fields except "status" and breaking the Spark application while parsing the Date. So, it was moved to bad records and written into a file for further analysis.

   - **parse_log_time() :** An UDF was register on top of this function to take a string date column with values as '01/Jul/1995:00:00:01 -0400' as input and retuns '1995-07-01 00:00:01'.

  - **top_n_visitors_day():** Takes DF, number as input and Agrregates data on each day. finally, retuns top 'N'  visitors for each day. Analytic was implemented in DF language. (There are other ways like Spark SQL, RDD to implement the same though)

   - **top_n_urls_day():** Takes DF, number as input and Agrregates data on each day. finally, retuns top 'N'  urls for each day.(There are other ways like Spark SQL, RDD to implement the same though)

  - **top_n_visitors_day():** Takes DF, number as input and Agrregates data on each day. finally, retuns top 'N'  visitors & urls for each day.

  - **save_df():** Takes DF, output dir, desired file format, desired number of files as input and writes the output in desired format to given directory with given number of files.

   
## Output :
   Pyspark application's output will be written into into the base directory /src/output_dir with individual folders for each analytic. Data is stored in CSV format with Header for better readability purpose. if we have any downstream applications consuming this data then better be saving it in "parquet" format for performance optimization. you may see "DR" in header it's a dense rank column and it can be dropped in real production. However, it's included here for quick understanding of the output.

   from shell a simple cat or vi or head command can be used to view the output.
   
   #### Command:
      ex: cat /app/src/output_dir/frequent_visitors/part*

   
```
host,date,count,DR
edams.ksc.nasa.gov,1995-07-27,283,1
192.223.3.88,1995-07-27,252,2
jalisco.engr.ucdavis.edu,1995-07-27,248,3
piweba4y.prodigy.com,1995-07-27,241,4
mega218.megamed.com,1995-07-27,239,5
piweba3y.prodigy.com,1995-07-13,1202,1
rush.internic.net,1995-07-13,1119,2
piweba4y.prodigy.com,1995-07-13,1003,3
piweba1y.prodigy.com,1995-07-13,939,4
alyssa.prodigy.com,1995-07-13,906,5
```
   

## Optimizations:
   - cached Extrcated DF for for multiple analytis so it would not have read file each time from file system for each action(Due to lazy evalution). 
   - base DF can be partitioned so that more tasks can be invoked and achieve performace with parallelism. (It's not implemented here due to local sys limitations.)
   - Use Parquet (did't use here since output has to be validated.)
