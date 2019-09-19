# ddos-spark
This repo contains project 'ddos', which has been created to meet the following requirements-

**Ingest**

  - Read a file from local drive and write to a message system such as Kafka

**Detection**

  - Write an application which reads messages from the message system and detects whether the attacker is part of the DDOS attack
  - Once an attacker is found, the host ip-address should be written to a directory which could be used for further processing

**Limitations**

  - This is just a hack, hence no logs in place for debug purpose
  - As this is an assignment to check the capability, the resources are limited - hence we will find batch processing instead of streaming solution
  - As it is batch processing, we need to make sure Ingest has to be run only once, else we need to clear the data from queue if you want to run that again, as we are reading whole content everytime we run Detection
  - Every Detection run creates a different target folder

# Project Structure
```
-ddos
 |-cofig <contains configurations like application-properties and log4j-properties>
 |-jobs <py-directory: contains all the python code here>
 |-libs <libs-directory: any external jar libs>
 |-resources <source or target data folder>
 |-requirements.txt <file which contains all the 3rd party py-libs>
```

# Project Build and Deployment

  - There are no scripts created for build and deployment
  - Will follow the instructions below to make it work
  - Take a zip of jobs folder
  - Make sure all the requirements have been installed
    ```
    $ pip install -r requirements.txt
    ```
  - Setup application-properties json file which contains
    ```
    {
	    "srcFilePath" : "<project-loc>/ddos/resources/source/apache-access-log.txt",
	    "targetPath" : "<project-loc>/ddos/resources/target",
	    "kafkaBootstrapServers" : "localhost:9092",
	    "kafkaTopic" : "ddos-log"
    }
    ```
  - Setup log4j-properties files if required

# Run Application

  - Make sure Zookeeper and Kafka are running
  - Make sure the same has been updated in application-properties
  - Make sure the required properties and libs are available
  - Then do spark-submit as below - here you run either ingest/detect operation at a time
    ```
    $ spark-submit --jars <project-loc>/ddos/libs/spark-sql-kafka-0-10_2.11-2.4.0.jar,<project-loc>/ddos/libs/kafka-clients-0.10.2.2.jar --py-files jobs.zip <project-loc>/ddos/jobs/main.py <project-loc>/ddos/config/application_properties.json ingest/detect
    ```
  - To pass log4j-properies, pass additional arguments with --conf option
    ```
    $ spark-submit --jars ... --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=<project-loc>/ddos/config/log4j-spark.properties" --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=<project-loc>/ddos/config/log4j-spark.properties" --py-files ...
