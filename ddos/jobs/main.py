'''
Created on 17-Sep-2019

@author: AJAYNH
'''

#import time
import sys
from pyspark.sql import SparkSession
from jobs.parse_logs import run as pRun
from jobs.kafka_producer import writeDFToKafka
from jobs import analyze, writeDFToFile


def parseArgs():
    args = {}
    if len(sys.argv) < 3:
        raise Exception("Missing command-line parameter, follow the format as given below\n" +
            "  Command: main.py <applicationPropertiesFilePath> <operation: load/parse>")
    else:
        args["configFilePath"] = sys.argv[1]
        if sys.argv[2] != 'ingest' and sys.argv[2] != 'detect':
            raise Exception("Invalid operation specified, accepts only ingest/detect")
        args["operation"] = sys.argv[2]
    return args

def getConfig(configFilePath):
    import json
    with open(configFilePath, 'r') as config_file:
        return json.load(config_file)

def validateConfig(config):
    configKeys = list(config.keys())
    if "srcFilePath" in configKeys and "kafkaBootstrapServers" in configKeys and "kafkaTopic" in configKeys and "targetPath" in configKeys:
        pass
    else:
        raise Exception("Application-properties json file should contain these mandatory properties - [srcFilePath, kafkaBootstrapServers, kafkaTopic, targetPath]")

if __name__ == '__main__':
    #print("Sleep for 2mins. current timestamp: {}".format(time.ctime()))
    #time.sleep(2*60)
    #print("Awaken. current timestamp: {}".format(time.ctime()))
    
    try:
        args = parseArgs()
        config = getConfig(args.get("configFilePath"))
        validateConfig(config)
        spark = SparkSession.builder.appName("ddos").master("local[*]").getOrCreate()
        if args.get("operation") == 'ingest':
            parsedDf = pRun(spark, config.get("srcFilePath"))
            writeDFToKafka(parsedDf, config.get("kafkaBootstrapServers"), config.get("kafkaTopic"))
        else:
            finalDf = analyze(spark, config.get("kafkaBootstrapServers"), config.get("kafkaTopic"))
            writeDFToFile(finalDf, config.get("targetPath"))
    except Exception as e:
        print("Exception: {}".format(str(e)))
