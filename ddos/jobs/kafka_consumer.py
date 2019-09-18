'''
Created on 17-Sep-2019

@author: AJAYNH
'''

def readFromKafka(spark, kafkaBootstrapServers, kafkaTopic):
    return spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafkaBootstrapServers) \
        .option("subscribe", kafkaTopic) \
        .load() \
        .selectExpr("CAST(value AS STRING)")