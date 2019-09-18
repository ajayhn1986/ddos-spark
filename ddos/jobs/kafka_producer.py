'''
Created on 17-Sep-2019

@author: AJAYNH
'''

def writeDFToKafka(df, kafkaBootstrapServers, kafkaTopic):
    df.selectExpr("CAST(value AS STRING)") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafkaBootstrapServers) \
        .option("topic", kafkaTopic) \
        .save()