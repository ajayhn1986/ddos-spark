def parseLogDf(logDf):
    from pyspark.sql.functions import regexp_extract, concat, lit, to_timestamp
    
    host_pattern = r'(^\S+\.[\S+\.]+\S+)\s'
    ts_pattern = r'\[(\d{2}/\w{3}/\d{4}):(\d{2}:\d{2}:\d{2}) ([+-]{1}\d{4})]'
    
    return logDf.withColumn('host',regexp_extract('value', host_pattern, 1)) \
                .withColumn('timeStamp',to_timestamp(concat(regexp_extract('value', ts_pattern, 1),lit(' '),regexp_extract('value', ts_pattern, 2)),'dd/MMM/yyyy HH:mm:ss')) \
                .select(['host','timeStamp'])

def analyze(spark, kafkaBootstrapServers, kafkaTopic):
    from jobs.kafka_consumer import readFromKafka
    
    logDf = readFromKafka(spark, kafkaBootstrapServers, kafkaTopic)
    parsedDf = parseLogDf(logDf)
    
    hostTrafficDf = parsedDf.orderBy(parsedDf.host.desc(), parsedDf.timeStamp.asc()) \
        .groupBy('host','timeStamp').count() \
        .withColumnRenamed('count','traffic')
    
    return hostTrafficDf.filter(hostTrafficDf.traffic > 1) \
        .select('host').dropDuplicates()

def writeDFToFile(df, targetPath):
    from datetime import datetime
    df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "false").save("{}/out-log-{}".format(targetPath,datetime.now().timestamp()))