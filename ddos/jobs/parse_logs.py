'''
Created on 17-Sep-2019

@author: AJAYNH
'''

def readLogToDF(spark, srcFilePath):
    return spark.read.text(srcFilePath)

def run(spark, srcFilePath):
    return readLogToDF(spark, srcFilePath)
    #return parseLogDf(inputDf)