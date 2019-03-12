import os
import sys

import pyhdfs
from pandas.core.algorithms import quantile

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DoubleType

import PinkMST
from Point import Point
import pandas as pd

os.environ['PYSPARK_PYTHON']='/usr/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON']='/usr/bin/python3'


from pyspark import SparkContext as sc, SQLContext
from pyspark import SparkConf

conf = SparkConf().setAppName("PinkMST").setMaster("local[1]").set("spark.cores.max","12").set("executor-memory","8g")

filePath = "hdfs://10.3.40.35:9000/yongrl/data.txt"
hadoop_master ='hdfs://10.3.40.35:9000'


class DataSpliter():
    def __init__(self, inputFileName=filePath, numSplits=None, outputDir=None):

        self.inputFileName = inputFileName
        self.numSplits = numSplits
        self.outputDir = outputDir
        self.sc = sc.getOrCreate(conf)
        self.data = None

    def getSparkConf(self):
        return self.sc

    def displayValue(points, size):
        print("=" * 100)
        num = 0
        for p in points:
            print("====:" + p)
            p += 1
            if p > size:
                break
        print("=" * 100)

    def readFile(self):
        data = self.sc.sequenceFile(self.outputDir).map(lambda x:x[1]).collect()
        PinkMST.displayValue(data,None,len(data))

    def createPartitionFiles(self,fileLoc,numPartitions):
        self.delete(fileLoc)
        idSubgraphs = list(range(int(numPartitions)))
        self.sc.parallelize(idSubgraphs, numPartitions).saveAsTextFile(hadoop_master+fileLoc)

    def writeSequenceFile(self):
        self.sc.addPyFile('Point.py')
        self.loadData()

        numPoints = len(self.features_data)
        print("user num: ", numPoints)
        pointId = 0
        points = []
        for i in self.features_data:
            points.append(str(pointId) + ":" + str(i))
            pointId += 1

        self.user_info = pd.DataFrame(self.total_users.collect()).reset_index(drop=True)
        self.displayValue(points, 5)
        self.saveAsSequenceFile(points)

    def displayValue(self,points, size):
        print("=" * 80)
        for i in range(size):
            print(points[i])
        print("=" * 80)


    def saveAsSequenceFile(self, points):
        self.delete(self.outputDir)

        pointsToWrite = self.sc.parallelize(points,self.numSplits)
        pointsPairToWrite = pointsToWrite.map(lambda x: x)
        # pointsPairToWrite.saveAsNewAPIHadoopFile(path=hadoop_master+self.outputDir,outputFormatClass="org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat",
        #                                          keyClass="org.apache.hadoop.io.NullWritable",
        #                                          valueClass="org.apache.hadoop.io.Text")
        pointsPairToWrite.saveAsTextFile(path=hadoop_master+self.outputDir)

    def loadData(self):
        # data = self.sc.textFile(self.inputFileName)
        # self.data = data.map(lambda line:line.split(" ")).map(lambda l:[float(i) for i in l])
        # schema = StructType([
        #         StructField("f1", FloatType(), True),
        #         StructField("f2", FloatType(), True),
        #         StructField("f3", FloatType(), True),
        #         StructField("f4", FloatType(), True),
        #         StructField("f5", FloatType(), True) ])
        # self.data = SparkSession(self.sc).createDataFrame(data,schema)
        sqlContext = SQLContext(self.sc)
        data = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(self.inputFileName)
        features=['pm_1','pm_2','pm_3','pm_4','pm_5','pm_6','pm_7','pm_8','pm_9','pm_10','pm_11','pm_12',
                  'pm_13', 'pm_14', 'pm_15', 'pm_16', 'pm_17', 'pm_18', 'pm_19', 'pm_20', 'pm_21']
        total_users = data[data['total_money']>0]
        #去除极值点
        # V = total_users['total_money'].quantile(0.8)
        #
        features_data = total_users[total_users['total_money']>0][features]
        total_users =data

        print("total_users type:",type(total_users))


        #total_users = self.sc.parallelize(total_users.collect())
        self.total_users = total_users.rdd.map(lambda line: [i for i in line])

       # features_data = self.sc.parallelize(features_data.collect())
        self.features_data = features_data.rdd.map(lambda line: [float(i) for i in line]).take(100)

        # numPoints = len(features_data)
        #
        # pointId_list = list(range(numPoints))


        return self.features_data,self.total_users

    def delete(self,file):
        fs = pyhdfs.HdfsClient(hosts='10.3.40.35,9000')
        if(fs.exists(file)):
            fs.delete(file,recursive=True)
        else:
            pass


    def writeSmallFiles(self):
        points = [Point(0, [0.0, 0.0]), Point(1, [1.0, 0.0]), Point(2, [0.0, 2.0]), Point(3, [3.0, 2.0])]
        self.saveAsSequenceFile_1(points, self.outputDir)

    @staticmethod
    def openSequenceFile(filename):
        return sc.sequenceFile(filename).collect()


if __name__ == '__main__':
    outputDir = 'hdfs://10.3.40.35:9000/yongrl/output'
    filePath = "hdfs://10.3.40.35:9000/yongrl/pay_money_21.csv"
    ds = DataSpliter(inputFileName=filePath,outputDir=outputDir )
    ds.writeSequenceFile()