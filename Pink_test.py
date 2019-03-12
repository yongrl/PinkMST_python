import time
from math import sqrt
from pyspark import SQLContext
from DataSpliter import DataSpliter
from KruskalMST_UnionFind import kruskalReducer
from PinkMST import PinkMST
from Point import Point
from graph_show import graph_show
from group_cluster import mstPartition_ZEMST
import pyhdfs



def GetPartitionFunction(line,inputDataFilesLoc=None,numDataSplits=None):
    partionId = int(line)
    print("partitionId:",partionId)
    subGraphsPair = getSubGraphPair(partionId,numDataSplits,inputDataFilesLoc)
    pinkMst = PinkMST(subGraphsPair,partionId)
    edgeList = pinkMst.getEdgeList()
    return edgeList

def SetPartitionIdFunction(line,K):
    key = line[0]//K           #需要整除
    return (key,line[1])


def CreateCombiner(edge):
    edgeList=[]
    edgeList.append(edge)
    return edgeList

def Merger(list,edge):
    mergeList = list
    mergeList.append(edge)
    return mergeList


def getSubGraphPair(partitionId,numDataSplits,inputDataFilesLoc):

    numBipartiteSubgraphs = numDataSplits * (numDataSplits - 1) / 2;
    if(partitionId < numBipartiteSubgraphs):
        rightId = getRightId(partitionId)
        leftId = getLeftId(partitionId,rightId)
    else:
        leftId = partitionId - numBipartiteSubgraphs
        rightId = leftId

    leftFileName = "%s/part-%05d" % (inputDataFilesLoc,leftId)
    rightFileName = "%s/part-%05d" % (inputDataFilesLoc,rightId)

    pointsLeft = openFile(leftFileName)
    pointsRight = None

    if (leftFileName!=rightFileName):
        pointsRight=openFile(rightFileName)

    print("sub Id:",leftId," - ",rightId)

    return (pointsLeft,pointsRight)

    #
def displayResults(mstToBeMerged):
    print("result mst count :",len(mstToBeMerged))
    for j in mstToBeMerged:
        print(j.getLeft(),",",j.getRight(),",",j.getWeight())

def openFile(fileName):
    client = pyhdfs.HdfsClient(hosts="10.3.40.35,9000")
    re = client.open(fileName)
    data = re.read()
    data = data.split(b'\n')
    pointsPair=[]
    for i in data:
        i = str(i,encoding='utf-8')
        if(len(i.strip())==0): continue
        info=i
        pointId = int(info.split(":")[0])
        ts = info.split(":")[1].replace("[","").replace("]","")
        coords = [float(i) for i in ts.split(",")]
        if pointId==63:
            print("63号数据：")
            print(coords)
        pointsPair.append(Point(pointId,coords))
    return pointsPair


def getRightId(partId):
    return int (sqrt((partId << 3) + 1) + 1) >> 1

def getLeftId(partId,rightId):
    return partId - (((rightId - 1) * rightId) >> 1)


# main process
hadoop_master ='hdfs://10.3.40.35:9000'

K=8
numDataSplits = 4
idPartitionFilesLoc='/yongrl/idPartition'
dataParitionFilesLoc='/yongrl/output'
filePath = "hdfs://10.3.40.35:9000/yongrl/pay_money_21.csv"

start = time.time()

numSubGraphs = numDataSplits * (numDataSplits - 1) / 2 + numDataSplits
splitter = DataSpliter(inputFileName=filePath, numSplits=numDataSplits, outputDir=dataParitionFilesLoc)
splitter.createPartitionFiles(idPartitionFilesLoc, numPartitions=numSubGraphs)
splitter.writeSequenceFile()

sc = splitter.getSparkConf()
sc.addPyFile('PinkMST.py')
sc.addPyFile('Edge.py')
sc.addPyFile('Point.py')
sqlContext = SQLContext(sc)

numPoints = len(splitter.features_data)
print('numPoints count: ', numPoints)

data = splitter.features_data
import pandas as pd
pd.DataFrame(data).to_csv("sample_data.csv")

partitionRdd = sc.textFile(name=hadoop_master+idPartitionFilesLoc,minPartitions = int(numSubGraphs))
print("partitionRdd count: ", partitionRdd.collect())


partitions = partitionRdd.flatMap(f=lambda x:GetPartitionFunction(line=x,inputDataFilesLoc=dataParitionFilesLoc,numDataSplits=numDataSplits))

print("partitions count: ",partitions.count())
edges = partitions.map(lambda edge:(edge[1].getLeft(),edge[1].getRight(),edge[1].getWeight())).collect()
pd.DataFrame(edges).to_csv("prim_results.csv")

mstToBeMerged = partitions.combineByKey(createCombiner=CreateCombiner, mergeValue=Merger, mergeCombiners=lambda x:kruskalReducer)

print("mstToBeMerged count: ", mstToBeMerged.count())

while(numSubGraphs>1):
    numSubGraphs = (numSubGraphs + (K - 1)) // K   #需要整除
    mstToBeMergedResult = mstToBeMerged.map(lambda x:SetPartitionIdFunction(x,K)).reduceByKey(kruskalReducer, int(numSubGraphs))
    mstToBeMerged = mstToBeMergedResult

mstToBeMerged=mstToBeMerged.collect()[0][1]
displayResults(mstToBeMerged)
end = time.time()
print("PinkTotalTime=" + str(end - start));
graph_show(mstToBeMerged)
cluster,edge_list = mstPartition_ZEMST(mstToBeMerged,3,5,2)
print("new edge_list")
#displayResults(edge_list)

for i in cluster:
    print(i)








