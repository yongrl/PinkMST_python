import sys
from pyspark.sql.functions import pandas_udf, PandasUDFType
from Edge import Edge

from Heap import Heap


class PinkMST():
    '''
    :param
    partitionId:int
    pairedData:(list<Point>,list<Point>)
    '''

    def __init__(self,pairedData=None,partitionId=None,mstToBeMerged=None):
        self.leftData = pairedData[0]
        self.rightData = pairedData[1]
        self.partitionId = partitionId
        self.mstToBeMerged = mstToBeMerged
        if(self.rightData is None):
            self.isBipartite = False
        else:
            self.isBipartite = True


    def getMST(self):
        return self.mstToBeMerged

    def getEdgeList(self):
        if (self.isBipartite):
            edgeList = self.BipartiteMST()
        else:
            edgeList = self.PrimLocal()
        return edgeList

    '''
    def BipartiteMST(self):
        data1 = self.leftData
        data2 = self.rightData
        localLeftData = []
        localRightData = []

        numLeftPoints = len(self.leftData)
        numRightPoints = len(self.rightData)

        numLeftPointsInTrack = 0
        numRightPointsInTrack = 0

        edgePairs = []

        next1 = []#size:numLeftPoints
        parent1 = [] #size:numLeftPoints
        key1 = [] #size:numLeftPoints

        next2 = []  # size:numRightPoints
        parent2 = [] # size:numRightPoints
        key2 = [] # size:numRightPoints

        nextLeft = None
        nextRight = None
        parentLeft = None
        parentRight = None
        keyLeft = None
        keyRight = None

        tmpData = []
        tmpKey = []
        tmpNext = []
        tmpParent = []

        tmpNumPoints = 0

        for i in range(numLeftPoints):
            key1.append(sys.float_info.max)
            next1.append(i)
            parent1.append(-1)

        for i in range(numRightPoints):
            key2.append(sys.float_info.max)
            next2.append(i)
            parent2.append(-1)

        if(numLeftPoints<=numRightPoints):
            numLeftPointsInTrack = numLeftPoints
            numRightPointsInTrack = numRightPoints

            keyLeft = key1
            nextLeft = next1
            parentLeft = parent1

            keyRight = key2
            nextRight = next2
            parentRight = parent2

            localLeftData = data1
            localRightData = data2
        else:
            numLeftPointsInTrack = numRightPoints
            numRightPointsInTrack = numLeftPoints

            keyLeft = key2
            nextLeft = next2
            parentLeft = parent2

            keyRight = key1
            nextRight = next1
            parentRight = parent1

            localLeftData = data2
            localRightData = data1

        parentLeft[0] = -1
        next = 0
        shift = 0
        currPoint = 0
        otherPoint = 0

        minV = 0
        dist = 0
        isSwitch = True
        gnextParent = 0
        gnext = 0


        while(numRightPointsInTrack>0):
            shift = 0
            currPoint = next
            next = nextRight[shift]
            minV = sys.float_info.max
            for i in range(numRightPointsInTrack):
                isSwitch = True
                otherPoint = nextRight[i]
                dist = max(0.001,localLeftData[currPoint].dtw_dist(localRightData[otherPoint]))
                if(keyRight[otherPoint]>dist):
                    keyRight[otherPoint] = dist
                    parentRight[otherPoint] = currPoint

                if (keyRight[otherPoint] < minV):
                    shift = i
                    minV = keyRight[otherPoint]
                    next = otherPoint


                if (dist < keyLeft[currPoint]):
                    keyLeft[currPoint] = dist
                    parentLeft[currPoint] = otherPoint

            gnext = localRightData[next].getId();
            gnextParent = localLeftData[parentRight[next]].getId();

            #find the global min
            for i in range(numLeftPointsInTrack):
                currPoint = nextLeft[i]
                if(minV > keyLeft[currPoint]):
                    isSwitch = False
                    minV = keyLeft[currPoint]
                    otherPoint = parentLeft[currPoint]
                    gnextParent = localLeftData[currPoint].getId()
                    gnext = localRightData[otherPoint].getId()
                    next = currPoint
                    shift = i


            for i in range(numLeftPointsInTrack):
                currPoint = nextLeft[i]
                otherPoint = parentLeft[currPoint]

            if((numLeftPointsInTrack == numLeftPoints) & (numRightPointsInTrack==numRightPoints)):
                numLeftPointsInTrack-=1
                nextLeft[0] = nextLeft[numLeftPointsInTrack]

            edge = Edge(min(gnext, gnextParent), max(gnext, gnextParent), minV)
            edgePairs.append((self.partitionId,edge))

            if(isSwitch==False):
                numLeftPointsInTrack-=1
                nextLeft[shift]=nextLeft[numLeftPointsInTrack]
                continue

            numRightPointsInTrack-=1
            nextRight[shift]=nextRight[numRightPointsInTrack]
            tmpData = localRightData
            localRightData = localLeftData
            localLeftData = tmpData

            # swap keyLeft and keyRight
            tmpKey = keyRight
            keyRight = keyLeft
            keyLeft = tmpKey

            # swap parentLeft and parentRight
            tmpParent = parentRight
            parentRight = parentLeft
            parentLeft = tmpParent

            # swap nextLeft and nextRight
            tmpNext = nextRight
            nextRight = nextLeft
            nextLeft = tmpNext

            # swap nptsLeft and nptsRight
            tmpNumPoints = numRightPointsInTrack
            numRightPointsInTrack = numLeftPointsInTrack
            numLeftPointsInTrack = tmpNumPoints


        for i in range(numLeftPointsInTrack):
            currPoint = nextLeft[i]
            otherPoint = parentLeft[currPoint]
            minV = keyLeft[currPoint]
            gnextParent = localLeftData[currPoint].getId()
            gnext = localRightData[otherPoint].getId()
            edge = Edge(min(gnext, gnextParent), max(gnext, gnextParent), minV)
            edgePairs.append((self.partitionId, edge))

        sorted(edgePairs, key=lambda x: x[1].getWeight(), reverse=False)
        print("edgePairs: ",len(edgePairs))

        return edgePairs

    def PrimLocal(self):

        data = self.leftData
        numPoints = len(data)
        edgePairs = []   # Capacity:numPoints

        left=list(range(numPoints))
        parent=list(range(numPoints))
        key=list(range(numPoints))

        for i in range(numPoints):
            key[i]=(sys.float_info.max)
            left[i]=(i+1)

        key[0] = 0
        parent[0] =-1

        next = 0
        j = numPoints - 1


        while(j>0):
            currPt = next
            shift = 0
            next = left[shift]
            minV = sys.float_info.max
            for i in range(j):
                otherPt = left[i]
                dist = max(0.001,data[currPt].dtw_dist(data[otherPt]))
                if(dist <key[otherPt]):
                    key[otherPt] = dist
                    parent[otherPt] = currPt

                if(key[otherPt]<minV):
                    shift = i
                    minV = key[otherPt]
                    next = otherPt

            globalNext = data[next].getId()
            globalNextParent = data[parent[next]].getId()
            edge = Edge(min(globalNext,globalNextParent),max(globalNext,globalNextParent),minV)
            edgePairs.append((self.partitionId,edge))
            j-=1
            left[shift] = left[j]


        sorted(edgePairs,key=lambda x:x[1].getWeight(),reverse=False)
        return edgePairs
    '''

    def BipartiteMST(self):
        # 存每个节点的key值
        edges={}
        left_data = self.leftData
        right_data = self.rightData


        num_left = len(left_data)
        num_right = len(right_data)

        key_left = []
        key_right = []

        parent_left = []
        parent_right =[]

        # 建立最小堆
        minHeap_left = Heap()
        minHeap_right =Heap()


        # 初始化左节点三个数据结构
        for v in range(num_left):
            parent_left.append(-1)#初始时，每个节点的父节点是-1
            key_left.append(float('inf'))#初始时，每个节点的key值都是无穷大
            minHeap_left.array.append( minHeap_left.newMinHeapNode(v, key_left[v]))
            minHeap_left.pos.append(v)

        # 初始化右节点三个数据结构
        for v in range(num_right):
            parent_right.append(-1)#初始时，每个节点的父节点是-1
            key_right.append(float('inf'))#初始时，每个节点的key值都是无穷大
            minHeap_right.array.append( minHeap_right.newMinHeapNode(v, key_right[v]))
            minHeap_right.pos.append(v)

        minHeap_left.pos[0] = 0#不懂这句，本来pos的0索引元素就是0啊
        key_left[0] = 0#让0节点作为第一个被挑选的节点
        minHeap_left.decreaseKey(0, key_left[0])
        #把堆中0位置的key值变成key[0]，函数内部重构堆

        minHeap_right.pos[0] = 0
        #key_right[0] = 0
        minHeap_right.decreaseKey(0, key_right[0])



        # 初始化堆的大小为V即节点个数
        minHeap_left.size = num_left
        minHeap_right.size = num_right

        label ='left'
        while minHeap_left.isEmpty() == False | minHeap_right.isEmpty() == False:
            # 抽取最小堆中key值最小的节点
            if label=='left':
                newHeapNode = minHeap_left.extractMin()
                u = newHeapNode[0]

                for i in range(minHeap_right.size):
                    id= minHeap_right.array[i][0]
                    dist = max(1/42,left_data[u].dtw_dist(right_data[id]))
                    if dist < key_right[id]:
                        key_right[id] = dist
                        parent_right[id] = u

                        # 也更新最小堆中节点的key值，重构
                        minHeap_right.decreaseKey(id, key_right[id])
                        edges[(u,id)]=dist

                label='right'
            else:
                newHeapNode = minHeap_right.extractMin()
                v = newHeapNode[0]

                for i in range(minHeap_left.size):
                    id= minHeap_left.array[i][0]
                    dist = max(1/42,right_data[v].dtw_dist(left_data[id]))
                    if dist < key_left[id]:
                        key_left[id] = dist
                        parent_left[id] = v

                        # 也更新最小堆中节点的key值，重构
                        minHeap_left.decreaseKey(id, key_left[id])

                        edges[(id,v)]=dist
                label='left'


        edgePairs=[]
        for i in range(1, num_left):
            edgePairs.append((self.partitionId,Edge(right_data[parent_left[i]].getId(),left_data[i].getId(),\
                                                     edges[(i,parent_left[i])])))

        for j in range(0,num_right):
            edgePairs.append((self.partitionId,Edge(left_data[parent_right[j]].getId(),right_data[j].getId(),\
                                                    edges[(parent_right[j],j)])))


        return edgePairs




    def PrimLocal(self):
        # 存每个节点的key值
        data = self.leftData
        V = len(data)
        edges={}

        key = []
        # 记录构造的MST
        parent = []
        # 建立最小堆
        minHeap = Heap()

        # 初始化以上三个数据结构
        for v in range(V):
            parent.append(-1)#初始时，每个节点的父节点是-1
            key.append(float('inf'))#初始时，每个节点的key值都是无穷大
            minHeap.array.append( minHeap.newMinHeapNode(v, key[v]) )
            #newMinHeapNode方法返回一个list，包括节点id、节点key值
            #minHeap.array成员存储每个list，所以是二维list
            #所以初始时堆里的每个节点的key值都是无穷大
            minHeap.pos.append(v)

        minHeap.pos[0] = 0#不懂这句，本来pos的0索引元素就是0啊
        key[0] = 0#让0节点作为第一个被挑选的节点
        minHeap.decreaseKey(0, key[0])
        #把堆中0位置的key值变成key[0]，函数内部重构堆

        # 初始化堆的大小为V即节点个数
        minHeap.size = V
        # print('初始时array为',minHeap.array)
        # print('初始时pos为',minHeap.pos)
        # print('初始时size为',minHeap.size)

        while minHeap.isEmpty() == False:

            # 抽取最小堆中key值最小的节点
            newHeapNode = minHeap.extractMin()
            # print('抽取了最小元素为',newHeapNode)
            u = newHeapNode[0]

            for i in range(minHeap.size):
                id= minHeap.array[i][0]
                dist = max(1/42,data[u].dtw_dist(data[id]))
                if dist < key[id]:
                    key[id] = dist
                    parent[id] = u

                    # 也更新最小堆中节点的key值，重构
                    minHeap.decreaseKey(id, key[id])
                    edges[(u,id)]=dist
                    edges[(id,u)]=dist

        edgePairs=[]
        for i in range(1, V):
            edgePairs.append((self.partitionId,Edge(data[parent[i]].getId(),data[i].getId(),edges[(parent[i],i)])))

        return edgePairs


    @staticmethod
    def displayValue(leftData,rightData,numPoints):
        for i in range(numPoints):
            print("==== samples " , i , ", " , leftData[i] ," || "
                    , rightData[i] if rightData != None else None)