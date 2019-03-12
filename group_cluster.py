import sys
from collections import defaultdict
import numpy as np


def mstToCluster_K(mstToBeMerged,cluster_num):
    '''

    :param mstToBeMerged:
    :param cluster_num: K
    :return:
    '''
    edge_list = mstToBeMerged
    edge_list=sorted(edge_list,key=lambda x:x.getWeight(),reverse=True)
    nodes=[]
    for edge in edge_list:
        u,v,w = edge.getLeft(),edge.getRight(),edge.getWeight()
        if u not in nodes:
            nodes.append(u)
        if v not in nodes:
            nodes.append(v)

    for i in range(0,cluster_num-1):
        edge_list.pop(0)

    return mstToCluster(edge_list,nodes)

def mstToCluster(mstToBeMerged,nodes):
    '''

    :param mstToBeMerged:
    :param cluster_num: K
    :return:
    '''
    edge_list = mstToBeMerged
    edge_list=sorted(edge_list,key=lambda x:x.getWeight(),reverse=True)


    results=[]
    points = nodes

    while len(points)>0:
        temp_cluster=[]
        temp=[]
        point = points.pop(0)
        temp_cluster.append(point)

        while True:
            for edge in edge_list:
                u = edge.getLeft()
                v = edge.getRight()
                if (u not in temp_cluster)&(v not in temp_cluster):
                    continue
                else:
                    if (u in temp_cluster)&(v not in temp_cluster):
                        temp_cluster.append(v)
                        points.remove(v)
                    if (u not in temp_cluster)&(v in temp_cluster):
                        temp_cluster.append(u)
                        points.remove(u)
                    edge_list.remove(edge)

            if(temp_cluster==temp):
                results.append(temp_cluster)
                break
            else:
                temp = temp_cluster.copy()
    return results

def mstPartition_ZEMST(mst,d,c,f):
    '''
    remove edges satisfied a predifined inconsistency measure
    :param mst:  edges list of a mst
    :param d: 搜索深度
    :param c: constant to detect outlier w
    :param f:
    :return:
    '''

    edge_list = mst
    neighbors=defaultdict(list)
    nodes=[]


    #邻接点倒装列表
    w_max=0
    w_min=sys.float_info.max
    for edge in edge_list:
        u, v, w = edge.getLeft(),edge.getRight(),edge.getWeight()
        if w < w_min:
            w_min = w
        if w > w_max:
            w_max = w
        neighbors[u].insert(0,(v,w))
        neighbors[v].insert(0,(u,w))
        if u not in nodes:
            nodes.append(u)
        if v not in nodes:
            nodes.append(v)

    for edge in edge_list:
        u,v,w = edge.getLeft(),edge.getRight(),edge.getWeight()


        #获取邻近边的权重
        #end1
        u_start_node=[u]
        u_parent_node=[v]
        u_ws = getDweight(neighbors,d,u_start_node,u_parent_node)


         #end2
        v_start_node=[v]
        v_parent_node=[u]
        v_ws = getDweight(neighbors,d,v_start_node,v_parent_node)


        #计算邻接边的均值和方差
        u_wmean, u_wstd = getMeanAndStd(u_ws)
        v_wmean, v_wstd = getMeanAndStd(v_ws)


        #定义边删除规则
        c_u, c_v = setC(u_wmean,c), setC(v_wmean,c)
        if  w > max(u_wmean + c_u*u_wmean, v_wmean + c_v*u_wmean):
            if edge in edge_list:
                edge_list.remove(edge)

        # if (w>(u_w_mean+c*u_w_std))|(w>(v_w_mean+c*v_w_std)):
        #     if edge in edge_list:
        #         edge_list.remove(edge)

        # if  w>max(u_w_mean+c*u_w_std,v_w_mean+c*v_w_std):
        #     if edge in edge_list:
        #         edge_list.remove(edge)

        # if  w>(f*(max(c*u_wmean,c*v_wmean))):
        #     if edge in edge_list:
        #         edge_list.remove(edge)
    return mstToCluster(edge_list,nodes),edge_list


def Min_Max_Standardize(x,x_max,x_min):
    return (x-x_min)/(x_max-x_min)

def getDweight(neighbors,d,start_node,parent_node):
    start_node = start_node
    parent_node = parent_node

    dep=0
    ws=[]
    while dep<d:
        temp_node=[]
        temp_parents=[]
        if len(start_node)==0:
            break
        for node in start_node:
            temp_parents.append(node) #下次不再访问父节点
            for edge in neighbors[node]:
                id = edge[0]
                temp_w = edge[1]
                if id not in parent_node:
                    temp_node.append(id)
                    if temp_w!=10:
                        ws.append(temp_w)
        start_node = temp_node
        parent_node = temp_parents
        dep += 1
    return ws

def getMeanAndStd(ws):
    if len(ws) == 0:
        w_mean = 1/42
        w_std = 0
    else:
        if len(ws)>2:
            #去除最大值
            ws.remove(max(ws))
        ws = np.array(ws)
        w_mean = ws.mean()
        w_std = ws.std()
    return w_mean,w_std

def setC(mean,c):
    if mean == 1/42:
        c=10

    return c


