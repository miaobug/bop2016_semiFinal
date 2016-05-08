# -*- coding: utf-8 -*-
import httplib
from api import *
from threading import Thread
from time import time
# from Queue import Queue

# todo list:
# 增加本地缓存机制/减少接口调用
# 提高求交集的性能
# 检查同步机制!!!!!(request_data
# GET请求的长度限制检查
# SSL链接建立优化
# 环的检查 √
# paper id 2292217923 的 RId 15万多...来个压力测试吧
# 优化RId查询
# 优化并行结构
# 找性能瓶颈
# 多个query测试 √
# 检查重复......

# 性能分析: python的dict采用哈希实现



start_time = time()

#done
def query(id1, id2):
    global start_time
    start_time = time()
    conn = httplib.HTTPSConnection('oxfordhk.azure-api.net')
    expr1 = "Or(Id="+id1+",Composite(AA.AuId=" +id1+ "))"
    expr2 = "Or(Id="+id2+",Composite(AA.AuId=" +id2+ "))"
    #这两次请求好像比较慢一些
    thread1 = Thread(target=send_request, args=[expr1], kwargs={})
    thread2 = Thread(target=send_request, args=[expr2], kwargs={})
    thread1.start()
    thread2.start()
    thread1.join()
    thread2.join()
    print "time: ", time() - start_time

    data1 = request_data[expr1]
    data2 = request_data[expr2]
    len1 = len(data1)
    len2 = len(data2)
    if(len1 <= 1):
        if(len2 <= 1):
            # print "hi"
            return query1(data1, data2, id1, id2)
        else:
            return query2(data1, data2, id1, id2)
    elif(len2 <= 1):
        return query3(data1, data2, id1, id2)
    else:
        return query4(data1, data2, id1, id2)


def query1(data1, data2, id1, id2): #Id-Id
    global start_time
    result = []
    RId1 =  data1[0]['RId']
    if(int(id2) in RId1):
        result.append([int(id1), int(id2)])

    if(len(RId1) >= 1):
        expr_id1_to_id = "Id="+str(RId1[0])
        for id in RId1[1:]:
            expr_id1_to_id = "Or("+expr_id1_to_id+",Id="+str(id)+")"
    elif (len(RId1) == 1):
        expr_id1_to_id = "Id="+str(RId1[0])
    else: #未测试#
        expr_id1_to_id = "empty"
    thread1 = Thread(target=send_request, args=[expr_id1_to_id])
    thread1.start()

    expr_id_to_id2 = "RId=" + id2
    thread2 = Thread(target=send_request, args=[expr_id_to_id2])
    thread2.start()

    id1_to_AuId = [x['AuId'] for x in data1[0]["AA"]] #id1的作者列表
    if (len(id1_to_AuId) >= 2):
        expr_id1_to_AuId = "Or(Composite(AA.AuId=" + str(id1_to_AuId[0]) + "),Composite(AA.AuId=" + str(id1_to_AuId[1]) + "))"
        for AuId in id1_to_AuId[2:]:
            expr_id1_to_AuId = "Or(" + expr_id1_to_AuId + ",Composite(AA.AuId=" + str(AuId) + "))"
    elif (len(id1_to_AuId) == 1):
        expr_id1_to_AuId = "Composite(AA.AuId=" + str(id1_to_AuId[0]) + ")"
    else:
        expr_id1_to_AuId = "empty"
    thread3 = Thread(target=send_request, args=[expr_id1_to_AuId])
    thread3.start()

    if(data1[0].has_key("F")):
        xids1 = [x["FId"] for x in data1[0]["F"]]
    else:
        xids1 = []
    if(data1[0].has_key("C")):
        xids1.append(data1[0]["C"]["CId"])
    if(data1[0].has_key("J")):
        xids1.append(data1[0]["J"]["JId"])
    if(data2[0].has_key("F")):
        xids2 = [x["FId"] for x in data2[0]["F"]]
    else:
        xids2 = []
    if (data2[0].has_key("C")):
        xids2.append(data2[0]["C"]["CId"])
    if (data2[0].has_key("J")):
        xids2.append(data2[0]["J"]["JId"])
    intersection_xid = list(set(xids1) & set(xids2))
    for xid in intersection_xid:
        result.append([int(id1), xid, int(id2)]) # id-xid-id

    thread1.join()
    print "time id1_to_id: ", time() - start_time
    # 这个循环和下面的循环合并是不是更快?
    for paper in request_data[expr_id1_to_id]: # id1引用的paper
        #依赖于thread1
        if int(id2) in paper["RId"]:
            # print "haha"+id1+"-"+str(paper["Id"])+"-"+id2
            result.append([int(id1), paper["Id"], int(id2)]) #id-id-id
        if (paper.has_key("F")):
            fids = [x["FId"] for x in paper["F"]]
            intersection_fid = list(set(fids) & set(xids2))
            for fid in intersection_fid:
                result.append([int(id1), paper["Id"], fid, int(id2)])  # id-id-fid-id ..
        if (paper.has_key("C")):
            if paper["C"]["CId"] in xids2:
                result.append([int(id1), paper["Id"], paper["C"]["CId"], int(id2)])  # id-cid-id-id ..
        if (paper.has_key("J")):
            if paper["J"]["JId"] in xids2:
                result.append([int(id1), paper["Id"], paper["J"]["JId"], int(id2)])  # id-jid-id-id ..

    thread2.join()
    print "time id to id2: ", time() - start_time
    ids_to_id2 = [x["Id"] for x in request_data[expr_id_to_id2]] #引用了id2的文章序号列表
    auids_to_id2 = [x["AuId"] for x in data2[0]["AA"]] #写了id2的作者
    for paper in request_data[expr_id1_to_id]: #id1引用的文章
        # 这块依赖于thread2的结果
        intersection_id= list(set(ids_to_id2) & set(paper["RId"]))
        for paper2 in intersection_id:
            result.append([int(id1), paper["Id"], paper2, int(id2)]) #id-id-id-id
        # 这块依赖于thread1的结果
        auids = [x["AuId"] for x in paper["AA"]] #写了id1引用的文章的作者.
        intersection_auid = list(set(auids) & set(auids_to_id2))
        for auid in intersection_auid:
            result.append([int(id1), paper["Id"], auid, int(id2)]) #id-id-auid-id
    #这块依赖于thread2的结果
    for paper in request_data[expr_id_to_id2]: #引用了id2的文章.
        if(paper.has_key("F")):
            fids = [x["FId"] for x in paper["F"]]
            intersection_fid = list(set(fids) & set(xids1))
            for fid in intersection_fid:
                result.append([int(id1), fid, paper["Id"], int(id2)]) #id-fid-id-id ..
        if(paper.has_key("C")):
            if paper["C"]["CId"] in xids1:
                result.append([int(id1), paper["C"]["CId"], paper["Id"], int(id2)]) #id-cid-id-id ..
        if(paper.has_key("J")):
            if paper["J"]["JId"] in xids1:
                result.append([int(id1), paper["J"]["JId"], paper["Id"], int(id2)]) #id-jid-id-id ..

    thread3.join()
    print "time id1 to AuId to id:", time() - start_time
    for paper in request_data[expr_id1_to_AuId]:
        if paper["Id"] in ids_to_id2:
            temp_auids = [x["AuId"] for x in paper["AA"]]
            id1_to_AuIds = list(set(temp_auids) & set(id1_to_AuId))
            for auid in id1_to_AuId:
                result.append([int(id1), auid, paper["Id"], int(id2)]) #id-auid-id-id

    print len(result), result
    return result

def query2(data1, data2, id1, id2):  # Id-AuId
    global start_time
    result = []

    auids = [x["AuId"] for x in data1[0]["AA"]]  # id1的作者们
    if int(id2) in auids:
        result.append([int(id1), int(id2)])  # id-auid

    RId1 = data1[0]['RId']  # id1引用的文章
    if (len(RId1) >= 2):
        expr_id1_to_id = "Or(Id=" + str(RId1[0]) + ",Id=" + str(RId1[1]) + ")"
        for id in RId1[2:]:
            expr_id1_to_id = "Or(" + expr_id1_to_id + ",Id=" + str(id) + ")"
    elif (len(RId1) == 1):
        expr_id1_to_id = "Id=" + str(RId1[0])
    else:  # 未测试#
        expr_id1_to_id = "empty"
    thread1 = Thread(target=send_request, args=[expr_id1_to_id])
    thread1.start()

    if (len(auids) >= 2):
        expr_ids_to_AuId_of_id1 = "Or(Composite(AA.AuId=" + str(auids[0]) + "),Composite(AA.AuId=" + str(
            auids[1]) + "))"
        for AuId in auids[2:]:
            expr_ids_to_AuId_of_id1 = "Or(" + expr_ids_to_AuId_of_id1 + ",Composite(AA.AuId=" + str(AuId) + "))"
    elif (len(auids) == 1):
        expr_ids_to_AuId_of_id1 = "Composite(AA.AuId=" + str(auids[0]) + ")"
    else:
        expr_ids_to_AuId_of_id1 = "empty"
    thread3 = Thread(target=send_request, args=[expr_ids_to_AuId_of_id1])
    thread3.start()

    ids_to_AuId = [x["Id"] for x in data2[1:]]  # AuId写的paper

    thread1.join()
    print "ids_to_auid: ", time() - start_time
    for paper in request_data[expr_id1_to_id]:  # id1引用的文章
        auids_rid_id1 = [x["AuId"] for x in paper["AA"]]
        if int(id2) in auids_rid_id1:
            result.append([int(id1), paper["Id"], int(id2)])  # id-id-auid
        intersection_id = list(set(paper["RId"]) & set(ids_to_AuId))
        for id in intersection_id:
            result.append([int(id1), paper["Id"], id, int(id2)])  # id-id-id-auid

    print "time ids to AuId to id:", time() - start_time
    xids1 = get_xids(data1[0])
    for paper in data2[1:]:  # AuId写的文章
        auids_ids_AuId = [x["AuId"] for x in paper["AA"]]
        intersection_auid = list(set(auids_ids_AuId) & set(auids))
        for auid in intersection_auid:
            result.append([int(id1), auid, paper["Id"], int(id2)])  # id-auid-id-auid
        if paper.has_key("F"):
            for fid in paper["F"]:
                if fid["FId"] in xids1:
                    result.append([int(id1), fid["FId"], paper["Id"], int(id2)])  # id-fid-id-auid 未测试
        if paper.has_key("C"):
            if paper["C"]["CId"] in xids1:
                result.append([int(id1), paper["C"]["CId"], paper["Id"], int(id2)])  # id-cid-id-auid 未测试
        if paper.has_key("J"):
            if paper["J"]["JId"] in xids1:
                result.append([int(id1), paper["J"]["JId"], paper["Id"], int(id2)])  # id-jid-id-auid 未测试

    thread3.join()
    print "time ids to auid of id1:", time() - start_time
    afids_to_AuId = []
    for paper in data2[1:]:
        for item in paper['AA']:
            if item.has_key('AfId') and str(item['AuId']) == id2:  # 注意有的作者没有机构的情况
                AfId = item['AfId']
                afids_to_AuId = afids_to_AuId if AfId in afids_to_AuId else afids_to_AuId + [
                    AfId]
    # 我的天啊这个求交集的方式有点丑...
    for paper in request_data[expr_ids_to_AuId_of_id1]:
        for item in paper['AA']:
            if item.has_key('AfId') and item['AuId'] in auids and item["AfId"] in afids_to_AuId:  # 注意有的作者没有机构的情况
                res_item = [int(id1), item["AuId"], item["AfId"], int(id2)]
                if not (res_item in result):
                    result.append(res_item)  # id-auid-afid-auid
    print len(result), result
    return result


def query3(data1, data2, id1, id2):# AuId-Id
    global start_time
    result = []

    ids_to_AuId = [x["Id"] for x in data1[1:]] # AuId的文章
    if int(id2) in ids_to_AuId:
        result.append([int(id1), int(id2)]) #auid-id

    expr_ids_to_id = "RId="+id2
    thread1 = Thread(target=send_request, args=[expr_ids_to_id])
    thread1.start()

    auids = [x["AuId"] for x in data2[0]["AA"]] #id2的作者们
    if (len(auids) >= 2):
        expr_ids_to_AuId_of_id2 = "Or(Composite(AA.AuId=" + str(auids[0]) + "),Composite(AA.AuId=" + str(
            auids[1]) + "))"
        for AuId in auids[2:]:
            expr_ids_to_AuId_of_id2 = "Or(" + expr_ids_to_AuId_of_id2 + ",Composite(AA.AuId=" + str(AuId) + "))"
    elif (len(auids) == 1):
        expr_ids_to_AuId_of_id2 = "Composite(AA.AuId=" + str(auids[0]) + ")"
    else:
        expr_ids_to_AuId_of_id2 = "empty"
    thread3 = Thread(target=send_request, args=[expr_ids_to_AuId_of_id2])
    thread3.start()

    thread1.join()
    ids_to_id2 = [x["Id"] for x in request_data[expr_ids_to_id]] #引用了id2的文章
    intersection_ids = list(set(ids_to_id2) & set(ids_to_AuId))
    for id in intersection_ids:
        result.append([int(id1), id, int(id2)]) #auid-id-id
    for paper in data1[1:]: #AuId的文章
        intersection_ids = list(set(paper["RId"]) & set(ids_to_id2))
        for id in intersection_ids:
            res_item = [int(id1), paper["Id"], id, int(id2)]
            if not(res_item in result):
                result.append(res_item) #auid-id-id-id
    afids_to_AuId = []
    xids_to_id2 = get_xids(data2[0])

    for paper in data1[1:]:  # auid写的文章
        for item in paper['AA']:
            if item.has_key('AfId') and str(item['AuId']) == id1:  # 求auid所属的afid
                AfId = item['AfId']
                afids_to_AuId = afids_to_AuId if AfId in afids_to_AuId else afids_to_AuId + [
                    AfId]
            if item["AuId"] in auids:  # 求auid-id-auid-id
                res_item = [int(id1), paper["Id"], item["AuId"], int(id2)]  # auid-id-auid-id
                result.append(res_item)
        xids_to_id_of_auid = get_xids(paper)
        intersection_xids = list(set(xids_to_id2) & set(xids_to_id_of_auid))
        for xid in intersection_xids:
            result.append([int(id1), paper["Id"], xid, int(id2)])  # auid-id-xid-id

    thread3.join()
    print "time ids to auid of id1:", time() - start_time

    # 我的天啊这个求交集的方式有点丑...
    for paper in request_data[expr_ids_to_AuId_of_id2]:#id2的作者写的其他文章
        for item in paper['AA']:
            if item.has_key('AfId') and item['AuId'] in auids and item["AfId"] in afids_to_AuId:  # 注意有的作者没有机构的情况
                res_item = [int(id1), item["AfId"], item["AuId"], int(id2)]
                if not (res_item in result):
                    result.append(res_item)  # auid-afid-auid-id


    print len(result), result
    return result


def query4(data1, data2, id1, id2):# AuId-AuId
    global start_time
    result = []

    afids_to_AuId1 = []
    for paper in data1[1:]:
        for item in paper['AA']:
            if item.has_key('AfId') and str(item['AuId']) == id1:
                AfId = item['AfId']
                afids_to_AuId1 = afids_to_AuId1 if AfId in afids_to_AuId1 else afids_to_AuId1 + [AfId]
    afids_to_AuId2 = []
    for paper in data2[1:]:
        for item in paper['AA']:
            if item.has_key('AfId') and str(item['AuId']) == id2:
                AfId = item['AfId']
                afids_to_AuId2 = afids_to_AuId2 if AfId in afids_to_AuId2 else afids_to_AuId2 + [
                    AfId]
    intersection_afids = list(set(afids_to_AuId1) & set(afids_to_AuId2))
    for afid in intersection_afids:
        result.append([int(id1), afid, int(id2)]) #auid-afid-auid
    ids_to_auid1 = [x["Id"] for x in data1[1:]]
    ids_to_auid2 = [x["Id"] for x in data2[1:]]
    for id in list(set(ids_to_auid1) & set(ids_to_auid2)):
        result.append([int(id1), id, int(id2)])
    for paper in data1[1:]: #auid1写的文章
        intersection_ids = list(set(paper["RId"]) & set(ids_to_auid2))
        for id in intersection_ids:
            res_item = [int(id1), paper["Id"], id, int(id2)]
            if not(res_item in result):
                result.append(res_item) #auid-id-id-auid


    print len(result), result


if __name__ == "__main__":
    # query("2065555069", "2167884222") # id-id-id-id test case
    # query("2065555069", "2008785686") #id-id,id-id-id,id-id-id-id test case
    # query("2065555069", "2137247190") #id-id-auid-id
    # query("2065555069", "94203212")  # id-auid-id-id
    # query("2065555069", "2138170171") #id-fid-id
    # query("2065555069", "2158241055")  # id-fid-id-id
    # query("2065555069", "2113671972")  # id-jid-id-id
    # query("2065555069", "2147264455")  # id-id-fid-id. 这组数据结果很差.thread2比较慢.引用第二篇文章的比较多,有4475篇
    # query("2065555069", "2034161127") #id-id-jid-id

    # query("2034161127", "56455408") #id-auid
    # query("2008785686", "197077518")  # id-id-auid
    # query("2008785686", "2178980517")  # id-id-id-auid
    # query("2008785686", "2134482257")  # id-auid-id-auid
    # query("2008785686", "671475949")  # id-fid-id-auid   (⊙o⊙)…长度太长爆掉了.可以拿来做后来的修正测试
    # Thread(target=query, args=("2008785686", "2130310236")).start()
    # Thread(target=query, args=("2008785686", "2117094889")).start()
    # query("2008785686", "2130310236")  # id-jid-id-auid 爆掉了...
    # query("2008785686", "2117094889")  # id-auid-afid-auid

    # query("2134482257", "1969892834") #auid-id
    # query("2134482257", "2008785686") # auid-id-id
    # query("1912875929", "2292217923") #auid-id-id 炒鸡厉害的压力测试.. 花了300s...
    # query("2134482257", "1996937460") # auid-id-id-id
    # query("2134482257", "1653619892")  # auid-afid-auid-id
    # query("2134482257", "2086248897")  # auid-id-auid-id
    # query("2134482257", "2021221018")  # auid-id-xid-id
    # Thread(target=query, args=("2134482257", "2021221018")).start()

    # query("2134482257", "2243222307") #auid-afid-auid
    # query("2134482257", "2307611627")  # auid-id-auid
    # query("2134482257", "2288329424")  # auid-id-id-auid

    query("189831743", "2147152072")