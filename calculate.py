# -*- coding: utf-8 -*-
import httplib
from api import *
from threading import Thread
from time import time
import logging

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] (%(funcName)s) %(message)s',
                    filename='bop2016.log',
                    filemode='w')
# from Queue import Queue

# todo list:
# 增加本地缓存机制/减少接口调用
# 提高求交集的性能
# 检查同步机制!!!!!(request_data                        √
# GET请求的长度限制检查                                 现在感觉基本不会超了...
# SSL链接建立优化                                        ×
# 环的检查                                              √
# paper id 2292217923 的 RId 15万多...来个压力测试吧      √
# 优化RId查询
# 优化并行结构
# 找性能瓶颈
# 多个query测试                                         √
# 检查重复......                                        √
# 优化链接
# list->set优化                                        √

# 性能分析: python的dict采用哈希实现

# start_time = time()

#done
def query(id1, id2):
    logging.debug("query starts")
    # global start_time
    # start_time = time()
    # conn = httplib.HTTPSConnection(hostname)
    expr1 = "Or(Id="+id1+",Composite(AA.AuId=" +id1+ "))"
    expr2 = "Or(Id="+id2+",Composite(AA.AuId=" +id2+ "))"
    #这两次请求好像比较慢一些
    thread1 = Thread(target=send_request, args=[expr1])
    thread2 = Thread(target=send_request, args=[expr2])
    thread1.start()
    thread2.start()
    thread1.join()
    thread2.join()
    logging.debug("first 2 requests finished")
    # print "time: ", time() - start_time

    data1 = request_data[expr1]
    data2 = request_data[expr2]


    len1 = len(data1)
    len2 = len(data2)

    # 这里假定每个作者肯定写一篇以上的文章咯~ mr
    if(len1 <= 1):
        if(len2 <= 1):
            return query1(data1, data2, id1, id2)
        else:
            return query2(data1, data2, id1, id2)
    elif(len2 <= 1):
        return query3(data1, data2, id1, id2)
    else:
        return query4(data1, data2, id1, id2)


def query1(data1, data2, id1, id2): #Id-Id
    # global start_time
    logging.debug("query1 starts")
    result = []
    RId1 =  data1[0]['RId']
    # logging.debug("id-id path search ends")

    expr_id1_to_id = get_exprs(RId1, "id")
    key_id1_to_id = "id1_to_id"+id1
    thread1 = Thread(target=get_info, args=[expr_id1_to_id, key_id1_to_id])
    thread1.start()

    #请求引用了id2的paper
    expr_id_to_id2 = "RId=" + id2
    thread2 = Thread(target=send_request, args=[expr_id_to_id2])
    thread2.start()

    #请求id1的作者们的paper
    expr_id1_to_AuId = ["Composite(AA.AuId="+str(x['AuId'])+")" for x in data1[0]["AA"]] #id1的作者列表
    key_id1_to_AuId = "id1_to_AuId"+id1
    thread3 = Thread(target=get_info, args=[expr_id1_to_AuId, key_id1_to_AuId])
    thread3.start()

    if int(id2) in RId1:
        result.append([int(id1), int(id2)])  # id-id
    xids1 = get_xids(data1[0])
    xids2 = get_xids(data2[0])
    intersection_xid = xids1 & xids2
    for xid in intersection_xid:
        result.append([int(id1), xid, int(id2)]) # id-xid-id

    thread1.join()

    logging.debug('请求id1引用的paper 结束')

    # print "time id1_to_id: ", time() - start_time
    auids_to_id2 = set(x["AuId"] for x in data2[0]["AA"])  # 写了id2的作者

    # 这个循环和下面的循环合并是不是更快?
    for paper in request_data[key_id1_to_id]: # id1引用的paper
        #依赖于thread1
        if int(id2) in paper["RId"]:
            # print "haha"+id1+"-"+str(paper["Id"])+"-"+id2
            result.append([int(id1), paper["Id"], int(id2)]) #id-id-id
        if paper.has_key("F"):
            fids = set(x["FId"] for x in paper["F"])
            intersection_fid = set(fids) & xids2
            for fid in intersection_fid:
                result.append([int(id1), paper["Id"], fid, int(id2)])  # id-id-fid-id ..
        if paper.has_key("C"):
            if paper["C"]["CId"] in xids2:
                result.append([int(id1), paper["Id"], paper["C"]["CId"], int(id2)])  # id-id-cid-id ..
        if paper.has_key("J"):
            if paper["J"]["JId"] in xids2:
                result.append([int(id1), paper["Id"], paper["J"]["JId"], int(id2)])  # id-id-jid-id ..
        auids = set(x["AuId"] for x in paper["AA"])  # 写了id1引用的文章的作者.
        intersection_auid = auids & auids_to_id2
        for auid in intersection_auid:
            result.append([int(id1), paper["Id"], auid, int(id2)])  # id-id-auid-id

    thread2.join()

    logging.debug('请求引用了id2的paper ends')
    # print "time id to id2: ", time() - start_time
    ids_to_id2 = set(x["Id"] for x in request_data[expr_id_to_id2]) #引用了id2的文章序号列表

    for paper in request_data[key_id1_to_id]: #id1引用的文章
        # 这块依赖于thread2的结果
        intersection_id= ids_to_id2 & set(paper["RId"])
        for id in intersection_id:
            result.append([int(id1), paper["Id"], id, int(id2)]) #id-id-id-id

    #这块依赖于thread2的结果
    for paper in request_data[expr_id_to_id2]: #引用了id2的文章.
        if(paper.has_key("F")):
            fids = set(x["FId"] for x in paper["F"])
            intersection_fid = fids & xids1
            for fid in intersection_fid:
                result.append([int(id1), fid, paper["Id"], int(id2)]) #id-fid-id-id ..
        if(paper.has_key("C")):
            if paper["C"]["CId"] in xids1:
                result.append([int(id1), paper["C"]["CId"], paper["Id"], int(id2)]) #id-cid-id-id ..
        if(paper.has_key("J")):
            if paper["J"]["JId"] in xids1:
                result.append([int(id1), paper["J"]["JId"], paper["Id"], int(id2)]) #id-jid-id-id ..

    thread3.join()
    logging.debug('请求id1的作者们的paper ends')
    # print "time id1 to AuId to id:", time() - start_time
    for paper in request_data[key_id1_to_AuId]:
        if paper["Id"] in ids_to_id2:
            temp_auids = set(x["AuId"] for x in paper["AA"])
            id1_to_AuIds = temp_auids & set(x["AuId"] for x in data1[0]["AA"])
            for auid in id1_to_AuIds:
                res_item = [int(id1), auid, paper["Id"], int(id2)]
                if not (res_item in result):
                    result.append(res_item) #id-auid-id-id

    # print "query1", len(result), result
    logging.debug('query1 ends, result length is ' + str(len(result)) + ', result is ' + str(result))
    return result


def query2(data1, data2, id1, id2):  # Id-AuId
    # global start_time
    logging.debug('query2 starts')
    result = []

    auids = [x["AuId"] for x in data1[0]["AA"]]  # id1的作者们
    if int(id2) in auids:
        result.append([int(id1), int(id2)])  # id-auid

    #请求id1引用的paper
    RId1 = data1[0]['RId']

    expr_id1_to_id = get_exprs(RId1, "id")
    key_id1_to_id = "id1_to_id"+id1
    thread1 = Thread(target=get_info, args=[expr_id1_to_id, key_id1_to_id])
    thread1.start()

    #请求id1的作者写的paper
    # if (len(auids) >= 2):
    #     expr_ids_to_AuId_of_id1 = "Or(Composite(AA.AuId=" + str(auids[0]) + "),Composite(AA.AuId=" + str(
    #         auids[1]) + "))"
    #     for AuId in auids[2:]:
    #         expr_ids_to_AuId_of_id1 = "Or(" + expr_ids_to_AuId_of_id1 + ",Composite(AA.AuId=" + str(AuId) + "))"
    # elif (len(auids) == 1):
    #     expr_ids_to_AuId_of_id1 = "Composite(AA.AuId=" + str(auids[0]) + ")"
    # else:
    #     expr_ids_to_AuId_of_id1 = "empty"
    expr_ids_to_AuId_of_id1 = ["Composite(AA.AuId="+str(auid)+")" for auid in auids]
    # key_ids_to_AuId_of_id1 = "ids_to_AuId_of_id1" + id1
    threads3 = [Thread(target=send_request, args=[expr]) for expr in expr_ids_to_AuId_of_id1]
    # for expr in expr_ids_to_AuId_of_id1:
    #     thread3 = Thread(target=send_request, args=[expr_ids_to_AuId_of_id1, key_ids_to_AuId_of_id1])
    for thread in threads3:
        thread.start()

    ids_to_AuId = set(x["Id"] for x in data2[1:])  # auid(id2)写的paper

    thread1.join()
    # print "ids_to_auid: ", time() - start_time
    logging.debug('id1引用的paper信息 请求ends, expr= '+expr_id1_to_id.__str__())

    for paper in request_data[key_id1_to_id]:  # id1引用的文章
        auids_rid_id1 = set(x["AuId"] for x in paper["AA"])
        if int(id2) in auids_rid_id1:
            result.append([int(id1), paper["Id"], int(id2)])  # id-id-auid
        intersection_id = set(paper["RId"]) & ids_to_AuId
        for id in intersection_id:
            result.append([int(id1), paper["Id"], id, int(id2)])  # id-id-id-auid

    # print "time ids to AuId to id:", time() - start_time
    xids1 = get_xids(data1[0])
    for paper in data2[1:]: # AuId写的文章
        auids_ids_AuId = set(x["AuId"] for x in paper["AA"])
        intersection_auid = auids_ids_AuId & set(auids)
        for auid in intersection_auid:
            result.append([int(id1), auid, paper["Id"], int(id2)])  # id-auid-id-auid
        if paper.has_key("F"):
            for fid in paper["F"]:
                if fid["FId"] in xids1:
                    result.append([int(id1), fid["FId"], paper["Id"], int(id2)])  # id-fid-id-auid
        if paper.has_key("C"):
            if paper["C"]["CId"] in xids1:
                result.append([int(id1), paper["C"]["CId"], paper["Id"], int(id2)])  # id-cid-id-auid
        if paper.has_key("J"):
            if paper["J"]["JId"] in xids1:
                result.append([int(id1), paper["J"]["JId"], paper["Id"], int(id2)])  # id-jid-id-auid

    # print "time ids to auid of id1:", time() - start_time
    logging.debug('请求id1的作者写的paper ends, expr = '+ expr_ids_to_AuId_of_id1.__str__())

    afids_to_AuId = set([])

    for paper in data2[1:]:
        if paper.has_key("AA"):
            for item in paper['AA']:
                if item.has_key('AfId') and str(item['AuId']) == id2:  # 注意有的作者没有机构的情况
                    AfId = item['AfId']
                    if not (AfId in afids_to_AuId):
                        afids_to_AuId.add(AfId)
    i = 0;
    for thread in threads3:
        thread.join()
        for paper in request_data[expr_ids_to_AuId_of_id1[i]]:
            for item in paper["AA"]:
                if item.has_key('AfId') and item["AfId"] in afids_to_AuId and item['AuId'] == auids[i]:  # 注意有的作者没有机构的情况
                    res_item = [int(id1), item["AuId"], item["AfId"], int(id2)]
                    if not (res_item in result):
                        result.append(res_item) #id-auid-afid-auid
        i += 1

    # 我的天啊这个求交集的方式有点丑...
    # for paper in request_data[key_ids_to_AuId_of_id1]:
    #     for item in paper['AA']:
    #         if item.has_key('AfId') and item["AfId"] in afids_to_AuId and item['AuId'] in auids:  # 注意有的作者没有机构的情况
    #             res_item = [int(id1), item["AuId"], item["AfId"], int(id2)]
    #             if not (res_item in result):
    #                 result.append(res_item)  # id-auid-afid-auid

    # print "query2", len(result), result
    logging.debug('query2 ends, result length is ' + str(len(result)) + ', result is ' + str(result))
    print len(result), result
    return result



def query3(data1, data2, id1, id2):# AuId-Id
    # global start_time
    result = []

    ids_to_AuId = set(x["Id"] for x in data1[1:]) # AuId的文章
    if int(id2) in ids_to_AuId:
        result.append([int(id1), int(id2)]) #auid-id

    #请求引用了id2的paper
    expr_ids_to_id = "RId="+id2
    thread1 = Thread(target=send_request, args=[expr_ids_to_id])
    thread1.start()

    #请求id2的作者们写的paper
    auids = [x["AuId"] for x in data2[0]["AA"]]
    # if (len(auids) >= 2):
    #     expr_ids_to_AuId_of_id2 = "Or(Composite(AA.AuId=" + str(auids[0]) + "),Composite(AA.AuId=" + str(
    #         auids[1]) + "))"
    #     for AuId in auids[2:]:
    #         expr_ids_to_AuId_of_id2 = "Or(" + expr_ids_to_AuId_of_id2 + ",Composite(AA.AuId=" + str(AuId) + "))"
    # elif (len(auids) == 1):
    #     expr_ids_to_AuId_of_id2 = "Composite(AA.AuId=" + str(auids[0]) + ")"
    # else:
    #     expr_ids_to_AuId_of_id2 = "empty"
    expr_ids_to_AuId_of_id2 = ["Composite(AA.AuId="+str(auid)+")" for auid in auids]
    key_ids_to_AuId_of_id2 = "ids_to_AuId_of_id2"+id2
    thread3 = Thread(target=get_info, args=[expr_ids_to_AuId_of_id2, key_ids_to_AuId_of_id2])
    thread3.start()

    thread1.join()

    logging.debug('请求引用了id2的paper ends, expr = '+expr_ids_to_id)

    ids_to_id2 = set(x["Id"] for x in request_data[expr_ids_to_id]) #引用了id2的文章
    intersection_ids = ids_to_id2 & ids_to_AuId
    for id in intersection_ids:
        result.append([int(id1), id, int(id2)]) #auid-id-id
    for paper in data1[1:]: #AuId的文章
        intersection_ids = set(paper["RId"]) & ids_to_id2
        for id in intersection_ids:
            res_item = [int(id1), paper["Id"], id, int(id2)]
            # if not(res_item in result):
            result.append(res_item) #auid-id-id-id
    afids_to_AuId = set()
    xids_to_id2 = get_xids(data2[0])

    for paper in data1[1:]:  # auid写的文章
        for item in paper['AA']:
            if item.has_key('AfId') and str(item['AuId']) == id1:  # 求auid所属的afid,在thread3.join()以后用到
                AfId = item['AfId']
                if not (AfId in afids_to_AuId):
                    afids_to_AuId.add(AfId)
            if item["AuId"] in auids:  # 求auid-id-auid-id
                res_item = [int(id1), paper["Id"], item["AuId"], int(id2)]  # auid-id-auid-id
                result.append(res_item)
        xids_to_id_of_auid = get_xids(paper)
        intersection_xids = xids_to_id2 & xids_to_id_of_auid
        for xid in intersection_xids:
            result.append([int(id1), paper["Id"], xid, int(id2)])  # auid-id-xid-id

    thread3.join()
    # print "time ids to auid of id1:", time() - start_time
    logging.debug('请求id2的作者们写的paper ends, expr = '+ expr_ids_to_AuId_of_id2.__str__())

    # 我的天啊这个求交集的方式有点丑...
    for paper in request_data[key_ids_to_AuId_of_id2]:#id2的作者写的其他文章
        for item in paper['AA']:
            if item.has_key('AfId') and item["AfId"] in afids_to_AuId and item['AuId'] in auids :  # 注意有的作者没有机构的情况
                res_item = [int(id1), item["AfId"], item["AuId"], int(id2)]
                if not (res_item in result):
                    result.append(res_item)  # auid-afid-auid-id



    # print "query3", len(result), result
    logging.debug('query3 ends, result length is ' + str(len(result)) + ', result is ' + str(result))
    return result


def query4(data1, data2, id1, id2):# AuId-AuId
    # global start_time
    logging.debug('query4 starts')
    result = []

    #求auid1所在的机构的afid
    afids_to_AuId1 = set()
    for paper in data1[1:]:
        for item in paper['AA']:
            if item.has_key('AfId') and str(item['AuId']) == id1:
                AfId = item['AfId']
                if not AfId in afids_to_AuId1:
                    afids_to_AuId1.add(AfId)
    #求auid2所在的机构的afid
    afids_to_AuId2 = set()
    for paper in data2[1:]:
        for item in paper['AA']:
            if item.has_key('AfId') and str(item['AuId']) == id2:
                AfId = item['AfId']
                if not AfId in afids_to_AuId2:
                    afids_to_AuId2.add(AfId)

    intersection_afids = afids_to_AuId1 & afids_to_AuId2
    for afid in intersection_afids:
        result.append([int(id1), afid, int(id2)]) #auid-afid-auid

    ids_to_auid1 = set(x["Id"] for x in data1[1:])
    ids_to_auid2 = set(x["Id"] for x in data2[1:])
    for id in ids_to_auid1 & ids_to_auid2:
        result.append([int(id1), id, int(id2)])
    for paper in data1[1:]: #auid1写的文章
        intersection_ids = set(paper["RId"]) & ids_to_auid2
        for id in intersection_ids:
            res_item = [int(id1), paper["Id"], id, int(id2)]
            # if not(res_item in result):  #好像没有必要检查重复
            result.append(res_item) #auid-id-id-auid

    # print "query4", len(result), result
    logging.debug('query4 ends, result length is ' + str(len(result)) + ', result is ' + str(result))

    return result


if __name__ == "__main__":
    query("2065555069", "2167884222") # id-id-id-id 315
    # query("2065555069", "2008785686") #id-id,id-id-id,id-id-id-id 76
    # query("2065555069", "2137247190") #id-id-auid-id 79
    # query("2065555069", "94203212")  # id-auid-id-id 50
    # query("2065555069", "2138170171") #id-fid-id 6
    # query("2065555069", "2158241055")  # id-fid-id-id
    # query("2065555069", "2113671972")  # id-jid-id-id
    # query("2065555069", "2147264455")  # id-id-fid-id. 这组数据结果很差.thread2比较慢.引用第二篇文章的比较多,有4475篇
    # query("2065555069", "2034161127") #id-id-jid-id
    #
    # query("2034161127", "56455408") #id-auid 471
    # query("2008785686", "56455408") #id-auid 135
    # query("2008785686", "197077518")  # id-id-auid 26
    # query("2008785686", "2178980517")  # id-id-id-auid 16
    # query("2008785686", "2134482257")  # id-auid-id-auid 67
    # query("2008785686", "671475949")  # id-fid-id-auid   (⊙o⊙)…长度太长爆掉了.可以拿来做后来的修正测试 120
    # Thread(target=query, args=("2008785686", "2130310236")).start()
    # Thread(target=query, args=("2008785686", "2117094889")).start()
    # query("2008785686", "2130310236")  # id-jid-id-auid 爆掉了... 36
    # query("2008785686", "2117094889")  # id-auid-afid-auid 1

    # query("2134482257", "1969892834") #auid-id 74
    # query("2134482257", "2008785686") # auid-id-id 71
    # query("1912875929", "2292217923") #auid-id-id 炒鸡厉害的压力测试.. 花了80s... 35
    # query("2134482257", "1996937460") # auid-id-id-id 47
    # query("2134482257", "1653619892")  # auid-afid-auid-id 1
    # query("2134482257", "2086248897")  # auid-id-auid-id 15
    # query("2134482257", "2021221018")  # auid-id-xid-id 33
    # Thread(target=query, args=("2134482257", "2021221018")).start()

    # query("2134482257", "2243222307") #auid-afid-auid 0
    # query("2134482257", "2307611627")  # auid-id-auid 3
    # query("2134482257", "2288329424")  # auid-id-id-auid 1j

    # query("189831743", "2147152072")
    # query("2126125555", "2060367530") #5616

    # 官方测试数据:
    # 189831743
    # 2147152072
    # 2310280492
    # 2332023333
    # 2180737804
    # 2251253715
    # 14, 18, 1
