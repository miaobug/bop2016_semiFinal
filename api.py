# -*- coding: utf-8 -*-
import httplib, urllib, base64, json
# from enum import Enum

# 参考返回格式(无CID)
# {
# "expr" : "Id=2065555069",
# "entities" :
# [{ "logprob":-17.747, "Id":2065555069, "Ti":"engineering education and the development of expertise", "RId":[2008785686, 2137247190, 1806940748, 2158534319, 2115669451, 1985858077, 2113671972, 2115548862, 1988565851, 2113079434, 1975912968, 2090305479, 2004509884, 1972739577, 2144447430, 2142111360, 2098290305, 2151330869, 2059767950, 2145395129, 2015294929, 1996571370, 2111591752, 2058239882, 2155319506, 2090427002, 1980239773, 2100702557, 13921348, 1628764219, 108489841], "AA":[{"AuId":2007341405}, {"AuId":2179778153}, {"AuId":2270958858}, {"AuId":2249923743}], "F":[{"FId":177704700}, {"FId":108583219}], "J":{"JId":73400788} }]
# }

# 参考返回格式 CID
# {
# "expr" : "Id=2165751040",
# "entities" :
# [{ "logprob":-19.704, "Id":2165751040, "Ti":"simulation based game learning environments building and sustaining a fish tank", "RId":[1994701398, 1676365973, 2035061933, 314405074, 2151715145, 2119649150, 2110893343, 1964392774, 1534728038, 2106969952], "AA":[{"AuId":2278995570}, {"AuId":2098746329}], "F":[{"FId":42525527}, {"FId":97812054}, {"FId":183322885}, {"FId":66024118}, {"FId":108583219}, {"FId":500300565}, {"FId":19417346}, {"FId":119857082}, {"FId":41008148}], "C":{"CId":1121720292} }]
# }

request_data = {} # 多线程send_request的返回值
hostname = 'oxfordhk.azure-api.net'

def Id_Info(conn, Id):
    Id = 'Id=' + Id +'&'
    # composite = "Composite(F.FN='deep%20learning')&"
    count = 'count=10000&'
    attributes = 'attributes=Id,RId,F.FId,C.CId,J.JId,AA.AuId&'
    key = 'subscription-key=f7cc29509a8443c5b3a5e56b0e38b5a6'
    conn.request("GET","/academic/v1.0/evaluate?expr="+ Id + count + attributes + key)
    response = conn.getresponse()
    data = json.loads(response.read())['entities'][0]
    AuId = [x['AuId'] for x in data['AA']] if data.has_key('AA') else []
    FId = [x['FId'] for x in data['F']] if data.has_key('F') else []
    RId = data['RId'] if data.has_key('RId') else []
    JId = [data['J']['JId']] if data.has_key('J') else []      #JId只会有一个,还要不要数组?
    CID = [data['C']['CId']] if data.has_key('C') else []      #CId只会有一个,还要不要数组?
    return AuId, FId, RId, JId, CID


def FId2Id(conn, FId):
    composite = 'Composite(F.FId=' + FId + ')&'
    count = 'count=1000000&'                            # 我特么也不知道一共有多少篇
    attributes = 'attributes=Id&'
    key = 'subscription-key=f7cc29509a8443c5b3a5e56b0e38b5a6'
    conn.request("GET", "/academic/v1.0/evaluate?expr=" + composite + count + attributes + key)
    response = conn.getresponse()
    data = json.loads(response.read())['entities']
    return [x['Id'] for x in data]


def JId2Id(conn, JId):
    composite = 'Composite(J.JId=' + JId + ')&'
    count = 'count=1000000&'  # 我特么也不知道一共有多少篇
    attributes = 'attributes=Id&'
    key = 'subscription-key=f7cc29509a8443c5b3a5e56b0e38b5a6'
    conn.request("GET", "/academic/v1.0/evaluate?expr=" + composite + count + attributes + key)
    response = conn.getresponse()
    data = json.loads(response.read())['entities']
    return [x['Id'] for x in data]

def CId2Id(conn, CId):
    composite = 'Composite(C.CId=' + CId + ')&'
    count = 'count=1000000&'  # 我特么也不知道一共有多少篇
    attributes = 'attributes=Id&'
    key = 'subscription-key=f7cc29509a8443c5b3a5e56b0e38b5a6'
    conn.request("GET", "/academic/v1.0/evaluate?expr=" + composite + count + attributes + key)
    response = conn.getresponse()
    data = json.loads(response.read())['entities']
    return [x['Id'] for x in data]

def AuId2Id(conn, AuId):
    composite = 'Composite(AA.AuId=' + AuId + ')&'
    count = 'count=1000000&'  # 我特么也不知道一共有多少篇
    attributes = 'attributes=Id&'
    key = 'subscription-key=f7cc29509a8443c5b3a5e56b0e38b5a6'
    conn.request("GET", "/academic/v1.0/evaluate?expr=" + composite + count + attributes + key)
    response = conn.getresponse()
    data = json.loads(response.read())['entities']
    return [x['Id'] for x in data]

def AuId2AFId(conn, AuId):
    composite = 'Composite(AA.AuId=' + AuId + ')&'
    count = 'count=1000000&'  # 我特么也不知道一共有多少篇
    attributes = 'attributes=AA.AuId,AA.AfId&'
    key = 'subscription-key=f7cc29509a8443c5b3a5e56b0e38b5a6'
    conn.request("GET", "/academic/v1.0/evaluate?expr=" + composite + count + attributes + key)
    response = conn.getresponse()
    data = json.loads(response.read())['entities']

    # 如下行所示,每篇paper对应多个AfId, 需要合并(我是智障,把其他作者的机构也合起来了,亏我写了那么久)
    # [{u'AA': [{u'AfId': 200719446}, {u'AfId': 200719446}], u'logprob': -19.704}, {u'AA': [{u'AfId': 200719446} ...
    # for AA in data:
    #     AfIds = list(set(AfIds).union(set([item['AfId'] for item in AA['AA'] if item.has_key('AfId')])))   #注意有的作者没有机构的情况

    AfIds = []
    for AA in data:
        for item in AA['AA']:
            # print str(item['AuId'])==AuId, item.has_key('AfId')  #卧槽json解析以后还变成int了...
            if item.has_key('AfId') and str(item['AuId']) == AuId:  #注意有的作者没有机构的情况
                AfId = item['AfId']
                AfIds =  AfIds if AfId in AfIds else AfIds + [AfId]
    return AfIds

# 慎用, 慢的不知道到哪里去了, 有的查询可能要跑几分钟
def AFId2AuId(conn, AfId):
    composite = 'Composite(AA.AfId=' + AfId + ')&'
    count = 'count=1000000&'  # 我特么也不知道一共有多少篇
    attributes = 'attributes=AA.AuId,AA.AfId&'
    key = 'subscription-key=f7cc29509a8443c5b3a5e56b0e38b5a6'
    conn.request("GET", "/academic/v1.0/evaluate?expr=" + composite + count + attributes + key)
    response = conn.getresponse()
    data = json.loads(response.read())['entities']

    AuIds = []
    for AA in data:
        for item in AA['AA']:
            if item.has_key('AfId') and str(item['AfId']) == AfId:  # 简直智障, 我用AfId查还能查出这个字段没东西的paper
                AuId = item['AuId']
                AuIds = AuIds if AuId in AuIds else AuIds + [AuId]
    # print len(AuIds)
    return AuIds

def send_request(expr):
    global request_data
    if expr == "empty":
        request_data[expr] = []
        return
    conn = httplib.HTTPSConnection(hostname)
    count = '&count=100000000&'
    attributes = 'attributes=RId,C.CId,J.JId,F.FId,Id,CC,AA.AuId,AA.AfId&'
    key = 'subscription-key=f7cc29509a8443c5b3a5e56b0e38b5a6'
    # print expr
    print "/academic/v1.0/evaluate?expr=" + expr + count + attributes + key
    conn.request("GET", "/academic/v1.0/evaluate?expr=" + expr + count + attributes + key)
    response = conn.getresponse()
    data = json.loads(response.read())['entities']
    request_data[expr] = data

def get_exprs(ids, mode):
    exprs = []
    if mode == "id":
        if(len(ids) >= 1):
            expr = "Id=" + str(ids[0])
            for id in ids[1:]:
                if expr <= 1700: # 长度具体限制还是要测一下
                    expr = "Or(" + expr + ",Id=" + str(id) + ")"
                else:
                    exprs.append(expr)
                    expr = "Id=" + str(id)
        exprs.append(expr)
    elif mode == "auid":
        if(len(ids) >= 1):
            expr = "composite(AA.AuId="+str(ids[0])+")"
            for id in ids[1:]:
                if expr <= 1700:
                    expr = "Or(" + expr + ",Composite(AA.AuId=" + str(id) + "))"
                else:
                    exprs.append(expr)
                    expr = "composite(AA.AuId="+str(id)+")"
    return exprs

def get_xids(paper):
    xids = []
    if paper.has_key("F"):
        xids = [x["FId"] for x in paper["F"]]
    if paper.has_key("C"):
        xids.append(paper["C"]["CId"])
    if paper.has_key("J"):
        xids.append(paper["J"]["JId"])
    return xids

if __name__ == '__main__':
    try:
        print "-------------Tests below-------------------"
        conn = httplib.HTTPSConnection(hostname)   #这个注意加在前面
        # time.sleep(10)
        Id_Info(conn, '2065555069')
        FId2Id(conn, '42525527')
        JId2Id(conn, '73400788')
        CId2Id(conn, '1121720292')
        AuId2Id(conn, '2278995570')
        print AuId2AFId(conn, '2098746329')
        # AFId2AuId(conn, '97018004')
        AFId2AuId(conn, '5518804')
        conn.close()
    except BaseException, e:
        print(str(e))
    print "-------------Tests ends-------------------"
