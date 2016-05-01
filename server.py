# -*- coding: utf-8 -*-
from calculate import *
from flask import Flask, request, Response
import json
app = Flask(__name__)


@app.route('/', methods = ['GET'])
def hello_world():
    if len(request.args) != 2:
        return "wrong query!"
    parms = request.args.lists()

    param1 = parms[0]
    param2 = parms[1]
    id1, id2 = param1[1], param2[1]


    # 4种query
    if param1[0] == 'Id' and param2[0] == 'Id':
        print '[Id, Id] query'
        result = query1(id1, id2)
    elif param1[0] == 'Id' and param2[0] == 'AA.AuId':
        print '[Id, AA.AuId] query'
        result = query2(id1, id2)
    elif param1[0] == 'AA.AuId' and param2[0] == 'Id':
        print '[AA.AuId, Id] query'
        result = query3(id1, id2)
    elif param1[0] == 'AA.AuId' and param2[0] == 'AA.AuId':
        print '[AA.AuId, AA.AuId] query'
        result = query4(id1, id2)

    return Response(response=json.dumps(result), status=200, mimetype="application/json")


if __name__ == '__main__':
    app.run()