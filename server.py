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
    result = query(id1, id2)

    return Response(response=json.dumps(result), status=200, mimetype="application/json")


if __name__ == '__main__':
    app.run()