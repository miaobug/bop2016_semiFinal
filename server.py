# -*- coding: utf-8 -*-
from calculate import *
from flask import Flask, request, Response
import json
import logging

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(funcName)s %(message)s',
                    # datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='bop2016.log',
                    filemode='w')

app = Flask(__name__)


@app.route('/', methods = ['GET'])
def hello_world():
    logging.debug('request begin')
    if len(request.args) != 2:
        return "wrong query!"
    parms = request.args.lists()

    param1 = parms[0]
    param2 = parms[1]
    id1, id2 = param1[1][0], param2[1][0]
    logging.debug('params get')
    result = query(str(id2), str(id1))
    logging.debug('query ends')

    return Response(response=json.dumps(result), status=200, mimetype="application/json")


if __name__ == '__main__':
    app.run(host="0.0.0.0")