
# from crypt import methods
from flask import Flask
from flask import request,make_response, jsonify

from __main__ import app

@app.route('/item',methods=['GET'])
def item():
    return 'it works'



