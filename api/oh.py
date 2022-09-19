
# from crypt import methods
from flask import Flask
from flask import request,make_response, jsonify

from __main__ import app

@app.route('/oh',methods=['GET'])
def oh():
    return 'oh'





