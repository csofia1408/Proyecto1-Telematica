from flask import Flask, request
from flask_restful import Api,Resource,reqparse, abort, fields, marshal_with
import requests

app = Flask(__name__)
api = Api(app)

data = []

filesPostArgs = reqparse.RequestParser()
filesPostArgs.add_argument("fileName", type=str, help="", required=True)
filesPostArgs.add_argument("dataNodeIp", type=str, help="", required=True)
filesPostArgs.add_argument("blockPosition", type=int, help="", required=True)
filesPostArgs.add_argument("totalBlocks", type=int, help="", required=True)

indexFilesFields = {
	'fileName': fields.String,
    "dataNodeIp": fields.String,
	'blockPosition': fields.Integer,
    'totalBlocks': fields.Integer,
}

class indexFiles(Resource):
    @marshal_with(indexFilesFields)
    def post(self):
        args = filesPostArgs.parse_args()
        data.append(args)
        return 200
    
    def get(self):
        fileName = request.args.get('fileName')
        print(fileName)
        print(data)
        filtered_objects = [obj for obj in data if obj['fileName'] == fileName]
        totalBlocks = filtered_objects[0]['totalBlocks']

        filtered_objects = filtered_objects[:totalBlocks]
        print(len(filtered_objects))
        print(totalBlocks)

        return {"files": filtered_objects}
    
api.add_resource(indexFiles, "/indexFiles")

if __name__== "__main__":
    app.run(debug=True)