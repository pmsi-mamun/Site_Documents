__author__ = 'atalhan'

from pymongo import MongoClient
import json




class MongoWrapper:
    """
    Wrapper class to expose mongodb api
    """
    def __init__(self, server, port):
        self.mongoClient = MongoClient(server, port)

    def get_data_for_field(self, data_bucket, data_bucket_type, field, value):
        db = self.mongoClient[data_bucket]
        tbl = db[data_bucket_type]
        rtnData = []

        for data in tbl.find({field: str(value)}):
            rtnData.append(data)

        return rtnData


    def bulk_index(self, mongoClient, data_bucket, data_bucket_type, data_rows, doc_id_field):
        db = self.mongoClient[data_bucket]
        tbl = db[data_bucket_type]
        tbl.insert(data_rows)
        ###if len(data) > 0:
            ###"""MongoDb don't allow '.' in key name """
            ###tbl.insert({k.replace('.',''):v for k, v in data.items()})


    def get_all_data_from_collection(self,data_bucket,data_bucket_type):
        db = self.mongoClient[data_bucket]
        tbl = db[data_bucket_type]
        rtnData = []

        for data in tbl:
            rtnData.append(data)


