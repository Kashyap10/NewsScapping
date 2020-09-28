import sys
import time
import demjson
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError, BulkWriteError, ServerSelectionTimeoutError, OperationFailure
from Environment import *


class MongoThings:
    if LOCAL_ENV:
        ConnUrl = demjson.decode(config['mongo-url']['Url'])[STAGE]

class DbName:
    DsyhMongo = 'news_scrapping'

class QueryType:
    many = "many"
    one = "one"

class DbOperations(object):
    ClientConn = None
    clientid = None
    channelid = None
    def __init__(self):
        DbOperations.ClientConn = DbOperations.GetConnection(DbName.DsyhMongo)

    @staticmethod
    def GetConnection(dbName, count=0):

        clientConn = DbOperations.ClientConn

        try:

            if (clientConn is None):

                try:
                    clientConn = MongoClient(MongoThings.ConnUrl)
                    clientConn.server_info()
                    clientConn = clientConn[dbName]

                except (ServerSelectionTimeoutError, OperationFailure) as e:
                    print (e)
                    if count < 5:
                        time.sleep(5)
                        DbOperations.GetConnection(DbName, count + 1)
                    else:
                        print("Connection is not established")
                        exit(0)
                except Exception as e:
                    print(e)
        except Exception as e:
            print (str(e))

        return clientConn

    @staticmethod
    def InsertIntoMongo(collection, dataToInert, count=0):

        """
        :param collection: name of the collection
        :param dataToInert: list or dict type.
        :return: False when insertion is failed, on successful for one insertion returns inserted_id in ObjectId, for a list returns True
        """

        clientConn = DbOperations.GetConnection(DbName.DsyhMongo)
        returnvalue = {'nInserted': 0, 'inserted_ids': [], 'nDuplicates': 0, 'anyDuplicates': False,
                       'nTotal': len(dataToInert), 'writeErrors': []}

        try:

            if (type(dataToInert) is list):
                val = clientConn[collection].insert_many(dataToInert, ordered=False)
                returnvalue['nInserted'] = len(val.inserted_ids)
                returnvalue['inserted_ids'] = val.inserted_ids
            elif (type(dataToInert) is dict):
                val = clientConn[collection].insert_one(dataToInert)
                returnvalue['nInserted'] = 1
                returnvalue['inserted_ids'] = [val.inserted_id]
            else:
                print ("In insert Nothing")

        except BulkWriteError as e:
            details = e.details
            code = details['writeErrors'][0]['code']
            if code == 11000:
                duplicatevalues = len(dataToInert) - details['nInserted']
                returnvalue['nInserted'] = details['nInserted']
                returnvalue['nDuplicates'] = duplicatevalues
                returnvalue['anyDuplicates'] = True
                returnvalue['writeErrors'] = details['writeErrors']
            else:
                returnvalue['writeErrors'] = details['writeErrors']

        except DuplicateKeyError as er:
            returnvalue['nInserted'] = 0
            returnvalue['nDuplicates'] = 1
            returnvalue['anyDuplicates'] = True
            returnvalue['writeErrors'] = er.details


        except (ServerSelectionTimeoutError, OperationFailure) as e:
            print (e)
            if count < 5:
                time.sleep(5)
                DbOperations.InsertIntoMongo(collection, dataToInert, count + 1)
            else:
                print ("Error while inserting")
                exit(0)

        return returnvalue

    @staticmethod
    def Get_object_for_bulkop(isordered, collection_name, count=0):

        """
        :param isordered: true of false
        :return: if true returns an bulk object of ordered else object for unordered
        """

        bulkop = None
        try:
            clientConn = DbOperations.GetConnection(DbName.DsyhMongo)
            if (isordered):
                bulkop = clientConn[collection_name].initialize_ordered_bulk_op()
            else:
                bulkop = clientConn[collection_name].initialize_unordered_bulk_op()
        except (ServerSelectionTimeoutError, OperationFailure) as e:
            print (e)
            if count < 5:
                time.sleep(5)
                DbOperations.Get_object_for_bulkop(isordered, collection_name, count + 1)
            elif count >= 5:
                print ("maximum try exceeds")
                exit(0)
        return bulkop

    @staticmethod
    def Update_oneMongo(collection, selection, fieldsToUpdate, upsert=False, count=0, array_filters=None):

        """
        :param collection: Name of the collection
        :param selection: Conditions (in dict only)
        :param fieldsToUpdate: Fields to update
        :param upsert: Extra options
        :return:
        """
        try:

            clientConn = DbOperations.GetConnection(DbName.DsyhMongo)

            clientConn[collection].update_one(selection, fieldsToUpdate, upsert, array_filters=array_filters)
        except (OperationFailure, ServerSelectionTimeoutError) as e:
            print(e)
            if count < 5:
                time.sleep(5)
                DbOperations.Update_oneMongo(collection, selection, fieldsToUpdate, upsert, count + 1,
                                             array_filters=array_filters)
            elif count >= 5:
                print("maximum try exceeds")
                exit(0)

    @staticmethod
    def Update_manyMongo(collection, selection, fieldsToUpdate, upsert=False, count=0):

        """
        :param collection: Name of the collection
        :param selection: Conditions (in dict only)
        :param fieldsToUpdate: Fields to update
        :param upsert: Extra options
        :return:
        """
        try:
            clientConn = DbOperations.GetConnection(DbName.DsyhMongo)
            clientConn[collection].update_many(selection, fieldsToUpdate, upsert)
        except (ServerSelectionTimeoutError, OperationFailure) as e:
            print(e)
            if count < 5:
                time.sleep(5)
                DbOperations.Update_manyMongo(collection, selection, fieldsToUpdate, upsert, count + 1)
            elif count >= 5:
                print("maximum try exceeds")
                exit(0)

    @staticmethod
    def Get_Aggregate_Data(collection, pipeline, count=0):

        result = []
        clientConn = DbOperations.GetConnection(DbName.DsyhMongo)

        try:
            logs = clientConn[collection]
            if len(pipeline) > 0:
                result = logs.aggregate(pipeline=pipeline, allowDiskUse=True)
                result = list(result)
            else:
                print ("Pipeline is Blank")
        except (ServerSelectionTimeoutError, OperationFailure) as e:
            print(e)
            if count < 5:
                time.sleep(5)
                DbOperations.Get_Aggregate_Data(collection, pipeline, count + 1)
            elif count >= 5:
                print("maximum try exceeds")
                exit(0)
        return result

    @staticmethod
    def GetData(collection, whereDict, projections, querytype=QueryType.many, limit=0, skip=0, sorting=None, count=0):
        """
        :param collection : Name of the collection
        :param whereDict : Key and value for you want to search
        :param projections : Values you want to get. Pass empty list, if you want to search all.
        :param querytype :
        :param limit :
        :param skip :
        """

        clientConn = DbOperations.GetConnection(DbName.DsyhMongo)

        result = {}
        projDict = {}

        try:
            if (len(projections) != 0):
                projDict = projections
            logs = clientConn[collection]

            val = {}

            if (len(projections) == 0):
                if limit == 0 and skip == 0:
                    if (querytype == QueryType.many):
                        val = logs.find(whereDict)
                    elif (querytype == QueryType.one):
                        val = logs.find_one(whereDict)
                elif limit != 0 and skip != 0:
                    if (querytype == QueryType.many):
                        val = logs.find(whereDict).skip(skip).limit(limit)
                    elif (querytype == QueryType.one):
                        val = logs.find_one(whereDict).skip(skip).limit(limit)
                elif limit != 0 and skip == 0:
                    if (querytype == QueryType.many):
                        val = logs.find(whereDict).limit(limit)
                    elif (querytype == QueryType.one):
                        val = logs.find_one(whereDict).limit(limit)
                elif limit == 0 and skip != 0:
                    if (querytype == QueryType.many):
                        val = logs.find(whereDict).skip(skip)
                    elif (querytype == QueryType.one):
                        val = logs.find_one(whereDict).skip(skip)
            else:
                if limit == 0 and skip == 0:
                    if (querytype == QueryType.many):
                        val = logs.find(whereDict, projDict)
                    elif (querytype == QueryType.one):
                        val = logs.find_one(whereDict, projDict)
                elif limit != 0 and skip != 0:
                    if (querytype == QueryType.many):
                        if sorting != None:
                            val = logs.find(whereDict, projDict).skip(skip).sort(
                                sorting).limit(limit)
                        else:
                            val = logs.find(whereDict, projDict).skip(skip).limit(
                                limit)
                    elif (querytype == QueryType.one):
                        val = logs.find_one(whereDict, projDict).skip(skip).limit(
                            limit)
                elif limit != 0 and skip == 0:
                    if (querytype == QueryType.many):
                        if sorting != None:
                            val = logs.find(whereDict, projDict).sort(
                                sorting).limit(limit)
                        else:
                            val = logs.find(whereDict, projDict).limit(limit)
                    elif (querytype == QueryType.one):
                        val = logs.find_one(whereDict, projDict).limit(limit)
                elif limit == 0 and skip != 0:
                    if (querytype == QueryType.many):
                        val = logs.find(whereDict, projDict).skip(skip)
                    elif (querytype == QueryType.one):
                        val = logs.find_one(whereDict, projDict).skip(skip)

            if querytype == QueryType.many and sorting != None and limit == 0:
                val = val.sort(sorting)

            if val is not None:
                if (querytype == QueryType.many):
                    result = list(val)
                else:
                    result = val
        except (OperationFailure, ServerSelectionTimeoutError) as e:
            print(e)
            if count < 5:
                time.sleep(5)
                DbOperations.GetData(collection, whereDict, projections, querytype, limit, skip, sorting, count + 1)
            elif count >= 5:
                print("maximum try exceeds")
                exit(0)
        return result

