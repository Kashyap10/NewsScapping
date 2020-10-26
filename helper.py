import datetime
import pandas as pd
from DbOps import DbOperations,QueryType,QueryType

class Helper(object):

    @staticmethod
    def get_news_dict():
        return {
                "publishedAt_scrapped": datetime.datetime.now(),
                "endt":datetime.datetime.now(),
                "url":"",
                "date": "",
                "news_provider": "",
                "formatted_text": "",
                "is_scrapped": 1,
                "news_url_uid": "",
                "ticker": "News",
                "formatted_sub_header": "",
                "publishedAt": "",
                "industry_name": "",
                "description":"" ,
                "filetype": "",
                "title": "",
                "text": "",
                "company_id": "",
                "news_title_uid": "",
                "topic_name": "News",
                "link": "",
                "sub_header": ""
            }
    @staticmethod
    def processNews(news_collection,processed_collection,company):
            isInserted = 0
            rowCount = 0
            for row in DbOperations.GetData(news_collection,{"is_used": {'$exists': False},"news_provider":company},{}):
                try:
                    DbOperations.InsertIntoMongo(processed_collection,row)
                    isInserted = 1
                    print('Success in inserting Process collection => [url: "' + row['url'] + '"]')
                    DbOperations.Update_oneMongo(news_collection,{"news_url_uid": row['news_url_uid']},{"$set": {"is_used": 1}})
                    rowCount = rowCount + 1
                except Exception as e:
                    print('Error in inserting Process collection => [url: "' + row['url'] + '"]', e)
                    pass
            return isInserted,rowCount

    @staticmethod
    def processNewsBasedOnTitle(news_collection, processed_collection, company):
        isInserted = 0
        rowCount = 0
        for row in DbOperations.GetData(news_collection, {"is_used": {'$exists': False}, "news_provider": company}, {}):
            try:
                DbOperations.InsertIntoMongo(processed_collection, row)
                isInserted = 1
                print('Success in inserting Process collection => [title: "' + row['title'] + '"]')
                DbOperations.Update_oneMongo(news_collection, {"news_title_uid": row['news_title_uid']},
                                             {"$set": {"is_used": 1}})
                rowCount = rowCount + 1
            except Exception as e:
                print('Error in inserting Process collection => [title: "' + row['title'] + '"]', e)
                pass
        return isInserted, rowCount

    @staticmethod
    def getProcessNewsCollection():
        today = datetime.datetime.now()
        year = today.year
        month = '{:02d}'.format(today.month)
        day = '{:02d}'.format(today.day)
        strNewsDate = str(year) + str(month) + str(day)
        return strNewsDate

    @staticmethod
    def getNewsCollection():
        return 'all_news'

    @staticmethod
    def getLogCollection():
        return 'news_processed_log'

    @staticmethod
    def makeLog(newlogcollection,processedcollection,companyname):
        print("Your Hourly Collection is - " + processedcollection)
        log = {}
        log['db_name'] = processedcollection
        log['processed_by_all_type_news'] = 1
        log['endt'] = datetime.datetime.now()
        log['script_scrapped_name'] = str(companyname)+'_daily_scrapping_python'
        DbOperations.InsertIntoMongo(newlogcollection, log)

    @staticmethod
    def parse_date(date_string, format=None):
            if format is not None:
                    return pd.to_datetime(date_string, format=format)
            elif ((date_string.find('-') <= 2 and date_string.find('.') == -1 and date_string.find('/') == -1) or (
                    date_string.find('.') <= 2 and date_string.find('-') == -1 and date_string.find('/') == -1) or (
                          date_string.find('/') <= 2 and date_string.find('-') == -1 and date_string.find('.') == -1)):
                    return pd.to_datetime(date_string, dayfirst=True)
            else:
                    return pd.to_datetime(date_string, yearfirst=True)