from os import path
import sys
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
import requests
from helper import Helper
from DbOps import *
import json
import hashlib
import logging
import urllib3

urllib3.disable_warnings()

class abb(object):
    def __init__(self, url, body=None, headers=None, logger=None):
        """
        Set initial paramaeters

        :param url: scraping url
        :param body: scraping url body
        :param headers: scraping url header
        :param logger: logger object
        """
        self.url = url
        self.body = body
        self.headers = headers
        self.news_collection = Helper.getNewsCollection()
        self.logger = logger
    def crawler_news(self):
        try:
            header = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36",
                "Accept": "*/*"
            }

            loop = True
            offset = 1
            while loop:
                bulk_obj = DbOperations.Get_object_for_bulkop(False, self.news_collection)
                response = requests.get(self.url.format(off_set=offset), headers=header)
                news_data = json.loads(response.content.decode('utf-8'))

                # check if we found any news data or not
                if news_data.__contains__('count') and news_data['count'] > 0:
                    for news in news_data['news']:
                        news_dict = Helper.get_news_dict()

                        url = "https://new.abb.com/" + news['path']
                        # Check if already present
                        unqUrl = hashlib.md5(url.encode()).hexdigest()
                        chkIsExists = DbOperations.GetData(self.news_collection, {"news_url_uid": str(unqUrl)}, {},
                                                           QueryType.one)
                        if (chkIsExists):
                            print("Already saved. url - ( " + url + " )")
                            continue

                        news_dict.update(
                            {"title": news['title'], "news_title_uid": hashlib.md5(news['title'].encode()).hexdigest(),
                             "url": url, "link": url, "news_url_uid": hashlib.md5(url.encode()).hexdigest(),
                             "description": news['description'], "text": news['description'],
                             "publishedAt": Helper.parse_date(news['publishedOn']), 'date': Helper.parse_date(news['publishedOn']), "publishedAt_scrapped": Helper.parse_date(news['publishedOn']),
                             "company_id": "abb", "ticker": "abb_scrapped", "industry_name": "abb",
                             "news_provider": "abb"})
                        bulk_obj.insert(news_dict)
                        if len(bulk_obj._BulkOperationBuilder__bulk.__dict__['ops']) > 1:
                            bulk_obj.execute()
                            bulk_obj = DbOperations.Get_object_for_bulkop(False, self.news_collection)
                else:
                    print("No data found")
                offset += 1
                loop = False
                if len(bulk_obj._BulkOperationBuilder__bulk.__dict__['ops']) > 0:
                    bulk_obj.execute()
        except Exception as e:
            self.logger.error(f"Error Occured : \n",exc_info=True)

#Create and configure logger
logging.basicConfig(filename="news_scraping_logs.log",
                    format='%(asctime)s %(message)s',
                    filemode='a')
logger = logging.getLogger()
url = "https://global.abb/group/en/media/releases/group.abbglobal-news-search.json?feeds=abb:feeds/group_functions/corporate_communications/group_press_releases&feedsOperator=OR&limit=10&offset={off_set}"
news_obj = abb(url,logger=logger)
news_obj.crawler_news()


news_collection = Helper.getNewsCollection()
processed_collection = Helper.getProcessNewsCollection()
news_log_collection = Helper.getLogCollection()
isInserted,rowCount = Helper.processNews(news_collection,processed_collection,'abb')
print('Total rows added Process collection => ' + str(rowCount))

# UPDATING LOG COLLECTION
if (isInserted):
    Helper.makeLog(news_log_collection,processed_collection,'abb')