from os import path
import sys
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from os import path
import sys
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from helper import Helper
import logging
from crawler import crawler
from bs4 import BeautifulSoup
from DbOps import DbOperations,QueryType
import json,hashlib

class ppg(object):
    def __init__(self, url, body=None, headers=None, logger=None):
        self.url = url
        self.body = body
        self.headers = headers
        self.logger = logger
        self.news_collection = Helper.getNewsCollection()

    def crawler_news(self):
        try:
            response = crawler.MakeRequest(self.url,'Get',postData=self.body,headers=self.headers)
            soup = BeautifulSoup(response.content, 'html.parser')
            bulk_obj = DbOperations.Get_object_for_bulkop(False,self.news_collection)
            news_data = json.loads(response.content.decode('utf-8'))
            if news_data:
                for news in news_data['GetPressReleaseListResult']:

                    news_dict = Helper.get_news_dict()

                    title = news['Headline'] if 'Headline' in news else ""
                    url = news['LinkToUrl'] if 'LinkToUrl' in news else ""

                    # Check if already present
                    unqUrl = hashlib.md5(url.encode()).hexdigest()
                    chkIsExists = DbOperations.GetData(self.news_collection, {"news_url_uid": str(unqUrl)}, {},
                                                       QueryType.one)
                    if (chkIsExists):
                        print("Already saved. url - ( " + url + " )")
                        continue

                    description = news['ShortBody'] if 'ShortBody' in news else ""
                    news_url_uid = news['PressReleaseId'] if 'PressReleaseId' in news else ""
                    publish_date = Helper.parse_date(news['PressReleaseDate']) if 'PressReleaseDate' in news else ""

                    news_dict.update(
                        {"title": title, "url": url, "formatted_sub_header": title, "description": description, "link": url,
                         "publishedAt":publish_date,'date':publish_date,"news_url_uid":news_url_uid,"news_provider": "ppg"})

                    bulk_obj.insert(news_dict)

                    if len(bulk_obj._BulkOperationBuilder__bulk.__dict__['ops']) >100:
                        bulk_obj.execute()
                        bulk_obj = DbOperations.Get_object_for_bulkop(False,self.news_collection)

                if len(bulk_obj._BulkOperationBuilder__bulk.__dict__['ops']) > 0:
                    bulk_obj.execute()
        except Exception as e:
            self.logger.error(f"Error Occured : \n",exc_info=True)

#Create and configure logger
logging.basicConfig(filename="news_scraping_logs.log",
                    format='%(asctime)s %(message)s',
                    filemode='a')
logger = logging.getLogger()
url = "https://news.ppg.com/feed/PressRelease.svc/GetPressReleaseList?apiKey=BF185719B0464B3CB809D23926182246&LanguageId=1&bodyType=3&pressReleaseDateFilter=3&categoryId=953a78e4-99ff-4cc5-bfef-b5432b56da87&pageSize=-1&pageNumber=0&tagList=&includeTags=true&year=-1&excludeSelection=1"
news_obj = ppg(url,logger=logger)
news_obj.crawler_news()

news_collection = Helper.getNewsCollection()
processed_collection = Helper.getProcessNewsCollection()
news_log_collection = Helper.getLogCollection()
isInserted,rowCount = Helper.processNews(news_collection,processed_collection,'ppg')
print('Total rows added Process collection => ' + str(rowCount))

# UPDATING LOG COLLECTION
if (isInserted):
    Helper.makeLog(news_log_collection,processed_collection,'ppg')
