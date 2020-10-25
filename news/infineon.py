from os import path
import sys
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
import datetime
import hashlib
import logging
from DbOps import DbOperations,QueryType
import urllib3

urllib3.disable_warnings()
import json

from bs4 import BeautifulSoup

from crawler import *
from helper import Helper


class infineon(object):
    def __init__(self, url, body=None, headers=None, logger=None):
        self.url = url
        self.body = body
        self.headers = headers
        self.logger = logger
        self.news_collection = Helper.getNewsCollection()

    def crawler_news(self):
        try:
            loop = True
            offset = 0
            while loop:
                bulk_obj = DbOperations.Get_object_for_bulkop(False, self.news_collection)
                response = crawler.MakeRequest(self.url, 'Post', postData=self.body.format(off_set=offset),
                                               headers=self.headers)
                if response is not None:
                    news_data = json.loads(response.content.decode('utf-8'))

                    if news_data.__contains__('count') and news_data['count'] > 0:
                        for news in news_data['pages']['items']:
                            print(news)
                            date = Helper.parse_date(news['news_date'])
                            if date:
                                if date.year < datetime.datetime.now().year:
                                    break

                            url = "https://www.infineon.com/" + news['url']
                            # Check if already present
                            unqUrl = hashlib.md5(url.encode()).hexdigest()
                            chkIsExists = DbOperations.GetData(self.news_collection,
                                                               {"news_url_uid": str(unqUrl)}, {}, QueryType.one)
                            if (chkIsExists):
                                print("Already saved. url - ( " + url + " )")
                                continue

                            news_dict = Helper.get_news_dict()
                            description = self.fetchDescription("https://www.infineon.com/" + news['url'])
                            news_dict.update(
                                {"date": Helper.parse_date(news['news_date']),
                                 "news_provider": "Infineon",
                                 "url": "https://www.infineon.com/" + news['url'],
                                 "formatted_sub_header": "",
                                 "publishedAt": Helper.parse_date(news['news_date']),
                                 "description": description,
                                 "title": news['title'],
                                 "ticker": "Infineon_scrapped", "industry_name": "Infineon",
                                 "news_title_uid": hashlib.md5(news['title'].encode()).hexdigest(),
                                 "link": "https://www.infineon.com/" + news['url'],
                                 "text": description,
                                 "company_id": "Infineon",
                                 "news_url_uid": hashlib.md5(
                                     ("https://www.infineon.com/" + news['url']).encode()).hexdigest()
                                 })

                            bulk_obj.insert(news_dict)
                            if len(bulk_obj._BulkOperationBuilder__bulk.__dict__['ops']) > 1:
                                bulk_obj.execute()
                                bulk_obj = DbOperations.Get_object_for_bulkop(False, self.news_collection)
                    else:
                        print("No data found")
                        loop = False
                    offset += 10
                    if len(bulk_obj._BulkOperationBuilder__bulk.__dict__['ops']) > 0:
                        bulk_obj.execute()

                else:
                    break
        except Exception as e:
            self.logger.error(f"Error Occured : \n",exc_info=True)

    def fetchDescription(self, url):
        article = ''
        try:
            description = crawler.MakeRequest(url, "Get")
            articlesoup = BeautifulSoup(description.content, 'html.parser')
            articles = articlesoup.find("div", {"class": "copy"})
            for ar in articles.find_all("p"):
                article = article + " " + ar.text
        except Exception as e:
            self.logger.error(f"Error Occured : \n", exc_info=True)
        return article

#Create and configure logger
logging.basicConfig(filename="news_scraping_logs.log",
                    format='%(asctime)s %(message)s',
                    filemode='a')
logger = logging.getLogger()
url = "https://www.infineon.com/cms/en/services/ajax/search/pressReleases"
news_obj = infineon(url,
                    "term=&offset={off_set}&max_results=10&lang=en&news_category_ids=news%2Fcategory%2Ffinancial-press%2F&news_category_ids=news%2Fcategory%2Fquarterly-report%2F&parent_folder=/en/",
                    {"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                     "X-Requested-With": "XMLHttpRequest", "Accept": "application/json, text/javascript, */*; q=0.01"},logger=logger)
news_obj.crawler_news()

news_collection = Helper.getNewsCollection()
processed_collection = Helper.getProcessNewsCollection()
news_log_collection = Helper.getLogCollection()
isInserted,rowCount = Helper.processNews(news_collection,processed_collection,'Infineon')
print('Total rows added Process collection => ' + str(rowCount))

# UPDATING LOG COLLECTION
if (isInserted):
    Helper.makeLog(news_log_collection,processed_collection,'Infineon')
