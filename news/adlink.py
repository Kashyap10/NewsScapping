from os import path
import sys
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
import datetime
from helper import Helper
import requests
from bs4 import BeautifulSoup
from crawler import *
from DbOps import DbOperations,QueryType
import hashlib
import logging
import urllib3

urllib3.disable_warnings()
import urllib3

urllib3.disable_warnings()

class adlinktech(object):

    def __init__(self,url,body=None,headers=None,logger=None):
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

    def crawler(self):
        try:
            bulk_obj = DbOperations.Get_object_for_bulkop(False, self.news_collection)
            response = crawler.MakeRequest(self.url,"Get")
            soup = BeautifulSoup(response.content, "html.parser")
            boxs = soup.find_all("div",{"class":'listCol sort-item news-item'})
            for box in boxs:
                datadict = Helper.get_news_dict()
                datadict.update({"url":"https://www.adlinktech.com"+box.find("a")['href']})
                url = "https://www.adlinktech.com"+box.find("a")['href']
                # Check if already present
                unqUrl = hashlib.md5(url.encode()).hexdigest()
                chkIsExists = DbOperations.GetData(self.news_collection, {"news_url_uid": str(unqUrl)}, {},
                                                   QueryType.one)
                if (chkIsExists):
                    print("Already saved. url - ( " + url + " )")
                    continue
                date,description = self.fetchDescription("https://www.adlinktech.com" + box.find("a")['href'])
                datadict.update({
                    "date": Helper.parse_date(date),
                    "news_provider": "adlink",
                    "formatted_sub_header": box.find("div",{"class":"contentText"}).text,
                    "publishedAt": Helper.parse_date(date),
                    "description": description,
                    "title": box.find("div",{"class":"contentText"}).text,
                    "link": "https://www.adlinktech.com"+box.find("a")['href'],
                    "ticker": "adlink_scrapped", "industry_name": "adlink",
                    "news_title_uid": hashlib.md5(box.find("div",{"class":"contentText"}).text.encode()).hexdigest(),
                    "text":description,
                    "company_id" : "adlink",
                    "news_url_uid" : hashlib.md5(("https://www.adlinktech.com"+box.find("a")['href']).encode()).hexdigest()

                })
                bulk_obj.insert(datadict)

                if len(bulk_obj._BulkOperationBuilder__bulk.__dict__['ops']) > 100:
                    bulk_obj.execute()
                    bulk_obj = DbOperations.Get_object_for_bulkop(False, self.news_collection)

            if len(bulk_obj._BulkOperationBuilder__bulk.__dict__['ops']) > 0:
                bulk_obj.execute()
        except Exception as e:
            self.logger.error(f"Error Occured : \n", exc_info=True)

    def fetchDescription(self,url):
        article = ''
        date = ''
        try:
            description = crawler.MakeRequest(url, "Get")
            articlesoup = BeautifulSoup(description.content, 'html.parser')
            date = articlesoup.find("div", {"class": "newsPage-date floatL"}).text
            article = articlesoup.find("div", {"class": "contentText"}).text


        except Exception as e:
            self.logger.error(f"Error Occured : \n", exc_info=True)
        return date,article
#Create and configure logger
logging.basicConfig(filename="news_scraping_logs.log",
                    format='%(asctime)s %(message)s',
                    filemode='a')
logger = logging.getLogger()
obj = adlinktech('https://www.adlinktech.com/en/CompanyNews',logger=logger)
obj.crawler()

news_collection = Helper.getNewsCollection()
processed_collection = Helper.getProcessNewsCollection()
news_log_collection = Helper.getLogCollection()
isInserted,rowCount = Helper.processNews(news_collection,processed_collection,'adlink')
print('Total rows added Process collection => ' + str(rowCount))

# UPDATING LOG COLLECTION
if (isInserted):
    Helper.makeLog(news_log_collection,processed_collection,'adlink')