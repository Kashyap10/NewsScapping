from os import path
import sys
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
import datetime
from helper import Helper
import requests
from bs4 import BeautifulSoup
from DbOps import DbOperations,QueryType
from crawler import *
import hashlib
import logging
import urllib3

urllib3.disable_warnings()

class pemex(object):

    def __init__(self, url, body=None, headers=None, logger=None):
        self.url = url
        self.body = body
        self.headers = headers
        self.logger = logger
        self.news_collection = Helper.getNewsCollection()

    def crawler(self):
        try:
            response = crawler.MakeRequest(self.url,"Get")
            soup = BeautifulSoup(response.content, "html.parser")
            data = []
            boxs = soup.find_all("div",{"class":'news-box span3 left'})
            for box in boxs:
                datadict = Helper.get_news_dict()
                url = "https://www.pemex.com"+box.find("a")['href']
                # Check if already present
                unqUrl = hashlib.md5(url.encode()).hexdigest()
                chkIsExists = DbOperations.GetData(self.news_collection, {"news_url_uid": str(unqUrl)}, {},
                                                   QueryType.one)
                if (chkIsExists):
                    print("Already saved. url - ( " + url + " )")
                    continue

                datadict.update({"url":"https://www.pemex.com"+box.find("a")['href']})
                description = self.fetchDescription("https://www.pemex.com" + box.find("a")['href'])
                datadict.update({
                    "date": box.find("p",{"class":"news-meta news-date"}).text,
                    "news_provider": "pemex",
                    "formatted_sub_header": box.find("div",{"class":"ms-WPBody h2"}).text,
                    "publishedAt": Helper.parse_date(box.find("p",{"class":"news-meta news-date"}).text),
                    "description": description,
                    "title": box.find("div",{"class":"ms-WPBody h2"}).text,
                    "link": self.url,
                    "text":description,
                    "company_id" : "pemex",
                    "news_url_uid" : hashlib.md5(("https://www.pemex.com"+box.find("a")['href']).encode()).hexdigest()

                })
                data.append(datadict)

            DbOperations.InsertIntoMongo(self.news_collection,data)
        except Exception as e:
            self.logger.error(f"Error Occured : \n",exc_info=True)

    def fetchDescription(self,url):
        article = ''
        try:
            description = crawler.MakeRequest(url, "Get")
            articlesoup = BeautifulSoup(description.content, 'html.parser')
            article = articlesoup.find("div", {"class": "article-content"}).text
        except Exception as e:
            self.logger.error(f"Error Occured : \n",exc_info=True)
        return article

#Create and configure logger
logging.basicConfig(filename="news_scraping_logs.log",
                    format='%(asctime)s %(message)s',
                    filemode='a')
logger = logging.getLogger()
obj = pemex('https://www.pemex.com/en/Paginas/default.aspx',logger=logger)
obj.crawler()

news_collection = Helper.getNewsCollection()
processed_collection = Helper.getProcessNewsCollection()
news_log_collection = Helper.getLogCollection()
isInserted,rowCount = Helper.processNews(news_collection,processed_collection,'pemex')
print('Total rows added Process collection => ' + str(rowCount))

# UPDATING LOG COLLECTION
if (isInserted):
    Helper.makeLog(news_log_collection,processed_collection,'pemex')