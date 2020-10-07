from os import path
import sys
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
import datetime
from helper import Helper
import requests
from bs4 import BeautifulSoup
from crawler import *
import hashlib
import logging
from DbOps import DbOperations,QueryType
import urllib3

urllib3.disable_warnings()

class fujitsu(object):

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

    def crawler(self):
        try:
            response = crawler.MakeRequest(self.url,"Get")
            soup = BeautifulSoup(response.content, "html.parser")
            data = []
            boxs = soup.find_all("ul",{"class":'filterlist'})
            for box in boxs:
                date = ''.join(box.p.strong.text.split(',')[-2:])
                date = Helper.parse_date(date.lstrip().rstrip())
                datadict = Helper.get_news_dict()
                datadict.update({"url":"https://www.fujitsu.com"+box.find("a")['href']})

                url = "https://www.fujitsu.com"+box.find("a")['href']
                # Check if already present
                unqUrl = hashlib.md5(url.encode()).hexdigest()
                chkIsExists = DbOperations.GetData(self.news_collection, {"news_url_uid": str(unqUrl)}, {},
                                                   QueryType.one)
                if (chkIsExists):
                    print("Already saved. url - ( " + url + " )")
                    continue

                description = self.fetchDescription("https://www.fujitsu.com" + box.find("a")['href'])
                datadict.update({
                    "date": date,
                    "news_provider": "fujitsu",
                    "formatted_sub_header": box.find("a").text,
                    "publishedAt": date,
                    "description": description,
                    "title": description,
                    "link": url,
                    "text":box.p.text,
                    "company_id" : "fujitsu",
                    "news_url_uid" : hashlib.md5(("https://www.fujitsu.com"+box.find("a")['href']).encode()).hexdigest()

                })
                data.append(datadict)

            DbOperations.InsertIntoMongo(self.news_collection,data)
        except Exception as e:
            self.logger.error(f"Error Occured : \n", exc_info=True)

    def fetchDescription(self,url):
        article = ''
        try:
            description = crawler.MakeRequest(url, "Get")
            articlesoup = BeautifulSoup(description.content, 'html.parser')
            articles = articlesoup.find("div", {"class": "bannercopy"})
            articles = articles.find_all("p")
            for ar in articles:
                article += ar.text + '\n'
        except Exception as e:
            self.logger.error(f"Error Occured : \n", exc_info=True)
        return article

#Create and configure logger
logging.basicConfig(filename="news_scraping_logs.log",
                    format='%(asctime)s %(message)s',
                    filemode='a')
logger = logging.getLogger()
obj = fujitsu('https://www.fujitsu.com/global/about/resources/news/',logger=logger)
obj.crawler()

news_collection = Helper.getNewsCollection()
processed_collection = Helper.getProcessNewsCollection()
news_log_collection = Helper.getLogCollection()
isInserted,rowCount = Helper.processNews(news_collection,processed_collection,'fujitsu')
print('Total rows added Process collection => ' + str(rowCount))

# UPDATING LOG COLLECTION
if (isInserted):
    Helper.makeLog(news_log_collection,processed_collection,'fujitsu')