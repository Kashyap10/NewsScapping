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

class BD(object):

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
            counter = 1
            data = []
            while True:

                response = crawler.MakeRequest(self.url,"Get")
                soup = BeautifulSoup(response.content, "html.parser")
                if response.status_code == 200:

                    boxs = soup.find_all("div",{"class":'item'})
                    for box in boxs:
                        date = Helper.parse_date(box.find("p",{"class":"fade"}).text)
                        if date:
                            if date.year < datetime.datetime.now().year:
                                break

                        url = "https://www.bd.com/" + box.find("a")['href']

                        # Check if already present
                        unqUrl = hashlib.md5(url.encode()).hexdigest()
                        chkIsExists = DbOperations.GetData(self.news_collection, {"news_url_uid": str(unqUrl)},
                                                           {}, QueryType.one)
                        if (chkIsExists):
                            print("Already saved. url - ( " + url + " )")
                            continue
                        datadict = Helper.get_news_dict()
                        datadict.update({"url":"https://www.bd.com/" + box.find("a")['href']})
                        description = self.fetchDescription("https://www.bd.com/" + box.find("a")['href'])
                        datadict.update({
                            "date": Helper.parse_date(box.find("p",{"class":"fade"}).text),
                            "news_provider": "Becton, Dickinson and Company",
                            "formatted_sub_header": box.find("a").text.strip(),
                            "publishedAt": Helper.parse_date(box.find("p",{"class":"fade"}).text),
                            "description": description,
                            "title": box.find("a").text.strip(),
                            "news_title_uid": hashlib.md5(box.find("a").text.strip().encode()).hexdigest(),
                            "link": url,
                            "text":description,
                            "ticker": "bd_scrapped", "industry_name": "Becton, Dickinson and Company",
                            "company_id" : "Becton, Dickinson and Company",
                            "news_url_uid" : hashlib.md5(url.encode()).hexdigest()

                        })
                        data.append(datadict)
                else:
                    break
            DbOperations.InsertIntoMongo(self.news_collection,data)
        except Exception as e:
            self.logger.error(f"Error Occured : \n", exc_info=True)

    def fetchDescription(self,url):
        article = ''
        try:
            # print(url)
            description = crawler.MakeRequest(url, "Get")
            articlesoup = BeautifulSoup(description.content, 'html.parser')
            # print(articlesoup)
            articlesoupobj = articlesoup.find("div",{"class":"container"})
            articles = articlesoupobj.find_all("p",attrs={'class': None})
            for art in articles:
                article += art.text + "\n"
        except Exception as e:
            self.logger.error(f"Error Occured : \n", exc_info=True)
        return article

#Create and configure logger
logging.basicConfig(filename="news_scraping_logs.log",
                    format='%(asctime)s %(message)s',
                    filemode='a')
logger = logging.getLogger()
obj = BD('https://www.bd.com/en-us/company/news-and-media/press-releases?page=1',logger=logger)
obj.crawler()

news_collection = Helper.getNewsCollection()
processed_collection = Helper.getProcessNewsCollection()
news_log_collection = Helper.getLogCollection()
isInserted,rowCount = Helper.processNews(news_collection,processed_collection,'Becton, Dickinson and Company')
print('Total rows added Process collection => ' + str(rowCount))

# UPDATING LOG COLLECTION
if (isInserted):
    Helper.makeLog(news_log_collection,processed_collection,'Becton, Dickinson and Company')