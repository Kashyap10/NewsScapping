from os import path
import sys
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
import datetime
from helper import Helper
from bs4 import BeautifulSoup
from DbOps import DbOperations,QueryType
from crawler import *
import hashlib
import logging
import urllib3

urllib3.disable_warnings()

class rtx(object):

    def __init__(self,url,body=None,headers=None,logger=None):
        self.url = url
        self.body = body
        self.headers = headers
        self.news_collection = Helper.getNewsCollection()
        self.logger = logger

    def crawler(self):
        try:
            page = 1
            bulk_obj = DbOperations.Get_object_for_bulkop(False, self.news_collection)
            while True:
                response = crawler.MakeRequest(self.url.format(page=page),"Get",headers=self.headers)
                if 'we did not find any results related' in response.text:
                    break
                soup = BeautifulSoup(response.content, "html.parser")
                boxs = soup.find_all("li",{"class":'utc-cards--item'})
                for box in boxs:
                    date = box.find("time", {"class": "utc-card--date"}).text
                    if date:
                        date = Helper.parse_date(date)
                        if date.year < datetime.datetime.now().year:
                            break
                    datadict = Helper.get_news_dict()
                    datadict.update({"url":"https://www.rtx.com"+box.find("a")['href']})
                    description = self.fetchDescription("https://www.rtx.com" + box.find("a")['href'])

                    url = "https://www.rtx.com" + box.find("a")['href']

                    # Check if already present
                    unqUrl = hashlib.md5(url.encode()).hexdigest()
                    chkIsExists = DbOperations.GetData(self.news_collection, {"news_url_uid": str(unqUrl)}, {},
                                                       QueryType.one)
                    if (chkIsExists):
                        print("Already saved. url - ( " + url + " )")
                        continue

                    datadict.update({
                        "date": date,
                        "news_provider": "UNITED TECHNOLOGIES CORPORATION",
                        "formatted_sub_header": box.find("a").text,
                        "publishedAt": date,
                        "description": description,
                        "title": box.find("a").text,
                        "link": "https://www.rtx.com"+box.find("a")['href'],
                        "text":description,
                        "company_id" : "rtx",
                        "news_url_uid" : hashlib.md5(("https://www.rtx.com"+box.find("a")['href']).encode()).hexdigest()

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
        try:
            description = crawler.MakeRequest(url, "Get")
            articlesoup = BeautifulSoup(description.content, 'html.parser')
            article = articlesoup.find("div", {"class": "utc-container--content utc-article--content-text field-newscontentarea2"})
        except Exception as e:
            self.logger.error(f"Error Occured : \n", exc_info=True)
        return article
#Create and configure logger
logging.basicConfig(filename="news_scraping_logs.log",
                    format='%(asctime)s %(message)s',
                    filemode='a')
logger = logging.getLogger()
headers = {
    'cache-control': "no-cache"
    }
obj = rtx('https://www.rtx.com/Overlays/NewsList?npagenum={page}',headers=headers,logger=logger)
obj.crawler()

news_collection = Helper.getNewsCollection()
processed_collection = Helper.getProcessNewsCollection()
news_log_collection = Helper.getLogCollection()
isInserted,rowCount = Helper.processNews(news_collection,processed_collection,'rtx')
print('Total rows added Process collection => ' + str(rowCount))

# UPDATING LOG COLLECTION
if (isInserted):
    Helper.makeLog(news_log_collection,processed_collection,'rtx')