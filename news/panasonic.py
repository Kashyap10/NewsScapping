from os import path
import sys
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
import datetime,logging,hashlib
from DbOps import DbOperations,QueryType
from helper import Helper
from bs4 import BeautifulSoup
from crawler import *

class panasonic(object):

    def __init__(self, url, body=None, headers=None, logger=None):
        self.url = url
        self.body = body
        self.headers = headers
        self.logger = logger
        self.news_collection = Helper.getNewsCollection()
    def crawler(self):
        try:
            data = []
            counter = 1
            while True:
                response = crawler.MakeRequest(self.url.format(counter=counter),"Get")
                if response.status_code == 200:
                    soup = BeautifulSoup(response.content, "html.parser")

                    boxs = soup.find_all("div",{"class":'unicom-newsListItem'})
                    for box in boxs:
                        date = box.find("p",{"class":"unicom-listInformationDate"}).text
                        if date:
                            date = Helper.parse_date(date)
                            if date.year < datetime.datetime.now().year:
                                break
                        datadict = Helper.get_news_dict()
                        url = box.find("a")['href']
                        # Check if already present
                        unqUrl = hashlib.md5(url.encode()).hexdigest()
                        chkIsExists = DbOperations.GetData(self.news_collection, {"news_url_uid": str(unqUrl)}, {},
                                                           QueryType.one)
                        if (chkIsExists):
                            print("Already saved. url - ( " + url + " )")
                            continue
                        datadict.update({"newsurl":box.find("a")['href']})
                        description = self.fetchDescription(box.find("a")['href'])
                        datadict.update({
                             "url": url, "link": url, "news_url_uid": hashlib.md5(url.encode()).hexdigest(),
                            "date": box.find("p",{"class":"unicom-listInformationDate"}).text,
                            "news_provider": "panasonic",
                            "formatted_sub_header": box.find("h3",{"class":"unicom-newsListTitleIn"}).text,
                            "publishedAt": date,
                            "description": description,
                            "title": box.find("h3",{"class":"unicom-newsListTitleIn"}).text
                        })

                        data.append(datadict)
                    counter += counter
                    self.url = "https://news.panasonic.com/global/all/all_{counter}.html"
                else:
                    break
            DbOperations.InsertIntoMongo(self.news_collection,data)
        except Exception as e:
            self.logger.error(f"Error Occured : \n", exc_info=True)

    def fetchDescription(self,url):
        article = ''
        try:
            description = crawler.MakeRequest(url, "Get")
            articlesoup = BeautifulSoup(description.content, 'html.parser')
            articles = articlesoup.find_all("p", {"class": "block"})
            for a in articles:
                article +=a.text
        except Exception as e:
            self.logger.error(f"Error Occured : \n",exc_info=True)
        return article
#Create and configure logger
logging.basicConfig(filename="news_scraping_logs.log",
                    format='%(asctime)s %(message)s',
                    filemode='a')
logger = logging.getLogger()
obj = panasonic('https://news.panasonic.com/global/all/all.html',logger=logger)
obj.crawler()

news_collection = Helper.getNewsCollection()
processed_collection = Helper.getProcessNewsCollection()
news_log_collection = Helper.getLogCollection()
isInserted,rowCount = Helper.processNews(news_collection,processed_collection,'panasonic')
print('Total rows added Process collection => ' + str(rowCount))

# UPDATING LOG COLLECTION
if (isInserted):
    Helper.makeLog(news_log_collection,processed_collection,'panasonic')