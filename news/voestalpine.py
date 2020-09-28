from os import path
import sys
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from helper import Helper
from crawler import crawler
from bs4 import BeautifulSoup
from DbOps import DbOperations,QueryType
import hashlib
import logging
import urllib3
urllib3.disable_warnings()

class voestalpine(object):
    def __init__(self,url,body=None,headers= None,logger=None):
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
        self.logger = logger
        self.news_collection = Helper.getNewsCollection()

    def crawler_news(self):
        try:
            response = crawler.MakeRequest(self.url,'Get',postData=self.body,headers=self.headers)
            soup = BeautifulSoup(response.content, 'html.parser')
            bulk_obj = DbOperations.Get_object_for_bulkop(False,self.news_collection)
            article_data = soup.find_all('div',{'class':'article'})
            if article_data:
                for article in article_data:
                    news_data = article.find_all('section',{'class':""})
                    for news in news_data[1::2]:

                        news_dict = Helper.get_news_dict()

                        title_data = news.find('h2')
                        title = title_data.text if title_data else ""

                        url = news.find_next_sibling().a['href'] if news.find_next_sibling() else ''

                        # Check if already present
                        unqUrl = hashlib.md5(url.encode()).hexdigest()
                        chkIsExists = DbOperations.GetData(self.news_collection, {"news_url_uid": str(unqUrl)}, {},
                                                           QueryType.one)
                        if (chkIsExists):
                            print("Already saved. url - ( " + url + " )")
                            continue

                        description_data = news.find('p')
                        description = description_data.text if description_data else ''

                        url_response = crawler.MakeRequest(url,'Get',postData=self.body,headers=self.headers)
                        url_soup = BeautifulSoup(url_response.content, 'html.parser')
                        publish_date_data = url_soup.find('p',{'class':'meta large inline'})
                        publish_date = Helper.parse_date(publish_date_data.text.replace('|',"").strip()) if publish_date_data and publish_date_data.text != '' else ''

                        news_dict.update(
                            {"title": title, "news_title_uid": hashlib.md5(title.encode()).hexdigest(),
                             "url": url, "link": url, "news_url_uid": hashlib.md5(url.encode()).hexdigest(),
                             "description": description, "text": description,
                             "publishedAt": publish_date, 'date': publish_date, "publishedAt_scrapped": publish_date,
                             "company_id": "voestalpine", "ticker": "voestalpine_scrapped", "industry_name": "voestalpine", "news_provider": "voestalpine"})

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
url = "https://www.voestalpine.com/group/en/media/press-releases/latest-news/"
news_obj = voestalpine(url,logger=logger)
news_obj.crawler_news()

news_collection = Helper.getNewsCollection()
processed_collection = Helper.getProcessNewsCollection()
news_log_collection = Helper.getLogCollection()
isInserted,rowCount = Helper.processNews(news_collection,processed_collection,'voestalpine')
print('Total rows added Process collection => ' + str(rowCount))

# UPDATING LOG COLLECTION
if (isInserted):
    Helper.makeLog(news_log_collection,processed_collection,'voestalpine')