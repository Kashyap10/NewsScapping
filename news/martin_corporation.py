from os import path
import sys
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from helper import Helper
from crawler import crawler
from bs4 import BeautifulSoup
from DbOps import DbOperations,QueryType
import hashlib,logging

class martin_corporation(object):
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
        self.news_collection = Helper.getNewsCollection()
        self.logger = logger
        self.header = {
                            'upgrade-insecure-requests': "1",
                            'user-agent': "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36",
                            'accept': "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                            'sec-fetch-site': "same-origin",
                            'sec-fetch-mode': "navigate",
                            'sec-fetch-user': "?1",
                            'sec-fetch-dest': "document",
                            'cache-control': "no-cache"
                      }
    def crawler_news(self):
        try:
            loop = True
            count = 0
            while loop:
                response = crawler.MakeRequest(self.url.format(count=count),'Get',postData=self.body,headers=self.header)
                soup = BeautifulSoup(response.content, 'html.parser')
                bulk_obj = DbOperations.Get_object_for_bulkop(False,self.news_collection)
                news_data = soup.find_all('li', {'class': "wd_item"})
                if news_data:
                    for news in news_data:
                        news_dict = Helper.get_news_dict()

                        title_data = news.find('div',{'class','wd_title'})
                        title = title_data.text if title_data else ""

                        url_data = news.find('a', {'href': True})
                        url = url_data['href'] if url_data else ''

                        # Check if already present
                        unqUrl = hashlib.md5(url.encode()).hexdigest()
                        chkIsExists = DbOperations.GetData(self.news_collection, {"news_url_uid": str(unqUrl)}, {},
                                                           QueryType.one)
                        if (chkIsExists):
                            print("Already saved. url - ( " + url + " )")
                            continue

                        description_data = news.find('div',{'class':'wd_subtitle'})
                        description = description_data.text if description_data else ''

                        text_data = news.find('div', {'class': 'wd_summary'})
                        text = text_data.text if text_data else ''

                        publish_date_data = news.find('div',{'class':'wd_date'})
                        publish_date = Helper.parse_date(publish_date_data.text) if publish_date_data and publish_date_data.text != '' else ''

                        news_dict.update(
                            {"title": title, "news_title_uid": hashlib.md5(title.encode()).hexdigest(),
                             "url": url, "link": url, "news_url_uid": hashlib.md5(url.encode()).hexdigest(),
                             "description": description, "text": description,
                             "publishedAt": publish_date, 'date': publish_date, "publishedAt_scrapped": publish_date,
                             "company_id": "martin_corporation", "ticker": "martin_corporation_scrapped", "industry_name": "martin_corporation",
                             "news_provider": "martin_corporation"})

                        bulk_obj.insert(news_dict)

                        if len(bulk_obj._BulkOperationBuilder__bulk.__dict__['ops']) >100:
                            bulk_obj.execute()
                            bulk_obj = DbOperations.Get_object_for_bulkop(False,self.news_collection)

                    if len(bulk_obj._BulkOperationBuilder__bulk.__dict__['ops']) > 0:
                        bulk_obj.execute()

                    count += 100
                else:
                    print("All news has been scrapped !!")
                    loop = False
        except Exception as e:
            self.logger.error(f"Error Occured : \n",exc_info=True)


#Create and configure logger
logging.basicConfig(filename="news_scraping_logs.log",
                    format='%(asctime)s %(message)s',
                    filemode='a')
logger = logging.getLogger()
url = "https://news.lockheedmartin.com/news-releases?l=100&o={count}"
news_obj = martin_corporation(url,logger=logger)
news_obj.crawler_news()

news_collection = Helper.getNewsCollection()
processed_collection = Helper.getProcessNewsCollection()
news_log_collection = Helper.getLogCollection()
isInserted,rowCount = Helper.processNews(news_collection,processed_collection,'martin_corporation')
print('Total rows added Process collection => ' + str(rowCount))

# UPDATING LOG COLLECTION
if (isInserted):
    Helper.makeLog(news_log_collection,processed_collection,'martin_corporation')