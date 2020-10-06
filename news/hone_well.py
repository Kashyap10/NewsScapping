from os import path
import sys
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
import requests,hashlib,logging
from helper import Helper
from bs4 import BeautifulSoup
from DbOps import *

class honey_well(object):
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
    def crawler_news(self):
        try:
            header = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36",
                "Accept": "*/*"
            }
            response = requests.get(self.url, headers=header)
            soup = BeautifulSoup(response.content, 'html.parser')
            bulk_obj = DbOperations.Get_object_for_bulkop(False,self.news_collection)
            for news in soup.find_all('div', {'class': "col-md-4 cg-item d-none"}):
                title_data = news.find('h4', {'class': "header5 give-ellipsis-after-3lines"})
                if title_data:
                    title = title_data.text
                else:
                    title = ""

                url_data = news.find('a', {'href': True})
                if url_data:
                    url = "https://www.honeywell.com"+str(url_data['href'])
                else:
                    url = ""

                # Check if already present
                unqUrl = hashlib.md5(url.encode()).hexdigest()
                chkIsExists = DbOperations.GetData(self.news_collection, {"news_url_uid": str(unqUrl)}, {},
                                                   QueryType.one)
                if (chkIsExists):
                    print("Already saved. url - ( " + url + " )")
                    continue

                url_response = requests.get(url,headers=header)
                url_soup_obj = BeautifulSoup(url_response.content, 'html.parser')
                description_data = url_soup_obj.find('meta',{'name':'description'})

                if description_data:
                    description = description_data['content']
                else:
                    description = ''
                news_dict = Helper.get_news_dict()
                news_dict.update({"title":title,"url":url,"formatted_sub_header":title,"description":description,"link":url,"news_provider":"honeywell"})

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
url = "https://www.honeywell.com/en-us/news"
news_obj = honey_well(url,logger=logger)
news_obj.crawler_news()

news_collection = Helper.getNewsCollection()
processed_collection = Helper.getProcessNewsCollection()
news_log_collection = Helper.getLogCollection()
isInserted,rowCount = Helper.processNews(news_collection,processed_collection,'honeywell')
print('Total rows added Process collection => ' + str(rowCount))

# UPDATING LOG COLLECTION
if (isInserted):
    Helper.makeLog(news_log_collection,processed_collection,'honeywell')