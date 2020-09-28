from os import path
import sys
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from helper import Helper
from crawler import crawler
from bs4 import BeautifulSoup
from DbOps import DbOperations,QueryType
import re
import hashlib
import logging
import urllib3

urllib3.disable_warnings()

class dbc(object):
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

    def crawler_news(self):

        """
        This function will scrap news page wise for given url
        :return:
        """

        try:
            loop = True
            page = 1
            while loop:
                response = crawler.MakeRequest(self.url.format(page=page),'Get',postData=self.body,headers=self.headers)
                soup = BeautifulSoup(response.content, 'html.parser')
                bulk_obj = DbOperations.Get_object_for_bulkop(False,self.news_collection)
                news_data = soup.find_all('div', {'class': "press_list"})
                if news_data:
                    for news in news_data:
                        try:
                            news_dict = Helper.get_news_dict()

                            title_data = news.find('div',{'class':'press_title'})
                            title = title_data.text.strip() if title_data else ""

                            url_data = news.find('a', {'href': True})
                            url = "https://www.dbc.com.tw/"+str(url_data['href']) if url_data else ''

                            # Check if already present
                            unqUrl = hashlib.md5(url.encode()).hexdigest()
                            chkIsExists = DbOperations.GetData(self.news_collection,{"news_url_uid": str(unqUrl)},{},QueryType.one)
                            if (chkIsExists):
                                print("Already saved. url - ( " + url + " )")
                                continue

                            publish_date_data = news.find('div',{'class':'press_data'})
                            publish_date = Helper.parse_date(publish_date_data.text) if publish_date_data  else ''

                            url_response = crawler.MakeRequest(url, 'Get', postData=self.body, headers=self.headers)
                            url_soup = BeautifulSoup(url_response.content, 'html.parser')
                            description_data = url_soup.find_all('p')

                            description = []
                            regex = re.compile(r'[\n\xa0]')
                            for desc in description_data:
                                description.append(regex.sub("", str(desc.text)))
                            description= ''.join(description)

                            news_dict.update(
                                {"title": title, "news_title_uid": hashlib.md5(title.encode()).hexdigest(),
                                 "url": url, "link": url, "news_url_uid": hashlib.md5(url.encode()).hexdigest(),
                                 "description": description, "text": description,
                                 "publishedAt": publish_date, 'date': publish_date, "publishedAt_scrapped": publish_date,
                                 "company_id": "dbc", "ticker": "dbc_scrapped", "industry_name": "dbc",
                                 "news_provider": "dbc"})

                            bulk_obj.insert(news_dict)

                            if len(bulk_obj._BulkOperationBuilder__bulk.__dict__['ops']) >100:
                                bulk_obj.execute()
                                bulk_obj = DbOperations.Get_object_for_bulkop(False,self.news_collection)

                        except Exception as e:
                            self.logger.error(f"Error Occured : \n", exc_info=True)

                    if len(bulk_obj._BulkOperationBuilder__bulk.__dict__['ops']) > 0:
                        bulk_obj.execute()

                    page += 1
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
url = "https://www.dbc.com.tw/press.php?sid=13&page={page}"
news_obj = dbc(url,logger=logger)
news_obj.crawler_news()

news_collection = Helper.getNewsCollection()
processed_collection = Helper.getProcessNewsCollection()
news_log_collection = Helper.getLogCollection()
isInserted,rowCount = Helper.processNews(news_collection,processed_collection,'dbc')
print('Total rows added Process collection => ' + str(rowCount))

# UPDATING LOG COLLECTION
if (isInserted):
    Helper.makeLog(news_log_collection,processed_collection,'dbc')