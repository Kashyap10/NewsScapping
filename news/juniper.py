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

class juniper(object):
    def __init__(self,url,body=None,headers=None,logger=None):
        self.url = url
        self.body = body
        self.headers = headers
        self.logger = logger
        self.news_collection = Helper.getNewsCollection()
    def crawler_news(self):
        try:
            loop = True
            page = 0
            while loop:
                response = crawler.MakeRequest(self.url.format(page=page),'Get',postData=self.body,headers=self.headers)
                soup = BeautifulSoup(response.content, 'html.parser')
                bulk_obj = DbOperations.Get_object_for_bulkop(False,self.news_collection)
                news_data = soup.find('div', {'class': "nir-widget--list"})
                if news_data:
                    final_news_data = news_data.find_all('article',{'role':'article'})
                    if final_news_data:
                        for news in final_news_data:
                            news_dict = Helper.get_news_dict()

                            title_data = news.find('div',{'class':'nir-widget--field nir-widget--news--headline'})
                            title = title_data.text.strip() if title_data else ""

                            url_data = news.find('a', {'href': True})
                            url = "https://newsroom.juniper.net"+str(url_data['href']) if url_data else ''

                            # Check if already present
                            unqUrl = hashlib.md5(url.encode()).hexdigest()
                            chkIsExists = DbOperations.GetData(self.news_collection, {"news_url_uid": str(unqUrl)}, {},
                                                               QueryType.one)
                            if (chkIsExists):
                                print("Already saved. url - ( " + url + " )")
                                continue

                            publish_date_data = news.find('div',{'class':'nir-widget--field nir-widget--news--date-time'})
                            publish_date = Helper.parse_date(publish_date_data.text) if publish_date_data and publish_date_data.text != '' else ''

                            url_response = crawler.MakeRequest(url, 'Get', postData=self.body, headers=self.headers)
                            url_soup = BeautifulSoup(url_response.content, 'html.parser')
                            description_data = url_soup.find('div',{'class':"node__content, full_article_texts"})

                            description = []
                            regex = re.compile(r'[\n\xa0]')
                            for desc in description_data.find_all('p'):
                                description.append(regex.sub("", str(desc.text)))
                            description= ''.join(description)

                            news_dict.update(
                                {"title": title, "news_title_uid": hashlib.md5(title.encode()).hexdigest(),
                                 "url": url, "link": url, "news_url_uid": hashlib.md5(url.encode()).hexdigest(),
                                 "description": description, "text": description,
                                 "publishedAt": publish_date, 'date': publish_date, "publishedAt_scrapped": publish_date,
                                 "company_id": "juniper", "ticker": "juniper_scrapped", "industry_name": "juniper",
                                 "news_provider": "juniper"})

                            bulk_obj.insert(news_dict)

                            if len(bulk_obj._BulkOperationBuilder__bulk.__dict__['ops']) >100:
                                bulk_obj.execute()
                                bulk_obj = DbOperations.Get_object_for_bulkop(False,self.news_collection)

                        if len(bulk_obj._BulkOperationBuilder__bulk.__dict__['ops']) > 0:
                            bulk_obj.execute()

                        page += 1
                    else:
                        print("All news has been scrapped !!")
                        loop = False
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
url = "https://newsroom.juniper.net/?bfc50a07_year%5Bvalue%5D=2020&op=Filter&bfc50a07_widget_id=bfc50a07&form_build_id=form-FwbWHS7QJAjU7-d0FP1FLm7AguRwFK0IEWQDu5Ppzq8&form_id=widget_form_base&page={page}"
news_obj = juniper(url,logger=logger)
news_obj.crawler_news()

news_collection = Helper.getNewsCollection()
processed_collection = Helper.getProcessNewsCollection()
news_log_collection = Helper.getLogCollection()
isInserted,rowCount = Helper.processNews(news_collection,processed_collection,'juniper')
print('Total rows added Process collection => ' + str(rowCount))

# UPDATING LOG COLLECTION
if (isInserted):
    Helper.makeLog(news_log_collection,processed_collection,'juniper')