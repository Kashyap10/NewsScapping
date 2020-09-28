import requests
print("It works!!!")

class crawler(object):

    @staticmethod
    def MakeRequest(url, requestType, postData=None,headers = None):
        response = None
        try:
            if not headers:
                headers = {}
                headers.update({'Accept': 'application/json, text/plain, */*'})
                headers.update({'cache-control': "no-cache"})
                headers.update({'Accept-Encoding': 'gzip, deflate, br'})
                headers.update({'Accept-Language': 'en-US,en;q=0.9'})
                headers.update({'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:56.0) Gecko/20100101 Firefox/56.0'})

            if (requestType == "Post"):
                response = requests.post(url, data=postData, headers=headers, verify=False)
            else:
                response = requests.get(url, headers=headers, verify=False)

        except Exception as e:
            print(e)
        return response
