# -*- coding: utf-8 -*-
import re
import math
import time
import logging
import ujson as json

from bs4 import BeautifulSoup
import requests

from multiprocessing import Process, Queue

EXIT = 1

class Categorycrawler(Process):
    base_url = 'https://search.shopping.naver.com/category/category.nhn'
    headers = {
        "accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "accept-encoding":"gzip, deflate, br",
        "accept-language":"ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "user-agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36"}
    params = {
        'cat_id':50000004,
    }
    logger = logging.getLogger("crawl_app.Itemcrawler")

    def __init__(self, cat_list, queue, **kwargs):
        super(Categorycrawler,self).__init__()
        self.research_list = cat_list
        self.delay = kwargs.get('delay',0.5)
        self.queue = queue

    def run(self):
        global EXIT
        self.logger.info("start to run")
        for cat_id in self.research_list:
            start = time.time()
            self.params['cat_id'] = cat_id
            res_text, res_url = self._requests_get_text()

            if res_text is None:
                continue
            self.queue.put((res_text, res_url))

            end = time.time()
            if (end-start)-self.delay > 0:
                time.sleep(end-start-self.delay)
            start = time.time()
        self.logger.info("end to run")
        self.queue.put((EXIT,EXIT))

    def _requests_get_text(self):
        # get method로 요청하여, text로 반환하는 메소드
        try:
            res = requests.get(self.base_url,
                               params=self.params,
                               headers=self.headers,
                               timeout=5)
            res_text = res.text
            res_url = res.url
        except requests.ConnectionError as e:
            self.logger.error(e)
            res_text = None
            res_url = None
        except requests.Timeout as e:
            self.logger.error(e)
            res_text = None
            res_url = None
        except requests.RequestException as e:
            self.logger.error(e)
            res_text = None
            res_url = None
        finally:
            return res_text, res_url


class Brand_crawl_parser(Process):
    base_url = 'https://search.shopping.naver.com/search/category.nhn'
    headers = {
        "accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "accept-encoding":"gzip, deflate, br",
        "accept-language":"ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "user-agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36"}
    params = {
        'cat_id':0,
        "pagingIndex":1,
        'pagingSize':20,
        "viewType":'list',
        'sort':'date',
        'frm':'NVSHBRD'
    }
    method = "get"
    logger = logging.getLogger("crawl_app.Itemcrawler")

    def __init__(self, loader, saver, **kwargs):
        super(Brand_crawl_parser, self).__init__()
        self.research_list = self._load_task(loader)
        self.delay = kwargs.get('delay',0.5)
        self.saver = saver

    def _load_task(self,loader):
        cats = loader()
        cat_list = []
        for cat in cats:
            idx = 4
            while True:
                if cat.get('cat{}_id'.format(idx)):
                    cat_list.append(cat.get('cat{}_id'.format(idx)))
                    break
                elif idx < 0:
                    break
                idx -= 1
        return cat_list

    def run(self):
        global EXIT
        self.logger.info('start to run')
        for cat_id in self.research_list:
            self.params['cat_id'] = cat_id
            res_text, res_url = self._requests_text()
            self._parse_and_save(res_text, res_url)
            if res_text is None:
                continue
        self.logger.info('end to run')

    def _requests_text(self):
        try:
            res = requests.request(self.method,
                                   self.base_url,
                                   params=self.params,
                                   headers=self.headers,
                                   timeout=5)
            res_text = res.text
            res_url = res.url
        except requests.ConnectionError as e:
            self.logger.error(e)
            res_text = None
            res_url = None
        except requests.Timeout as e:
            self.logger.error(e)
            res_text = None
            res_url = None
        except requests.RequestException as e:
            self.logger.error(e)
            res_text = None
            res_url = None
        finally:
            return res_text, res_url

    def _parse_and_save(self, res_text,res_url):
        bsObj = BeautifulSoup(res_text,"html.parser")

        row = {'cat_id':self.params['cat_id']}
        brand_div = bsObj.find("div",{"class":re.compile("brand_filter")})
        brand_hot = brand_div.find("ul",{'class':re.compile("finder_tit")})

        for brand_sec in brand_hot.find_all('li'):
            try:
                row["brand_name"] = brand_sec.a.attrs['title']
                row["brand_id"] = brand_sec.a.attrs['data-filter-value']
            except AttributeError as e:
                self.logger.error("brand_name or brand_id is missing... url : {}".format(res_url))
                continue
            row_serial = json.dumps(row)
            self.saver(row_serial)


class Itemcrawler(Process):
    base_url = 'https://search.shopping.naver.com/search/category.nhn'
    headers = {
        "accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "accept-encoding":"gzip, deflate, br",
        "accept-language":"ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "user-agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36"}
    params = {
        'brand':0,
        'cat_id':0,
        "productSet":"model",
        "pagingIndex":1,
        'pagingSize':80,
        "viewType":'list',
        'sort':'date',
        'frm':'NVSHBRD'
    }
    method = "get"
    logger = logging.getLogger("crawl_app.Itemcrawler")

    def __init__(self, loader, queue, **kwargs):
        super(Itemcrawler,self).__init__()
        self.research_list = self._load_task(loader)
        self.delay = kwargs.get('delay',0.5)
        self.queue = queue

    def _load_task(self, loader):
        return loader()

    def run(self):
        global EXIT
        self.logger.info("start to run")
        for item in self.research_list:
            self.params['brand'] = item["brand_id"]
            self.params['cat_id'] = item["cat_id"]
            self.params['pagingIndex'] = 1

            res_text, res_url = self._requests_text()
            if res_text is None:
                continue
            res_text = self._append_info(res_text,item['brand_id'],item['brand_name'])
            self.queue.put((res_text, res_url))

            paging_count = self._count_pages(res_text,res_url)
            self.logger.info("start to search brand[{}]|category[{}] ... nums of paging : {}"\
                             .format(item['brand_id'],item['cat_id'],paging_count))
            start = time.time()
            for idx in range(2,paging_count):
                self.params['pagingIndex'] = idx

                res_text, res_url = self._requests_text()
                if res_text is None:
                    continue
                res_text = self._append_info(res_text,item['brand_id'],item['brand_name'])
                self.queue.put((res_text, res_url))

                end = time.time()
                if (end-start)-self.delay > 0:
                    time.sleep(end-start-self.delay)
                start = time.time()
        self.logger.info("end to run")
        self.queue.put((EXIT,EXIT))

    def _requests_text(self):
        try:
            res = requests.request(self.method,
                                   self.base_url,
                                   params=self.params,
                                   headers=self.headers,
                                   timeout=5)
            res_text = res.text
            res_url = res.url
        except requests.ConnectionError as e:
            self.logger.error(e)
            res_text = None
            res_url = None
        except requests.Timeout as e:
            self.logger.error(e)
            res_text = None
            res_url = None
        except requests.RequestException as e:
            self.logger.error(e)
            res_text = None
            res_url = None
        finally:
            return res_text, res_url

    def _count_pages(self,res_text,res_url):
        # 해당 Brand & Cat에 해당하는 검색 결과의 페이지 수를 가져오는 메소드
        bsObj = BeautifulSoup(res_text,"html.parser")
        try:
            count_expr = bsObj.find("ul",{'class':'snb_list'})\
                            .find("a",{'class':'_productSet_total'})\
                            .find(text=re.compile("[\d,]+"))
        except AttributeError as e:
            self.logger.error("count_expr is missing ...url : {}".format(res_url))
            count_expr = "0"
        total_count = int(re.sub("\D","",count_expr))
        paging_count = math.ceil(total_count / 80)
        return paging_count

    def _append_info(self, res_text,brand_id,brand_name):
        res_text = '<div class="brand_id">{}</div><div class="brand_name">{}</div>'.format(brand_id,brand_name) + res_text
        return res_text


class Reviewcrawler(Process):
    base_url = "https://search.shopping.naver.com/detail/review_list.nhn"
    sess_url = "https://search.shopping.naver.com/detail/detail.nhn"
    headers = {
        "accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "accept-encoding":"gzip, deflate, br",
        "accept-language":"ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "user-agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36"
    }
    params = {
       "nv_mid":10010250511,
        "page": 1,
        "reviewSort": "registration",
        "reviewType": "all"
    }
    method = "post"
    logger = logging.getLogger("crawl_app.Reviewcrawler")
    def __init__(self, loader, updater, queue, **kwargs):
        super(Reviewcrawler,self).__init__()
        self.research_list = self._load_task(loader)
        self.queue = queue
        self.updater = updater
        self.delay = kwargs.get('delay',0.5)

    def _load_task(self,loader):
        return loader()

    def run(self):
        global EXIT
        self.logger.info("start to run")
        for nv_mid, count in self.research_list.items():
            with requests.Session() as sess:
                self.params['nv_mid'] = nv_mid
                self.params['pagingIndex'] = 1

                res = sess.get(self.sess_url,params={"nv_mid":nv_mid})
                res_text = res.text
                res_url = res.url
                if res_text is None:
                    continue
                res_text = self._append_info(res_text,nv_mid)
                self.queue.put((res_text, res_url))
                total_paging_count = self._count_pages(res_text,res_url) - int(count)
                paging_count = total_paging_count - int(count)
                self.logger.info("start to search review. NvMid[{}] ... nums of paging : {}"\
                                 .format(nv_mid,paging_count))
                start = time.time()
                for idx in range(2,paging_count):
                    self.params['pagingIndex'] = idx

                    res_text, res_url = self._requests_text(sess)
                    if res_text is None:
                        continue
                    res_text = self._append_info(res_text,nv_mid)
                    self.queue.put((res_text, res_url))

                    end = time.time()
                    if (end-start)-self.delay > 0:
                        time.sleep(end-start-self.delay)
                    start = time.time()
            self.updater({nv_mid:total_paging_count})
        self.logger.info("end to run")
        self.queue.put((EXIT,EXIT))

    def _requests_text(self,sess):
        try:
            res = sess.request(self.method,
                            self.base_url,
                            params=self.params,
                            headers=self.headers,
                            timeout=5)
            res_text = res.text
            res_url = res.url
        except requests.ConnectionError as e:
            self.logger.error(e)
            res_text = None
            res_url = None
        except requests.Timeout as e:
            self.logger.error(e)
            res_text = None
            res_url = None
        except requests.RequestException as e:
            self.logger.error(e)
            res_text = None
            res_url = None
        finally:
            return res_text, res_url

    def _count_pages(self,res_text,res_url):
        bsObj = BeautifulSoup(res_text, 'html.parser')
        try:
            count_expr = bsObj.find('a',{'data-tab-name':'review'}).text
        except AttributeError as e:
            self.logger.error("count_expr is missing ... url : {}".format(res_url))
            count_expr = "0"
        try:
            total_count = int(re.sub("\D","",count_expr))
        except ValueError as e:
            total_count = 0
        paging_count = math.ceil(total_count / 20)
        return paging_count

    def _append_info(self, res_text,nv_mid):
        res_text = '<div class="nv_mid">{}</div>'.format(nv_mid) + res_text
        return res_text
