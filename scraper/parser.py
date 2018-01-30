# -*- coding: utf-8 -*-
import re
import math
import time
import ujson as json
import logging

from bs4 import BeautifulSoup
from bs4 import NavigableString, Tag
import requests

from datetime import datetime
from multiprocessing import Process, Queue

EXIT = 1

class Categoryparser(Process):
    logger = logging.getLogger('crawl_app.Categoryparser')
    def __init__(self, queue, saver):
        super(Categoryparser,self).__init__()
        self.queue = queue
        self.saver = saver

    def run(self):
        global EXIT
        while True:
            try:
                res_text, res_url = self.queue.get(timeout=60)
            except Exception as e:
                self.logger.error('timeout(60s) exception for waiting queue')
                break
            if res_text == EXIT:
                self.queue.put((EXIT,EXIT))
                break
            else:
                self._parse_context(res_text,res_url)

    def _parse_context(self,res_text,res_url):
        row = {}
        bsObj = BeautifulSoup(res_text,"html.parser")
        container = bsObj.find('div',{'id':'container'})
        try:
            row['cat1_id'] = res_url.split("=")[1]
            row['cat1_name'] = container.h2.text
        except AttributeError as e:
            self.logger.error("category level 1 is missing ... url : {}".format(res_url))
            return
        for cat_cell in container.find_all('div',{'class':'category_cell'}):
            try:
                row["cat2_name"] = cat_cell.h3.strong.text
                row["cat2_id"]   = cat_cell.h3.a.attrs['href'].split("=")[1]
            except AttributeError as e:
                self.logger.error("category level 2 is missing ... url : {}".format(res_url))
                continue

            li = cat_cell.find('li')
            while True:
                if isinstance(li, Tag):
                    try:
                        row["cat3_name"] = li.a.text
                        row["cat3_id"] = li.a.attrs['href'].split('=')[1]
                    except AttributeError as e:
                        self.logger.error("category level 3 is missing ... url : {}".format(res_url))
                        continue
                    if len(li.find_all('li')) > 0:
                        for cat4 in li.find_all('li'):
                            try:
                                row["cat4_name"] = cat4.a.text
                                row["cat4_id"] = cat4.a.attrs['href'].split('=')[1]
                            except AttributeError as e:
                                self.logger.error("category level 3 is missing ... url : {}".format(res_url))
                                continue
                            row_serial = json.dumps(row)
                            self.saver(row_serial)
                    else:
                        row["cat4_name"] = ""
                        row["cat4_id"] = ""
                        row_serial = json.dumps(row)
                        self.saver(row_serial)
                    li = li.next_sibling
                elif isinstance(li, NavigableString):
                    li = li.next_sibling
                    continue
                else:
                    break

class Itemparser(Process):
    logger = logging.getLogger("crawl_application.Itemparser")
    def __init__(self, queue, saver,task_saver):
        super(Itemparser,self).__init__()
        self.queue = queue
        self.saver = saver
        self.task_saver = task_saver

    def run(self):
        global EXIT
        while True:
            try:
                res_text, res_url = self.queue.get(timeout=60)
            except Exception as e:
                self.logger.error('timeout(60s) exception for waiting queue')
            if res_text == EXIT:
                self.queue.put((EXIT,EXIT))
                break
            else:
                self._parse_item_context(res_text,res_url)

    def _parse_item_context(self,res_text,res_url):
        # item 창 파싱하기
        bsObj = BeautifulSoup(res_text,"html.parser")
        brand_id = bsObj.find('div',{'class':'brand_id'}).text
        brand_name = bsObj.find('div',{'class':'brand_name'}).text
        row = {"brand_id":brand_id,"brand_name":brand_name}
        for li in bsObj.find_all("li",{"class":"_itemSection"}):
            try:
                row["nv_mid"] = li.attrs['data-nv-mid']
            except AttributeError as e:
                self.logger.error("nvmid is missing... url : {}".format(res_url))
                continue

            try:
                info = li.find('div',{'class':"info"})
                if info is None or not info.text:
                    raise AttributeError
            except AttributeError as e:
                self.logger.error("info is missing... url : {}".format(res_url))
                continue

            try:
                row["url"] = li.a.attrs['href']
            except AttributeError as e:
                row['url'] = ""
                self.logger.warn("url is missing... url : {}".format(res_url))

            try:
                row["img_url"] = li.img.attrs['data-original']
            except AttributeError as e:
                row['img_url'] = ""
                self.logger.warn("image url is missing... url : {}".format(res_url))

            if info.find('a',{'class':'btn_compare'}):
                row["price_compare_exist"] = True
            else:
                row["price_compare_exist"] = False

            try:
                row["item_title"] = info.find('a',{'class':'tit'}).attrs['title']
            except AttributeError as e:
                row["item_title"] = ""
                self.logger.warn("item title is missing... url : {}".format(res_url))

            try:
                price_expr = info.find('span',{"class":'price'}).em.text
                row["min_price"] = re.sub("\D","",price_expr)
                if int(row['min_price']) > 1e8:
                    raise AttributeError
            except AttributeError as e:
                row["min_price"] = ""
                self.logger.warn("item price is missing... url : {}".format(res_url))

            try:
                cat_expr = info.find('span',{'class':'depth'}).text
                row["item_cats"] =  [expr.strip() for expr in re.sub("\n *","",cat_expr).split(">")]
            except AttributeError as e:
                row["item_cats"] = []
                self.logger.warn("category is missing... url : {}".format(res_url))

            try:
                row["item_spec"] = info.find('span',{'class':'detail'}).text
            except AttributeError as e:
                row["item_spec"] = ""
                self.logger.warn("item spec is missing... url : {}".format(res_url))

            try:
                date_expr =info.find('span',{'class':'date'}).text
                row["reg_date"] = re.search("\d+.\d+.",date_expr).group(0)
            except AttributeError as e:
                row["reg_date"] = datetime.strftime(datetime.now(),"%Y.%m.")
                self.logger.warn("reg date is missing... url : {}".format(res_url))
            row_serial = json.dumps(row)
            self.saver(row_serial)
            self.task_saver({row['nv_mid']:0})

class Reviewparser(Process):
    logger = logging.getLogger("crawl_app.Reviewparser")

    def __init__(self, queue, saver):
        super(Reviewparser,self).__init__()
        self.queue = queue
        self.saver = saver

    def run(self):
        global EXIT
        while True:
            try:
                res_text, res_url = self.queue.get(timeout=60)
            except Exception as e:
                self.logger.error('timeout(60s) exception for waiting queue')
                break
            if res_text == EXIT:
                self.queue.put((EXIT,EXIT))
                break
            else:
                self._parse_context(res_text,res_url)

    def _parse_context(self,res_text,res_url):
        bsObj = BeautifulSoup(res_text,'html.parser')
        try:
            nv_mid = bsObj.find("div",{'class':'nv_mid'}).text
            row = {'nv_mid':nv_mid}
        except:
            self.logger.error("nvmid is missing... queue error.")
            return
        if len(bsObj.find_all("div",{'class':'atc_area'})) == 0 :
            self.logger.info('no review ... url {}'.format(res_url))
            return
        for atc_area in bsObj.find_all("div",{'class':'atc_area'}):
            try:
                title_expr = atc_area.p.text
                row["review_title"] = re.sub("[\n\t]","",title_expr).strip()
            except:
                self.logger.warning("info is missing... url : {}".format(res_url))
                row['review_title'] = ""

            try:
                atc_expr = atc_area.find("div",{'class':'atc'}).text
                row["review_atc"] = re.sub("[\n\t]","",atc_expr).strip()
            except:
                self.logger.error('article is missing... url : {}'.format(res_url))
                continue

            try:
                row["review_grade"] = atc_area.find("span",{'class':'curr_avg'}).text
            except:
                self.logger.warning('grade is missing... url : {}'.format(res_url))
                row['review_grade'] = "0"


            try:
                date_expr = atc_area.find("span",{'class':'date'}).text
                row["review_date"] = re.sub("[^\d.]","",date_expr)
            except:
                self.logger.warning('date is missing... url : {}'.format(res_url))
                row['review_date'] = datetime.strftime(datetime.now(),format="%Y.%m.%d.")

            try:
                row["review_path"] = atc_area.find("span",{'class':'path'}).text
            except:
                self.logger.warning('path is missing... url : {}'.format(res_url))
                row['review_path'] = ""

            row_serial = json.dumps(row)
            self.saver(row_serial)
