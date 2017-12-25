# -*- coding: utf-8 -*-
from urllib.request import urlopen
from urllib.request import HTTPError
from urllib.parse import urlparse
from urllib.parse import urljoin
from urllib.parse import parse_qs
from bs4 import BeautifulSoup
import requests

import re
import json
import math
import yaml

import time
import logging

import pandas as pd

# Loading Configuration
with open("./configs.yml",'r') as stream:
    try:
        CONFIGS = yaml.load(stream)
    except yaml.YAMLError as e:
        print(e)
HEADERS = CONFIGS['browser']['headers'] # Human-like header style
SCP_INTERVAL = CONFIGS['browser']['scraping_interval'] # 스크래핑 시간 간격

NAVER_SEARCH_URL = CONFIGS['naver_search']['address'] # 네이버쇼핑 검색창 URL
NAVER_SEARCH_PARAMS = CONFIGS['naver_search']['params'] 

NAVER_CATEGORY_URL = CONFIGS['naver_category']['address'] # 네이버쇼핑의 카테고리 검색창 URL
NAVER_CATEGORY_PARAMS = CONFIGS['naver_category']['params']

NAVER_SHOPPING_URL = CONFIGS['naver_shopping']['address'] # 네이버쇼핑의 제품 내 리뷰창 URL
NAVER_SHOPPING_PARAMS = CONFIGS['naver_shopping']['params']

INTERNAL = CONFIGS['naver_search']['redirect_internal']
EXTERNAL = CONFIGS['naver_search']['redirect_external']
'''
#################################
메소드 설명
    - get_naver_review :
        네이버 리뷰를 가져오는 메인 메소드
        - arguments
            search_type : keyword / category
                                    keyword : 단일 키워드 검색으로 나오는 section list
                                    category : 카테고리 검색으로 나오는 section list
            keyword : search_type이 keyword일 때, 해당 검색 keyword
            brand : search_type이 category일 때, 해당 검색의 brand 이름
            cat_id : search_type이 category일 때, 해당 검색의 카테고리 id
        파생 메소드
        1) 검색결과(section)리스트를 가져오는 메소드    
            - get_section_list_in_keyword
                : keyword검색으로 검색결과를 가져오는 메소드
            - get_section_list_in_category
                : 카테고리검색으로 검색결과를 가져오는 메소드
            - count_section_nums
                : 검색결과가 갯수를 세는 메소드

        2) 검색결과 리스트를 parsing하는 메소드
                - parse_item_contents_in_section
                    : 검색결과 리스트를 Parsing 함
                - get_internal_url 
                    : 해당 검색결과 리스트가 네이버쇼핑URL인지 아닌지 확인

        3) 네이버 쇼핑 리뷰를 가져오는 메소드
            - get_naver_shopping_review
                네이버 쇼핑창에서 네이버 쇼핑리뷰를 가져오는 메소드
            - parse_naver_shopping_review
                네이버 쇼핑리뷰를 parsing하는 메소드

#################################
'''

def get_naver_review(search_type,**kwargs):
    if search_type == "keyword":
        keyword = kwargs.get('keyword')
        section_list = get_section_list_in_keyword(keyword)
    elif search_type == "category":
        brand = kwargs.get('brand')
        cat_id = kwargs.get('cat_id')
        section_list = get_section_list_in_category(brand, cat_id)

    contents_df = pd.DataFrame(columns=['name','url','target_url','min_price','max_price','category','date'])
    for section in section_list:
        item_row = parse_item_contents_in_section(section)
        contents_df = contents_df.append(item_row,ignore_index=True)
    
    review_list = []
    if not contents_df.empty:
        for target_url in contents_df.target_url:
            if target_url is not None:
                parse_result = urlparse(target_url)
                if parse_result.netloc == INTERNAL[0]: # == 'search.shopping.naver.com'
                    parsed = parse_qs(parse_result.query)
                    review_list.extend(get_naver_shopping_review(parsed['nv_mid'][0]))
                elif parse_result.netloc == INTERNAL[1]:# == 'storefarm.naver.com'
                    pass
                elif parse_result.netloc == INTERNAL[2]:# == 'swindow.naver.com'
                    pass
                
    return review_list

def get_section_list_in_keyword(keyword):
    global HEADERS, SCP_INTERVAL, NAVER_SEARCH_URL, NAVER_SEARCH_PARAMS    
    
    params = NAVER_SEARCH_PARAMS.copy()
    params['origQuery'] = keyword
    params['query'] = keyword
    with requests.Session() as sess: # For saving cookies    
        sess.headers.update(HEADERS) # set Human-browser style header
        sess.params.update(params)
        time.sleep(SCP_INTERVAL)
        response = sess.get(NAVER_SEARCH_URL)
        
        tot_nums = count_section_nums(response)

        section_list = []
        max_page = math.ceil(tot_nums/int(params['pagingSize']))
        for pag_num in range(1,max_page+1):
            params['pagingIndex'] = pag_num
            sess.params.update(params)
            
            time.sleep(SCP_INTERVAL)
            response = sess.get(NAVER_SEARCH_URL)
            if response.ok:
                try:
                    bsObj = BeautifulSoup(response.text,'html.parser')
                    _list = bsObj.\
                                   find('ul',{'class':'goods_list'}).\
                                   find_all('li',{'class':re.compile('itemSection')})        
                    section_list.extend(_list)
                except AttributeError as e:
                    return section_list
        
        return section_list

def get_section_list_in_category(cat_id,brand):
    global HEADERS, SCP_INTERVAL, NAVER_CATEGORY_PARAMS, NAVER_CATEGORY_URL
    
    params = NAVER_CATEGORY_PARAMS.copy()
    params['brand'] = brand
    params['cat_id'] = cat_id
    with requests.Session() as sess: # For saving cookies    
        # set Human-browser style header
        sess.headers.update(HEADERS)
        sess.params.update(params)
        
        time.sleep(SCP_INTERVAL)
        response = sess.get(NAVER_CATEGORY_URL)
        
        tot_nums = count_section_nums(response)

        section_list = []
        max_page = math.ceil(tot_nums/int(params['pagingSize']))
        for pag_num in range(1,max_page+1):
            params['pagingIndex'] = pag_num
            sess.params.update(params)
            
            time.sleep(SCP_INTERVAL)
            response = sess.get(NAVER_CATEGORY_URL)
            if response.ok:
                try:
                    bsObj = BeautifulSoup(response.text,'html.parser')
                    _list = bsObj.\
                                   find('ul',{'class':'goods_list'}).\
                                   find_all('li',{'class':re.compile('itemSection')})        
                    print(len(_list))
                    section_list.extend(_list)

                except AttributeError as e:
                    return section_list
        return section_list

def count_section_nums(response):
    # section response가 올바른지 평가
    if response.ok:
        try:
            bsObj = BeautifulSoup(response.text,'html.parser')
            tot_nums = bsObj.\
                        find('ul',{'class':"snb_list"}).\
                        find('a',{'data-filter-value':'total'}).\
                        text
            re_num = re.compile("[\d,]+")#pattern for number, ex:[12,301 | 12,113,131]
            tot_nums = int(re_num.findall(tot_nums)[0].replace(",",""))
            if tot_nums > 100000:
                # section 결과가 너무 많으면( > 100000) -> Exception
                raise Exception("검색 결과가 너무 많습니다．좀 더 적은 keyword를 입력해주세요")
        except AttributeError as e:
            # Keyword의 상품이 없으면 -> None
            print("검색결과가 없습니다.\nurl:\n{}".format(response.url))
            return 0
    else:
        raise Exception("bad http status. url : {}".format(response.url))
    
    return tot_nums        

def parse_item_contents_in_section(section):
    re_won = re.compile("[\d,]+원")
    re_date = re.compile("[\d.]+")
    # 제품 URL
    item_url = section.find('a',{'class':'tit'}).attrs['href']
    # Redirected된 최종 URL
    item_target_url = get_internal_url(item_url)
    # 제품 ID
    item_id = section.attrs['data-nv-mid']
    # 제품 이름
    item_name = section.find('a',{'class':'tit'}).attrs['title']
    # 가격
    item_price = re_won.findall(
                        section.find('span',{'class':'price'}).text)

    if len(item_price) > 1:
        item_min_price, item_max_price = item_price[:2]
    else:
        item_min_price, item_max_price = item_price[0], item_price[0]
    
    # item의 카테고리
    item_category = section.\
                    find('span',{'class':'depth'}).\
                    text.\
                    replace("\n","").\
                    replace(" ","").\
                    replace("\t","")
    
    # 아이템 등록 날짜
    item_date = section.\
                find('span',{'class':'date'}).\
                text
    item_date = re_date.findall(item_date)[0]
    
    return {
            'name' : item_name,
            'id' : item_id,
            'url'  : item_url,
            'target_url' : item_target_url,
            'min_price' : item_min_price,
            'max_price' : item_max_price,
            'category' : item_category,
            'date' : item_date,
    }

def get_internal_url(url):
    # 네이버 내부 쇼핑몰사이트인경우，주소 반환
    global SCP_INTERVAL, INTERNAL, EXTERNAL
    time.sleep(SCP_INTERVAL)
    response = urlopen(url)
    parse_result = urlparse(response.geturl())
    if parse_result.netloc in EXTERNAL:
        return None
    elif parse_result.netloc in INTERNAL:
        return response.geturl()

def get_naver_shopping_review(nvMid):
    global HEADERS, NAVER_SHOPPING_URL, NAVER_SHOPPING_PARAMS
    params = NAVER_SHOPPING_PARAMS.copy()
    
    with requests.Session() as sess: 
        sess.headers.update(HEADERS)
        params['nvMid'] = nvMid
        params['page'] = 1
        reviews_list = []
        while True:
            sess.params.update(params)
            time.sleep(SCP_INTERVAL)
            response = sess.post(NAVER_SHOPPING_URL)
            if response.ok:
                bsObj = BeautifulSoup(response.text,'html.parser')
                try:
                    atc_areas = bsObj.find_all('div',{'class':'atc_area'})
                except AttributeError as e : 
                    raise exception("there is no attribute in bsObj\n{}".format(e))
            else:
                raise exception("bad")

            if len(atc_areas) > 0 :
                for atc_area in atc_areas:        
                    review = parse_naver_shopping_review(atc_area)
                    reviews_list.append(review)
            else:
                return reviews_list
            
            params['page'] += 1

def parse_naver_shopping_review(atc_area):
    review_atc = atc_area.find("div",{'class':'atc'}).text
    review_atc = review_atc.replace('\n',"").\
                            replace("\t","").strip()
    review_grade = atc_area.find('span',{'class':'curr_avg'}).text
    review_date = atc_area.find("span",{'class':'date'}).text
    re_date = re.compile("[\d.]+")
    review_date = re_date.findall(review_date)[0]
    review_path = atc_area.find("span",{'class':'path'}).text
    return {
        'nvMid': nvMid,
        'atc' : review_atc,
        'grade': review_grade,
        'date': review_date,
        'path': review_path
    }