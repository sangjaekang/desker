# -*- coding: utf-8 -*-
import click
import logging
import time
import argparse
from multiprocessing import Process, Queue

from parser import Categoryparser, Itemparser, Reviewparser
from crawler import Categorycrawler, Brand_crawl_parser, Itemcrawler, Reviewcrawler
from crawl_utils import Redissaver, Redisloader

def run_category(host,category):
    logger.info("run category crawling --- start!")
    res_queue = Queue(100)
    saver = Redissaver('category',host=host,init=True)

    crawler = Categorycrawler([category],res_queue)
    parser = Categoryparser(res_queue,saver)

    start = time.time()
    crawler.start()
    time.sleep(1)
    parser.start()

    parser.join()
    crawler.join()
    end = time.time()
    logger.info("run category crawling --- end! consumed time --- {}".format(end-start))

def run_brand(host):
    logger.info("run brand crawling --- start!")
    loader = Redisloader('category',host=host, cond=lambda x : x.get("cat2_name",0) == "서재/사무용가구")
    saver = Redissaver('brand',host=host,init=True)

    crawler = Brand_crawl_parser(loader, saver)
    start = time.time()
    crawler.start()
    crawler.join()
    end = time.time()
    logger.info("run brand crawling --- end! consumed time --- {}".format(end-start))

def run_item(host):
    logger.info("run item crawling --- start!")
    loader = Redisloader('brand',host=host)
    saver = Redissaver('item',host=host,init=True,store_type='list')
    task_saver = Redissaver('nvmid',host=host,store_type='hmset')
    item_queue = Queue(100)
    crawler = Itemcrawler(loader,item_queue)
    parser = Itemparser(item_queue,saver,task_saver)

    start = time.time()
    crawler.start()
    time.sleep(1)
    parser.start()

    crawler.join()
    parser.join()
    end = time.time()
    logger.info("run item crawling --- end! consumed time --- {}".format(end-start))

def run_review(host):
    logger.info("run review crawling --- start!")
    saver = Redissaver('review', host=host, init=True, store_type='list')
    updater = Redissaver('nvmid', host=host, store_type='hmset_update')
    loader = Redisloader('nvmid', host=host, store_type='hmset')
    res_queue = Queue(100)

    crawler = Reviewcrawler(loader,updater,res_queue)
    parser = Reviewparser(res_queue,saver)

    start = time.time()
    crawler.start()
    time.sleep(1)
    parser.start()

    crawler.join()
    parser.join()
    end = time.time()
    logger.info("run review crawling --- end! consumed time --- {}".format(end-start))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--type',choices=['category','brand','item','review'])
    parser.add_argument("--host",default='localhost')
    parser.add_argument('--category', default='50000004')
    args = parser.parse_args()

    fh = logging.FileHandler('crawl_app.log')
    fh.setLevel(logging.DEBUG)

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    logger = logging.getLogger("crawl_app")
    logger.setLevel(logging.INFO)
    logger.addHandler(fh)
    logger.addHandler(ch)


    if args.type == 'category':
        run_category(args.host,args.category)
    elif args.type == 'brand':
        run_brand(args.host)
    elif args.type == 'item':
        run_item(args.host)
    elif args.type == 'review':
        run_review(args.host)

