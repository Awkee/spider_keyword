#!/usr/bin/env python3
# -*- coding: utf-8 -*-
################################################################################
# Author: zioer
# mail: xiaoyu0720@gmail.com
# Created Time: 2020年08月26日 星期三 12时53分25秒
# Brief:
################################################################################
import requests
from lxml import etree
from datetime import datetime
from parsel import Selector
import re
from pymongo import MongoClient


HEADERS_PC = {
    'User-Agent': 'Mozilla/5.0 (Windows x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'
}
# MongoDB config
DEFAULT_MONGO_URI = 'mongodb://myTestUser:abcd1234@localhost:27017/keywords'
DEFAULT_WEIBO_COOKIE = r'UM_distinctid=17468324a24b8e-0b277c780614b3-1d251809-1f7442-17468324a25d0e; SINAGLOBAL=6818766575328.816.1599489278592; SUB=_2AkMoA6LDf8NxqwJRmf8Xy2_naYlxyQDEieKeX1MYJRMxHRl-yT9kqm9ZtRB6A4OMLH2UeyOP91xt8tRacWkIqgbxJvYu; SUBP=0033WrSXqPxfM72-Ws9jqgMF55529P9D9W5i9sLvopsCIVR.-7UzRDLN; login_sid_t=44de1e4cdbb8b243072d58728cc22e51; cross_origin_proto=SSL; _s_tentry=passport.weibo.com; Apache=9245615733765.613.1600073207524; ULV=1600073207537:2:2:2:9245615733765.613.1600073207524:1599489279528; CNZZDATA1272960323=342704353-1600071974-%7C1600071974; TC-Page-G0=62b98c0fc3e291bc0c7511933c1b13ad|1600073353|1600073198'


class SocialKey(object):
    '''
    社会热点关键词爬取
    '''
    def __init__(self, mongo_uri=DEFAULT_MONGO_URI, db='keywords', coll='social'):
        '''初始化信息'''
        self.keyword_list = []
        self.timestamp = datetime.timestamp(datetime.now())
        self.key_list = ['rank', 'keyword', 'platform', 'type', 'timestamp']
        self.mongo_uri = mongo_uri
        self.mongo_db = db
        self.mongo_coll = coll
        self.client = MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]
        self.doc = self.db[self.mongo_coll]
        self.cookie_weibo = DEFAULT_WEIBO_COOKIE

    def add_item(self, rank, keyword, platform, ptype, timestamp=None):
        item = {}
        if timestamp is None:
            timestamp = self.timestamp
        item['rank'] = rank
        item['keyword'] = keyword
        item['platform'] = platform
        item['type'] = ptype
        item['timestamp'] = timestamp
        print('item:', item)
        self.keyword_list.append(item)

    def get_headers(self, ptype=None):
        '''获取Headers字典'''
        headers = HEADERS_PC
        if ptype == 'weibo':
            headers['referrer'] = "https://weibo.com/"
            headers['authority'] = 'weibo.com'
            headers['cookie'] = self.cookie_weibo
        return headers

    def social_key_baidu(self):
        '''百度热搜关键词'''
        url = 'http://top.baidu.com/buzz?b=1'
        resp = requests.get(url, headers=self.get_headers())
        doc = etree.HTML(resp.content.decode('gb2312'))
        tr_list = doc.xpath(r'//*[@id="main"]/div[2]/div/table/tr[not(contains(@class, "item-tr"))]')
        for tr in tr_list[1:]:
            td_list = tr.xpath('./td')
            nid = td_list[0].xpath('string(.)').strip()
            rank = int(nid)
            key = td_list[1].xpath('string(./a[@class="list-title"])').strip()
            score = td_list[3].xpath('string(.)').strip()
            self.add_item(rank, key, 'baidu', 'social')

    def social_key_weibo_hot_topic(self):
        '''微博热搜话题榜'''
        headers = self.get_headers('weibo')
        url_home = 'https://d.weibo.com/231650'
        for page in range(1, 6):
            url = f'{url_home}??cfs=920&Pl_Discover_Pt6Rank__3_filter=&Pl_Discover_Pt6Rank__3_page={page}'
            resp = requests.get(url, headers=headers)
            res_list = re.findall(r'"html":"(.*)"}\)</script>', resp.text)
            result = res_list[-1]

            a_list = re.sub(r'<.*?>|\\r|\\n|\\t|上一页|下一页|TOP', '', result)
            a_list = re.sub('\s*?主持人', ',主持人', a_list)
            a_list = a_list.split()[0:-1]
            items = re.findall(r'([0-9]*?)(#.*?#).*阅读数:(.*(?:万|亿))?(?:,主持人:)?(.*)?', '\n'.join(a_list))
            for item in items:
                rank = int(item[0])
                key = item[1]
                self.add_item(rank, key, 'weibo', 'social')

    def social_key_weibo_hot_realtime(self):
        '''微博热搜榜'''
        headers = self.get_headers('weibo')

        url = 'https://s.weibo.com/top/summary?cate=realtimehot'
        resp = requests.get(url, headers=headers)
        selector = Selector(resp.text)
        tr_list = selector.xpath('//div[@id="pl_top_realtimehot"]/table/tbody/tr')
        for tr in tr_list[1:]:
            rank = tr.xpath('./td[1]/text()').get()
            rank = int(rank)
            hotword = tr.xpath('./td[2]/a/text()').get()
            search_num = tr.xpath('./td[2]/span/text()').get()
            self.add_item(rank, hotword, 'weibo', 'social')

    def save_to_mongo(self):
        '''数据保存到MongoDB'''
        return self.doc.insert_many(self.keyword_list)

    def process(self):
        '''任务处理'''
        self.social_key_baidu()
        self.social_key_weibo_hot_realtime()
        self.save_to_mongo()

    def show_keyword(self):
        for key in self.keyword_list:
            print(key)

if __name__ == '__main__':
    social_key = SocialKey()
    social_key.process()
