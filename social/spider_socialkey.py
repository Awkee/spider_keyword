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

class SocialKey(object):
    '''
    社会热点关键词爬取
    '''
    def __init__(self, mongo_uri = DEFAULT_MONGO_URI, db='keywords', coll='social'):
        '''
        初始化信息
        '''
        self.keyword_list = []
        self.timestamp = datetime.timestamp(datetime.now())
        self.key_list = ['rank', 'keyword', 'platform', 'type', 'timestamp']
        self.mongo_uri = mongo_uri
        self.mongo_db = db
        self.mongo_coll = coll
        self.client = MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]
        self.doc = self.db[self.mongo_coll]

    def add_item(self, rank, keyword, platform, ptype, timestamp=None):
        item = {}
        if timestamp is None:
            timestamp = self.timestamp
        item['rank'] = rank
        item['keyword'] = keyword
        item['platform'] = platform
        item['type'] = ptype
        item['timestamp'] = timestamp
        self.keyword_list.append(item)

    def social_key_baidu(self):
        '''百度热搜关键词'''
        url = 'http://top.baidu.com/buzz?b=1'
        resp = requests.get(url, headers=HEADERS_PC)
        doc = etree.HTML(resp.content.decode('gb2312'))
        tr_list = doc.xpath(r'//*[@id="main"]/div[2]/div/table/tr[not(contains(@class, "item-tr"))]')
        for tr in tr_list[1:]:
            td_list = tr.xpath('./td')
            nid = td_list[0].xpath('string(.)').strip()
            key = td_list[1].xpath('string(./a[@class="list-title"])').strip()
            score = td_list[3].xpath('string(.)').strip()
            self.add_item(nid, key, 'baidu', 'social')
        resp.close()

    def social_key_weibo_hot_topic(self):
        '''微博热搜话题榜'''
        headers = HEADERS_PC
        headers['referrer'] = "https://weibo.com/"
        headers['authority'] = 'weibo.com'
        headers['cookie'] = 'SUB=_2AkMoGCnzf8NxqwJRmf8QzWjkbol3zgzEieKeRNgoJRMxHRl-yT9jqkM5tRB6A5gHHGlll66nBhQKS-cig_GPibaICo5Z; SUBP=0033WrSXqPxfM72-Ws9jqgMF55529P9D9WF70N0ypJucppYCyTVSmvZo; login_sid_t=72d0d19dda405cc237b42b6d95b256c3; cross_origin_proto=SSL; _s_tentry=passport.weibo.com; Apache=1136604738481.4927.1598334665588; SINAGLOBAL=1136604738481.4927.1598334665588; ULV=1598334665592:1:1:1:1136604738481.4927.1598334665588:; TC-Page-G0=62b98c0fc3e291bc0c7511933c1b13ad|1598335214|1598335126'
        
        url_home = 'https://d.weibo.com/231650'
        for page in range(1,6):
            url = f'{url_home}??cfs=920&Pl_Discover_Pt6Rank__3_filter=&Pl_Discover_Pt6Rank__3_page={page}' 
            resp = requests.get(url, headers = headers)
            res_list = re.findall(r'"html":"(.*)"}\)</script>', resp.text)
            result = res_list[-1]

            a_list = re.sub(r'<.*?>|\\r|\\n|\\t|上一页|下一页|TOP', '' , result)
            a_list = re.sub('\s*?主持人', ',主持人', a_list)
            a_list = a_list.split()[0:-1]
            items = re.findall(r'([0-9]*?)(#.*?#).*阅读数:(.*(?:万|亿))?(?:,主持人:)?(.*)?', '\n'.join(a_list))
            for item in items:
                nid = int(item[0])
                key = item[1]
                self.add_item(nid, key, 'weibo', 'social')

    def social_key_weibo_hot_realtime(self):
        '''微博热搜榜'''
        headers = HEADERS_PC
        headers['referrer'] = "https://weibo.com/"
        headers['authority'] = 'weibo.com'
        headers['cookie'] = 'SUB=_2AkMoGCnzf8NxqwJRmf8QzWjkbol3zgzEieKeRNgoJRMxHRl-yT9jqkM5tRB6A5gHHGlll66nBhQKS-cig_GPibaICo5Z; SUBP=0033WrSXqPxfM72-Ws9jqgMF55529P9D9WF70N0ypJucppYCyTVSmvZo; login_sid_t=72d0d19dda405cc237b42b6d95b256c3; cross_origin_proto=SSL; _s_tentry=passport.weibo.com; Apache=1136604738481.4927.1598334665588; SINAGLOBAL=1136604738481.4927.1598334665588; ULV=1598334665592:1:1:1:1136604738481.4927.1598334665588:; TC-Page-G0=62b98c0fc3e291bc0c7511933c1b13ad|1598335214|1598335126'
        url='https://s.weibo.com/top/summary?cate=realtimehot'
        resp = requests.get(url, headers = headers)
        selector = Selector(resp.text)
        tr_list = selector.xpath('//div[@id="pl_top_realtimehot"]/table/tbody/tr')
        for tr in tr_list[1:]:
            rank = tr.xpath('./td[1]/text()').get()
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
    social_key.show_keyword()

