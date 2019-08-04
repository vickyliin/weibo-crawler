#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import functools
import json
import math
import random
import sys
from argparse import ArgumentParser, FileType
from collections import OrderedDict
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from itertools import chain, zip_longest
from pathlib import Path
from time import sleep
from typing import SupportsInt

import pandas as pd
import requests
from lxml import etree


class random_sleep:
    """通过加入随机等待避免被限制。
    爬虫速度过快容易被系统限制(一段时间后限制会自动解除)，加入随机等待模拟人的操作，可降低被系统限制的风险。
    每爬取 freq[0] 到 freq[1] 页随机等待 time[0] 到 time[1] 秒。
    """

    def __init__(self, freq, time):
        self.freq = freq
        self.time = time
        self.reset()

    def reset(self):
        self.next_sleep = {
            'n_steps': random.randint(*self.freq),
            'time': random.randint(*self.time)
        }

    def __call__(self, fn):
        @functools.wraps(fn)
        def wrapped(*args, **kwargs):
            self.next_sleep['n_steps'] -= 1
            if self.next_sleep['n_steps'] == 0:
                sleep(self.next_sleep['time'])
                self.reset()
            return fn(*args, **kwargs)
        return wrapped


@dataclass
class UserWeibo:
    name: str
    id: SupportsInt
    followers_count: int
    statuses_count: int
    description: str = field(repr=False)
    avatar: str = field(repr=False)
    page: int = 1
    _iter: bool = field(default=False, repr=False)

    API_URL = 'https://m.weibo.cn/api/container/getIndex'

    @classmethod
    def from_id(cls, id):
        params = {'containerid': f'100505{id}'}
        response = cls._get_json(params)
        if response['ok']:
            info = response['data']['userInfo']
            if info.get('toolbar_menus'):
                del info['toolbar_menus']
            info = cls._standardize_info(info)
            return cls(
                info['screen_name'], info['id'],
                info['followers_count'],
                info['statuses_count'],
                info['description'],
                info['avatar_hd']
            )

        err = ValueError(f'Cannot find user info for id {cls.uid}')
        err.response = response
        raise err

    @classmethod
    def _get_json(cls, params):
        """获取网页中json数据"""
        r = requests.get(cls.API_URL, params=params)
        return r.json()

    @random_sleep(freq=(1, 5), time=(6, 10))
    def _get_page(self, page):
        """获取网页中微博json数据"""
        params = {'containerid': f'107603{self.id}', 'page': page}
        return self._get_json(params)

    def __len__(self):
        """获取微博页数"""
        weibo_count = self.statuses_count
        return math.ceil(weibo_count / 10)

    @staticmethod
    def _is_weibo(wb):
        return wb['card_type'] == 9 and 'retweeted_status' not in wb['mblog']

    @staticmethod
    def _get_pics(weibo_info):
        """获取微博原始图片url"""
        pic_info = weibo_info.get('pics', [])
        return [pic['large']['url'] for pic in pic_info]

    @staticmethod
    def _get_topics(selector):
        """获取参与的微博话题"""
        span_list = selector.xpath("//span[@class='surl-text']")
        topics = []
        for span in span_list:
            text = span.xpath('string(.)')
            if len(text) > 2 and text[0] == '#' and text[-1] == '#':
                topics.append(text[1:-1])
        return topics

    @staticmethod
    def _get_at_users(selector):
        """获取@用户"""
        a_list = selector.xpath('//a')
        return [a.xpath('string(.)')[1:] for a in a_list
                if '@' + a.xpath('@href')[0][3:] == a.xpath('string(.)')]

    @staticmethod
    def _string_to_int(string):
        """字符串转换为整数"""
        if isinstance(string, int):
            return string
        elif string.endswith('万+'):
            string = int(string[:-2] + '0000')
        elif string.endswith('万'):
            string = int(string[:-1] + '0000')
        return int(string)

    @staticmethod
    def _standardize_date(created_at):
        """标准化微博发布时间"""
        if u"刚刚" in created_at:
            created_at = datetime.now()
        elif u"分钟" in created_at:
            minute = created_at[:created_at.find(u"分钟")]
            minute = timedelta(minutes=int(minute))
            created_at = datetime.now() - minute
        elif u"小时" in created_at:
            hour = created_at[:created_at.find(u"小时")]
            hour = timedelta(hours=int(hour))
            created_at = datetime.now() - hour
        elif u"昨天" in created_at:
            day = timedelta(days=1)
            created_at = datetime.now() - day
        else:
            if created_at.count('-') == 1:
                year = datetime.now().strftime("%Y")
                created_at = year + "-" + created_at
            created_at = datetime.strptime(created_at, '%Y-%m-%d')
        return created_at

    @staticmethod
    def _standardize_info(weibo):
        """标准化信息，去除乱码"""
        for k, v in weibo.items():
            if isinstance(v, str):
                weibo[k] = v.replace(u"\u200b", "").encode(
                    sys.stdout.encoding, "ignore").decode(sys.stdout.encoding)
        return weibo

    @classmethod
    def get_long_weibo(cls, id):
        """获取长微博"""
        url = f'https://m.weibo.cn/detail/{id}'
        html = requests.get(url).text
        html = html[html.find('"status":'):]
        html = html[:html.rfind('"hotScheme"')]
        html = html[:html.rfind(',')]
        html = '{' + html + '}'
        js = json.loads(html, strict=False)
        weibo_info = js['status']
        weibo = cls._parse_weibo(weibo_info)
        return weibo

    @classmethod
    def _parse_weibo(cls, weibo_info):
        weibo = OrderedDict()
        weibo['user_id'] = weibo_info['user']['id']
        weibo['user_name'] = weibo_info['user']['screen_name']
        weibo['id'] = int(weibo_info['id'])
        text_body = weibo_info['text']
        selector = etree.HTML(text_body)
        weibo['text'] = etree.HTML(text_body).xpath('string(.)')
        weibo['images'] = cls._get_pics(weibo_info)
        weibo['created'] = cls._standardize_date(weibo_info['created_at'])
        weibo['attitudes_count'] = cls._string_to_int(
            weibo_info['attitudes_count'])
        weibo['comments_count'] = cls._string_to_int(
            weibo_info['comments_count'])
        weibo['reposts_count'] = cls._string_to_int(
            weibo_info['reposts_count'])
        weibo['topics'] = cls._get_topics(selector)
        weibo['at_users'] = cls._get_at_users(selector)
        weibo['is_long_text'] = weibo_info['isLongText']
        return cls._standardize_info(weibo)

    def __next__(self):
        """获取一页的全部微博"""
        if self._iter and self.page >= len(self):
            raise StopIteration()

        response = self._get_page(self.page)
        self.page += 1
        if not response['ok']:
            return

        weibos = response['data']['cards']
        page = [self._parse_weibo(wb['mblog'])
                for wb in weibos if self._is_weibo(wb)]
        return page

    def __iter__(self):
        """获取全部微博"""
        return replace(self, _iter=True)


def save_user_weibos(uids, f):
    users = [UserWeibo.from_id(uid) for uid in uids]
    try:
        for n, pages in enumerate(zip_longest(*users, fillvalue=[]), 1):
            weibos = (weibo for page in pages for weibo in page)
            for weibo in weibos:
                json.dump(weibo, f,
                          ensure_ascii=False,
                          default=datetime.isoformat)
                f.write('\n')
    except KeyboardInterrupt:
        print(f'\rInterrupted at page {n}')


def main():
    ap = ArgumentParser(__file__)
    ap.add_argument('-id', type=int, nargs='+', default=[],
                    help='Weibo user ids to collect.')
    ap.add_argument('-csv', type=pd.read_csv,
                    default=pd.DataFrame([], columns=['id']),
                    help='Path to a csv file with column "id" of user id.')
    ap.add_argument('-txt', type=lambda fp: [s.strip() for s in open(fp)],
                    default=[],
                    help='Path to a txt file with a user id per line.')
    ap.add_argument('-out', type=FileType('w'), default=sys.stdout,
                    help='Path to store output (*.jsonl), default to stdout')
    args = ap.parse_args()
    uids = chain(args.id, args.csv.id, args.txt)
    save_user_weibos(uids, args.out)


if __name__ == '__main__':
    main()
