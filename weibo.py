#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import psycopg2
import urllib3
import logging
import logging.config
from datetime import datetime
from utils import standardize_date

logging.config.fileConfig('logging.conf')
logger = logging.getLogger('weibo')

class Item:
    def __init__(self, mblog):
        self.lang = 'cn'
        self.src = 'weibo.cn'
        self.subcat = 'weibo'
        self.cat = 'weibo'
        self.body = mblog
        self.meta = {'comment': [], 'repost': [], 'longtext': ''}

class weibo:
    def __init__(self):
        self.http = urllib3.PoolManager()
        self.weibo_url = 'https://m.weibo.cn/api/container/getIndex?'
        self.comment_url = 'https://api.weibo.cn/2/comments/build_comments?'
        self.repost_url = 'https://m.weibo.cn/api/statuses/repostTimeline?'
        self.HEADERS = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Encoding': 'gzip, deflate, sdch',
            'Accept-Language': 'zh-CN,zh;q=0.8,en;q=0.6,ja;q=0.4,zh-TW;q=0.2,mt;q=0.2',
            'Connection': 'keep-alive',
            'Host': 'm.weibo.cn',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 11_3_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest',
            # 'cookie':
        }
        hostname = ''
        username = ''
        password = ''
        database = 'weibo'
        self.connection = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
        self.cur = self.connection.cursor()

    def start_crawl(self):
        user_ids = self.read_from_postgre()
        now_time = datetime.now()

        for user_id in user_ids:
            self.cur.execute('''UPDATE userid 
                                SET last_crawl = %s
                                where id = %s;''', (now_time, user_id[0]))
            self.connection.commit()
            self.parse_all_mblog(user_id[0], date_start=user_id[1], date_end=now_time)

    def close_crawl(self):
        self.cur.close()
        self.connection.close()

    def read_from_postgre(self):
        now_time = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M')
        print(now_time)
        self.cur.execute("SELECT id, last_crawl FROM userid where next_crawl<=%s or next_crawl is NULL;", (now_time,))
        user_ids = self.cur.fetchall()
        logger.info(u'获取到%d位用户id', len(user_ids))
        return user_ids

    def write_to_postgre(self, item):
        self.cur.execute("INSERT INTO mblog(lang, src, cat, subcat, meta, body) VALUES(%s, %s, %s, %s, %s, %s);", (item.lang, item.src, item.subcat, item.cat, str(item.meta), str(item.body)))
        self.connection.commit()

    def parse_all_mblog(self, user_id, date_start, date_end):
        logger.info(u'用户%s开始爬取', user_id)
        date_start = date_start if date_start else datetime.strptime("2009-8-14 00:00", '%Y-%m-%d %H:%M')
        for i in range(1, 201):
            url = f'{self.weibo_url}containerid=107603{user_id}&page={i}'
            r = self.http.request('GET', url, headers=self.HEADERS)
            if r.status != 200:
                logger.warning(u'<Response %s> from %s', r.status, url)
            try:
                r = json.loads(r.data)
                flag = True
                if r['ok']:
                    # 设置下一次爬取的时间
                    if url.endswith('page=1'):
                        interval = standardize_date(r['data']['cards'][0]['mblog']['created_at']) - standardize_date(r['data']['cards'][-1]['mblog']['created_at'])
                        next_crawl = datetime.now() + interval

                        self.cur.execute('''UPDATE userid 
                            SET next_crawl = %s
                            where id = %s;''', (next_crawl, user_id))
                        self.connection.commit()

                    for card in r['data']['cards']:
                        if card['card_type'] == 9:
                            created_at = standardize_date(card['mblog']['created_at'])
                            if date_start <= created_at and created_at <= date_end:
                                self.parse_mblog(card['mblog'])
                            elif date_end < created_at: # 微博在采集时间段后
                                continue
                            else:   # 微博超过需采集的时间
                                flag = False
                                break

                    if flag == False:
                        break
            except Exception as e:
                logger.exception(e)

        logger.info(u'用户%s停止爬取', user_id)
            
    def parse_mblog(self, mblog):
        item = Item(mblog)
        mblog_id = mblog['id']
        if mblog['comments_count'] > 0:
            item.meta['comment'] = self.parse_comment(mblog_id)
        if mblog['reposts_count'] > 0:
            item.meta['repost'] = self.parse_repost(mblog_id)
        if mblog['isLongText']:
            item.meta['longtext'] = self.parse_all_content(mblog_id)
        self.write_to_postgre(item)

    def parse_comment(self, mblog_id):
        url = f'{self.comment_url}is_show_bulletin=2&c=android&s=746fd605&id={mblog_id}&from=10A8195010&gsid=_2AkMolNMzf8NhqwJRmf4dxWzgb49zzQrEieKeyCLoJRM3HRl-wT9jqmwMtRV6AgOZP3LqGBH-29qGRB4vP3j-Hng6DkBJ&count=50&max_id_type=1'
        r = self.http.request('GET', url, headers={'Host': 'api.weibo.cn'})
        try:
            r = json.loads(r.data)
            comments = []
            for comment in r['root_comments']:
                comments.append(comment)

            # 翻页
            max_id = r['max_id']
            while max_id > 0:
                max_id_type = r['max_id_type']
                next_url = f'{self.comment_url}is_show_bulletin=2&c=android&s=746fd605&id={mblog_id}&from=10A8195010&gsid=_2AkMolNMzf8NhqwJRmf4dxWzgb49zzQrEieKeyCLoJRM3HRl-wT9jqmwMtRV6AgOZP3LqGBH-29qGRB4vP3j-Hng6DkBJ&count=50&max_id={max_id}&max_id_type={max_id_type}'
                r = self.http.request('GET', next_url, headers={'Host': 'api.weibo.cn'})
                r = json.loads(r.data)
                max_id = r['max_id']
                for comment in r['root_comments']:
                    comments.append(comment)
                    try:
                        self.cur.execute('''INSERT INTO userid (id) VALUES (%s);'''%comment['user']['idstr'])
                    except Exception as e:
                        logger.exception(e)

            self.connection.commit()
            return comments
        except Exception as e:
            logger.exception(e)

    def parse_repost(self, mblog_id):
        url = f"{self.repost_url}id={mblog_id}&page=1"
        r = self.http.request('GET', url, headers=self.HEADERS)
        r = json.loads(r.data)
        reposts = []
        if r['ok']:
            all_page = r['data']['max']
            if all_page >= 1:
                for page_num in range(all_page + 1):
                    page_url = url.replace('page=1', 'page={}'.format(page_num))
                    r =  self.http.request('GET', page_url, headers=self.HEADERS)
                    r = json.loads(r.data)
                    if r['ok']:
                        for repost in r['data']['data']:
                            reposts.append(repost)
                        # 把转发中的userid加入数据库
                        try:
                            self.cur.execute('''INSERT INTO userid (id) VALUES (%s);'''%str(repost['user']['id']))
                        except Exception as e:
                            logger.exception(e)
        self.connection.commit()
        return reposts

    def parse_all_content(self, mblog_id):
        url = 'https://m.weibo.cn/detail/' + mblog_id
        r = self.http.request('GET', url, headers=self.HEADERS)
        html = r.data
        html = html[html.find(b'"status":'):]
        html = html[:html.rfind(b'"hotScheme"')]
        html = html[:html.rfind(b',')]
        html = b'{' + html + b'}'
        js = json.loads(html, strict=False)
        weibo_info = js.get('status')
        if weibo_info:
            return weibo_info['text']

def main():
    crawl = weibo()
    crawl.start_crawl()
    crawl.close_crawl()

if __name__ == '__main__':
    main()