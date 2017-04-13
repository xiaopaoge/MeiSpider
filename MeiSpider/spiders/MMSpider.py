# -*- coding: utf-8 -*-

from scrapy import *
import logging, psycopg2, re

header = {
    'Referer': 'http://wh.meituan.com/',
    'User-Agent': 'Baiduspider'
}

re_location = re.compile("marker=coord:([^;]+)")
re_price = re.compile("人均：¥(.+)")


class MSpider(Spider):
    name = 'M'
    result = {}
    attr = {
        u'\u505c\u8f66\u4f4d': 9,
        u'\u8425\u4e1a\u65f6\u95f4': 10,
        'WIFI': 11,
        u'\u95e8\u5e97\u4ecb\u7ecd': 12
    }
    poi_count = {}
    poi_url = 'http://i.meituan.com/poi/{0}'
    feedback_url = 'http://i.meituan.com/poi/{0}/feedbacks/page_{1}'
    conn = psycopg2.connect("dbname=postgres user=postgres host=localhost port=5439")

    def __init__(self):
        cursor = self.conn.cursor()
        cursor.execute("select * from poi.meituan_poi")
        self.poi_ids = cursor.fetchall()

    def start_requests(self):
        for poi_info in self.poi_ids:
            yield Request(self.poi_url.format(poi_info[0]), meta={'poi_info': poi_info}, callback=self.parse_poi)
            #yield Request(self.feedback_url.format(poi_info[0],1), meta={'poi_info': poi_info,'page':1},
            #              callback=self.parse_feedbacks)

    def parse_poi(self, response):
        item = []
        item.append(response.meta['poi_info'][0])
        item.append(response.meta['poi_info'][1])
        # name
        try:
            item.append(response.xpath('//h1//text()').extract_first())
        except:
            item.append(None)
        # location
        try:
            item.append(re_location.findall(
                response.xpath("//a[contains(@href,'http://apis.map.qq.com/tools/poimarker')]//@href").extract_first())[
                            0])
        except:
            item.append(None)
        # address
        try:
            item.append(response.xpath("//div[@class='poi-address']//text()").extract_first())
        except:
            item.append(None)
        # star
        try:
            item.append(response.xpath("//em[@class='star-text']//text()").extract_first())
        except:
            item.append(None)
        # price
        try:
            item.append(response.xpath("//span[@class='avg-price']//text()").extract_first()[4:])
        except:
            item.append(None)
        # phone
        try:
            item.append(response.xpath("//a[@data-com='phonecall']//@data-tele").extract_first())
        except:
            item.append(None)
        # recommond_list
        try:
            item.append(','.join(response.xpath("//span[@class='recommond-item']//text()").extract()))
        except:
            item.append(None)
        item.append(None)
        item.append(None)
        item.append(None)
        item.append(None)
        for x in response.xpath("//*[@id='poi-summary']/dd/dl/dd"):
            n = x.xpath(".//h6//text()").extract_first()
            t = x.xpath(".//p//text()").extract()
            if n is None:
                continue
            if t.__len__()>1:
                t=t[0]+t[1]
            elif t.__len__()==1:
                t=t[0]
            else:
                t=None
            item[self.attr[n]] = t

        self.conn.cursor().execute("insert into poi.meituan_pois VALUES ({0}) on CONFLICT do nothing".format(
            ','.join(list('%s' for i in range(13)))), tuple(item))
        self.conn.commit()

    def parse_feedbacks(self, response):
        poi_info=response.meta['poi_info']
        page=response.meta['page']
        for x in response.xpath("//div[@class='feedbackCard']"):
            username=x.xpath(".//weak[@class='username']//text()").extract_first()
            score = x.xpath(".//div[@class='score']//i[@class='text-icon icon-star']").extract().__len__()
            time = x.xpath(".//weak[@class='time']//text()").extract_first()
            pics = x.xpath(".//div[@class='pics']//@data-pics").extract_first()
            comment = x.xpath(".//div[@class='comment']//p//text()").extract_first()
            more_comment = x.xpath(".//span[@class='feedbackmore']//text()").extract_first()
            if more_comment is not None:
                comment = comment + more_comment
            reply = x.xpath(".//div[@class='block-reply']//p//text()").extract_first()
            self.conn.cursor().execute("insert into poi.meituan_comments VALUES ({0}) on CONFLICT do nothing".format(
                ','.join(list('%s' for i in range(7)))), (poi_info[0],username,comment,score,time,pics,reply))
            self.conn.commit()
        if response.xpath("//a[@gaevent='imt/deal/feedbacklist/pageNext']").extract_first() is not None:
            yield Request(self.feedback_url.format(poi_info[0],page+1), meta={'poi_info': poi_info,'page':page+1},
                          callback=self.parse_feedbacks)