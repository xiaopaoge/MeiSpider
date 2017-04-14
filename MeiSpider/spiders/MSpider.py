# -*- coding: utf-8 -*-

from scrapy import *
import logging, psycopg2, re,threading

header = {
    'Referer': 'http://wh.meituan.com/',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36'
}

r = re.compile("//i.meituan.com/poi/([0-9]+)")


class MSpider(Spider):
    name = 'MSpider'
    running={}
    cids = {
               10: 'KTV',
               11: '甜点饮品',
               17: '火锅',
               19: '培训课程',
               20: '酒店',
               24: '其他美食',
               25: '其他生活',
               26: '其他娱乐',
               28: '日韩料理',
               20510: '整形',
               32: '体检/齿科',
               20514: '婴儿游泳',
               35: '西餐',
               36: '小吃快餐',
               37: '演出赛事',
               38: '桌游/电玩',
               40: '自助餐',
               41: '咖啡酒吧',
               52: '足疗按摩',
               54: '烧烤烤肉',
               55: '川湘菜',
               56: '江浙菜',
               57: '粤菜',
               58: '西北菜',
               59: '京菜鲁菜',
               60: '云贵菜',
               62: '东南亚菜',
               63: '海鲜',
               65: '鲜花',
               68: '配镜',
               74: '美发',
               75: '美甲美睫',
               76: '美容美体',
               79: '经济型酒店',
               80: '豪华酒店',
               112: '洗浴/汗蒸',
               20629: '驾校',
               20506: '美容SPA',
               20641: '个性写真',
               162: '景点门票',
               20507: '祛痘',
               20508: '瘦身纤体',
               20512: '化妆品',
               20515: '孕妇写真',
               20692: '宠物店',
               20693: '宠物医院',
               217: '素食',
               218: 'DIY手工',
               219: '真人CS',
               220: '瑜伽舞蹈',
               221: '照片冲印',
               20517: '亲子购物',
               20704: '超市/便利店',
               20706: '高星酒店',
               227: '台湾/客家菜',
               228: '创意菜',
               229: '汤/粥/炖菜',
               230: '密室逃脱',
               233: '新疆菜',
               235: '商场购物卡',
               20767: '汽车改装',
               20768: '汽车配件',
               20769: '低星酒店',
               20780: '公寓民宿',
               20781: '家庭出行',
               20782: '其它亲子游乐',
               20785: '宝宝派对',
               20786: '其他亲子',
               20787: '少儿英语',
               20788: '幼儿才艺',
               20789: '智力开发',
               20791: '其他幼儿教育',
               20792: '产后恢复',
               20793: '妇幼医院',
               20796: '月嫂',
               20798: '其他孕产护理',
               20802: '快餐简餐',
               20803: '面条',
               20804: '麻辣烫',
               20805: '米粉米线',
               20807: '西班牙菜',
               20808: '浙江菜',
               20810: '杭帮菜',
               20811: '烤鱼',
               20812: '干锅香锅',
               20813: '川味菜',
               20814: '烤串',
               20815: '韩式烤肉',
               20816: '粤菜',
               20817: '茶餐厅',
               20818: '客家菜',
               20819: '潮州菜',
               20821: '贵州菜',
               381: '主题酒店',
               382: '度假酒店/度假村',
               383: '公寓型酒店',
               384: '客栈',
               385: '青年旅社',
               393: '代金券',
               395: '聚餐宴请',
               396: '婚纱摄影',
               397: '个性写真',
               398: '亲子摄影',
               399: '其他摄影',
               400: '中式烧烤/烤串',
               20003: '东北菜',
               20004: '香锅烤鱼',
               20007: '母婴亲子',
               20042: '母婴护理',
               20044: '科普场馆',
               20045: '幼儿教育',
               20048: '儿童摄影',
               20049: '手工DIY',
               20050: '采摘/农家乐',
               20057: '服饰鞋包',
               20093: '婚庆',
               20097: '生日蛋糕',
               20104: '点播电影',
               20108: '儿童乐园',
               20115: '洗车',
               20118: '维修保养',
               20120: '汽车美容',
               20121: '汽车陪练',
               20130: '精洗',
               20134: '其他洗车',
               20135: '汽车用品',
               20516: '月子会所',
               20164: '更多服务',
               20171: '更多亲子服务',
               20176: '其他酒店',
               20180: '装修设计',
               20196: '证件照',
               20198: '婚纱摄影',
               20199: '婚纱礼服',
               20200: '西服定制',
               20201: '婚庆公司',
               20202: '婚戒首饰',
               20203: '婚礼小礼品',
               20204: '婚车租赁',
               20205: '彩妆造型',
               20218: '健身中心',
               20220: '舞蹈',
               20221: '游泳馆',
               20222: '羽毛球馆',
               20223: '篮球场',
               20224: '网球场',
               20227: '桌球馆',
               20228: '体育场馆',
               20230: '乒乓球馆',
               20231: '武术场馆',
               20232: '马术',
               20233: '溜冰',
               20234: '壁球',
               20235: '射箭射击',
               20236: '更多运动',
               20250: '内饰清洁',
               20252: '运动健身',
               20253: '健身房/体操房',
               20254: '瑜伽',
               20255: '舞蹈',
               20256: '游泳/水上运动',
               20257: '羽毛球',
               20258: '篮球',
               20259: '网球',
               20262: '台球',
               20263: '综合体育场馆',
               20265: '乒乓球',
               20266: '武术',
               20267: '骑马',
               20268: '滑冰',
               20269: '壁球',
               20270: '射箭',
               20271: '其他运动',
               20275: '医院',
               20276: '齿科口腔',
               20277: '体检中心',
               20283: '健康服务',
               20286: '外语培训',
               20287: '音乐培训',
               20288: '美术培训',
               20289: '教育院校',
               20290: '更多教育培训',
               20291: '职业技术',
               20292: '升学辅导',
               20293: '留学',
               20294: '兴趣生活',
               20302: '打蜡',
               20303: '旅拍婚照',
               20306: '英语',
               20307: '日语',
               20312: '其他外语',
               20313: '钢琴',
               20314: '吉他',
               20316: '古筝',
               20317: '架子鼓',
               20318: '声乐',
               20319: '其他音乐培训',
               20320: '绘画',
               20321: '书法',
               20322: '其他美术',
               20323: '美容化妆',
               20324: '会计',
               20325: 'IT',
               20327: '其他职业培训',
               20331: '其他院校',
               20333: '小学辅导',
               20334: '中学辅导',
               20336: '其他升学辅导',
               20346: '打蜡',
               20347: '玻璃贴膜',
               20348: '内饰清洁',
               20349: '镀晶镀膜',
               20350: '小保养',
               20351: '补胎',
               20352: '四轮定位',
               20353: '钣金喷漆',
               20354: '普洗',
               20382: '内饰清洁',
               20383: '时尚购',
               20384: '本地购物',
               20386: '配镜',
               20387: '鲜花',
               20418: '化妆品',
               20419: '韩式定妆',
               20420: '纹身',
               20421: '祛痘',
               20422: '瘦身纤体',
               20423: '整形',
               20425: '脱毛',
               20642: '数码家电',
               20457: '开锁'}
    poi_count = {}
    conn = psycopg2.connect("dbname=postgres user=postgres password = 123456 host=localhost port=5439")

    def start_requests(self):
        # yield Request('http://wh.meituan.com/',headers=header,callback=self.parse)
        for cid in self.cids:
            pagenum = 1
            self.poi_count[self.cids[cid]] = 0
            self.running[self.cids[cid]]=True
            while self.running[self.cids[cid]]:
                yield Request('http://i.meituan.com/wuhan?cid={0}&p={1}&cateType=poi'.format(cid, pagenum)
                              , headers=header, callback=self.parse_list, meta={'cid': self.cids[cid]})
                pagenum += 1
        logging.info(self.poi_count)

    def parse_list(self, response):
        if response.xpath("//div[@class='no-deals']").extract_first() is not None:
            self.running[response.meta['cid']] = False
            return
        poi_links = response.xpath("//a[contains(@class,'react')]/@href").extract()
        for poi in poi_links:
            id = r.findall(poi)
            if id.__len__() == 1:
                self.poi_count[response.meta['cid']] += 1
                self.conn.cursor().execute('insert into poi.meituan_poi VALUES (%s,%s) on CONFLICT do nothing',
                                           (id[0], response.meta['cid']))
                self.conn.commit()
