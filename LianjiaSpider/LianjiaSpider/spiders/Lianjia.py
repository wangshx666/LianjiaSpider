# -*- coding: utf-8 -*-
import scrapy

from LianjiaSpider.items import SZHouseItem


class LianjiaSpider(scrapy.Spider):
    name = 'Lianjia'
    # allowed_domains = ['https://sz.lianjia.com/ershoufang/']
    start_urls = ['https://sz.lianjia.com/ershoufang/']

    def parse(self, response):
        # 获取二手房页面的最大页数，构造翻页
        total_page = eval(response.css('div.page-box.house-lst-page-box::attr(page-data)').get())['totalPage']
        for page in range(1, total_page + 1):
            url = 'https://sz.lianjia.com/ershoufang/pg{num}'.format(num=str(page))
            yield scrapy.Request(url=url, callback=self.parsePage)

    def parsePage(self, response):
        # 获取每个二手房详情页的链接
        links = response.css('div.leftContent ul.sellListContent li a::attr(href)').getall()
        for link in links:
            yield scrapy.Request(url=link, callback=self.parseHouse)

    def parseHouse(self, response):
        SZHouse = SZHouseItem()
        url = 'https://sz.lianjia.com/ershoufang/'
        SZHouse['href'] = url + response.css('.aroundInfo .houseRecord span.info::text').get() + ".html"
        SZHouse['name'] = response.css('.aroundInfo .communityName a.info::text').get()
        SZHouse['unit_price'] = response.css('.price .total::text').get()
        SZHouse['total_price'] = response.css('.price .unitPriceValue::text').get()
        res = response.css('.base .content li::text').getall()
        SZHouse['room'] = res[0]
        SZHouse['area'] = res[2]
        SZHouse['floor'] = res[1]
        SZHouse['time'] = response.css('.transaction .content li span::text').getall()[1]
        print(SZHouse)
        yield SZHouse
