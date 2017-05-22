import scrapy


class TradeShowExhibitor(scrapy.Item):
    website_url = scrapy.Field()
    exhibitor_name = scrapy.Field()
