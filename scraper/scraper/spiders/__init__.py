from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scraper.items import TradeShowExhibitor
from bs4 import BeautifulSoup


class HrTech2017(CrawlSpider):
    name = "hrtech2017"
    allowed_domains = ["s23.a2zinc.net"]
    start_urls = ["http://s23.a2zinc.net/clients/lrp/hrtechnologyconference2017/Public/exhibitors.aspx?Index=All"]
    custom_settings = {
        'ITEM_PIPELINES': {
            'scraper.pipelines.TradeShowExhibitorPipeline': 60,
            'scraper.pipelines.TradeShowExhibitorSqlitePipeline': 6,
        },
        'CLOSESPIDER_ITEMCOUNT': 60,
        'CONCURRENT_REQUESTS': 6,
    }

    rules = [
        Rule(
            # rule for exhibitor detail link
            LinkExtractor(
                allow=(
                    u'\/(clients/lrp/hrtechnologyconference2017/Public/eBooth.aspx\?)',
                ),
                deny='robots.txt',
            ),
            callback='parse_exhibitor_item',
            follow=False,
        ),
    ]

    def parse_exhibitor_item(self, response):
        item = TradeShowExhibitor()
        html = BeautifulSoup(response.body, "html.parser")
        item['exhibitor_name'] = html.find('div', {'id': 'eboothContainer'}).find('h1').text
        contact_url_obj = html.find('a', {'id': 'BoothContactUrl'})
        if contact_url_obj:
            item['website_url'] = contact_url_obj.text
        yield item
