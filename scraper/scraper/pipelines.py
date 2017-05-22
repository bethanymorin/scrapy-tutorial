from scrapy.exceptions import DropItem


class TradeShowExhibitorPipeline(object):
    def process_item(self, item, spider):
        if not item['website_url'] or not item['exhibitor_name']:
            raise DropItem
        return item


class TradeShowExhibitorSqlitePipeline(object):
    def open_spider(self, spider):
        import sqlite3
        self.connection = sqlite3.connect('exhibitors.db')
        cursor = self.connection.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS exhibitors (company_name VARCHAR UNIQUE, website VARCHAR)")

    def close_spider(self, spider):
        self.connection.close()

    def process_item(self, item, spider):
        existing_item_query = "SELECT company_name, website FROM exhibitors WHERE company_name = '%s'" % item['exhibitor_name']
        cursor = self.connection.cursor()
        existing_item = cursor.execute(existing_item_query).fetchone()
        if existing_item:
            upsert_sql = "UPDATE exhibitors SET website = '%s' WHERE company_name = '%s'" % (
                item['exhibitor_name'],
                item['website_url']
            )
        else:
            upsert_sql = "INSERT INTO exhibitors (company_name, website) VALUES ('%s', '%s')" % (
                item['exhibitor_name'],
                item['website_url']
            )
        print upsert_sql
        with self.connection:
            self.connection.cursor().execute(upsert_sql)
        return item
