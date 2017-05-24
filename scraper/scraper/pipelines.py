from scrapy.exceptions import DropItem


class TradeShowExhibitorBase(object):
    def open_spider(self, spider):
        self.settings = spider.custom_settings


class TradeShowExhibitorPipeline(TradeShowExhibitorBase):
    def process_item(self, item, spider):
        for field in self.settings['REQUIRED_FIELDS']:
            if not item.get(field):
                raise DropItem
        return item


class TradeShowExhibitorSqlitePipeline(TradeShowExhibitorBase):
    def open_spider(self, spider):
        import sqlite3
        super(TradeShowExhibitorSqlitePipeline, self).open_spider(spider)

        self.pk_field = self.settings['SQLITE_FIELD_MAP']['pk']

        self.fields = {key: val for key, val in self.settings['SQLITE_FIELD_MAP'].items() if key != 'pk'}

        field_defs = []
        for field in self.fields:
            if field == self.pk_field:
                field_defs.append("%s VARCHAR UNIQUE" % field)
            else:
                field_defs.append("%s VARCHAR" % field)

        table_def = "CREATE TABLE IF NOT EXISTS exhibitors (%s)" % ', '.join(field_defs)
        self.connection = sqlite3.connect(self.settings['SQLITE_FILENAME'])
        cursor = self.connection.cursor()
        cursor.execute(table_def)

    def close_spider(self, spider):
        self.connection.close()

    def process_item(self, item, spider):
        existing_item_query = "SELECT company_name, website FROM exhibitors WHERE company_name = '%s'" % item['exhibitor_name']
        cursor = self.connection.cursor()
        existing_item = cursor.execute(existing_item_query).fetchone()
        if existing_item:
            update_fields = []
            for db_field, item_field in self.fields.items():
                if db_field != self.pk_field:
                    update_fields.append("%s = '%s'" % (db_field, item[item_field]))

            upsert_sql = "UPDATE exhibitors SET %s WHERE %s = '%s'" % (
                ', '.join(update_fields),
                self.pk_field,
                item[self.fields[self.pk_field]]
            )
        else:
            insert_fields = []
            insert_vals = []
            for db_field, item_field in self.fields.items():
                insert_fields.append(db_field)
                insert_vals.append("'%s'" % item[item_field])

            upsert_sql = "INSERT INTO exhibitors (%s) VALUES (%s)" % (
                ', '.join(insert_fields),
                ', '.join(insert_vals)
            )
        print(upsert_sql)
        with self.connection:
            self.connection.cursor().execute(upsert_sql)
        return item
