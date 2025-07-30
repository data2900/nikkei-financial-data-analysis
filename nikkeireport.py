import scrapy
import sqlite3
from datetime import datetime
import os

# DBファイルのパスを設定
DB_PATH = os.path.abspath("/market_data.db")

class NikkeiReportSpider(scrapy.Spider):
    name = "nikkeireport"

    def __init__(self, target_date=None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if not target_date:
            raise ValueError("実行時に -a target_date=YYYYMMDD の形式で日付を指定してください")

        try:
            datetime.strptime(target_date, "%Y%m%d")
        except ValueError:
            raise ValueError("日付の形式が正しくありません。YYYYMMDD形式で指定してください")

        self.target_date = target_date
        self.conn = sqlite3.connect(DB_PATH)
        self.cursor = self.conn.cursor()
        self.init_db()

    def init_db(self):
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS nikkei_reports (
                target_date TEXT,
                code TEXT,
                sector TEXT,
                name TEXT,
                price TEXT,
                per TEXT,
                yield_rate TEXT,
                pbr TEXT,
                roe TEXT,
                earning_yield TEXT,
                UNIQUE(code, target_date)
            )
        """)
        self.conn.commit()

    def start_requests(self):
        self.cursor.execute("""
            SELECT code, nikkeiurl FROM consensus_url WHERE target_date = ?
        """, (self.target_date,))
        records = self.cursor.fetchall()

        for code, url in records:
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                meta={"code": code}
            )

    def extract(self, response, xpath):
        try:
            return response.xpath(xpath).get().strip()
        except Exception:
            return "N/A"

    def format_percent(self, value):
        if value and value != "N/A" and not value.endswith("%"):
            return value + "%"
        return value

    def parse(self, response):
        code = response.meta["code"]
        sector = self.extract(response, '//*[@id="CONTENTS_MAIN"]/div[1]/span[1]/a/text()')
        name = self.extract(response, '//*[@id="CONTENTS_MAIN"]/div[3]/div/div/h1/text()')
        price = self.extract(response, '//*[@id="CONTENTS_MAIN"]/div[4]/dl[1]/dd/text()')
        per = self.extract(response, '//*[@id="JSID_stockInfo"]/div[1]/div[1]/div[1]/div[2]/ul/li[2]/span[2]/text()')
        yield_rate = self.format_percent(self.extract(response, '//*[@id="JSID_stockInfo"]/div[1]/div[1]/div[1]/div[2]/ul/li[3]/span[2]/text()'))
        pbr = self.extract(response, '//*[@id="JSID_stockInfo"]/div[3]/div/div[1]/ul/li[1]/span[2]/text()')
        roe = self.format_percent(self.extract(response, '//*[@id="JSID_stockInfo"]/div[3]/div/div[1]/ul/li[2]/span[2]/text()'))
        earning_yield = self.format_percent(self.extract(response, '//*[@id="JSID_stockInfo"]/div[3]/div/div[1]/ul/li[3]/span[2]/text()'))

        self.cursor.execute("""
            INSERT OR IGNORE INTO nikkei_reports (
                target_date, code, sector, name, price, per,
                yield_rate, pbr, roe, earning_yield
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            self.target_date, code, sector, name, price, per,
            yield_rate, pbr, roe, earning_yield
        ))

        self.conn.commit()

    def closed(self, reason):
        self.conn.close()
