import scrapy
import csv
from datetime import datetime

class NikkeiSpider(scrapy.Spider):
    name = "nikkeireport"

    def start_requests(self):
        self.output_file = f"nikkeireport_{datetime.now().strftime('%Y-%m-%d_%H-%M')}.csv"
        self.headers = [
            "証券コード", "セクター", "企業名", "株価", "予想PER", "予想配当利回り",
            "PBR（実績）", "ROE（予想）", "株式益回り（予想）"
        ]

        self.outfile = open(self.output_file, "w", newline='', encoding="utf-8")
        self.writer = csv.writer(self.outfile)
        self.writer.writerow(self.headers)

        with open("/Users/fukuotannaka/Desktop/FILE/DataAnalysis/money/NIKKEIFinancial/consensus2025-07-16_08-01.csv", newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                yield scrapy.Request(
                    url=row["日本経済新聞"].strip(),
                    callback=self.parse,
                    meta={"code": row["証券コード"]}
                )

    def extract(self, response, xpath):
        try:
            return response.xpath(xpath).get().strip()
        except Exception:
            return "N/A"

    def parse(self, response):
        code_val_raw = self.extract(response, '//*[@id="CONTENTS_MAIN"]/div[1]/span[1]/text()')
        code_val = code_val_raw.split(":")[0].strip()
        sector = self.extract(response, '//*[@id="CONTENTS_MAIN"]/div[1]/span[1]/a/text()')
        name = self.extract(response, '//*[@id="CONTENTS_MAIN"]/div[3]/div/div/h1/text()')
        price = self.extract(response, '//*[@id="CONTENTS_MAIN"]/div[4]/dl[1]/dd/text()')
        per = self.extract(response, '//*[@id="JSID_stockInfo"]/div[1]/div[1]/div[1]/div[2]/ul/li[2]/span[2]/text()')
        yield_rate = self.extract(response, '//*[@id="JSID_stockInfo"]/div[1]/div[1]/div[1]/div[2]/ul/li[3]/span[2]/text()')
        pbr = self.extract(response, '//*[@id="JSID_stockInfo"]/div[3]/div/div[1]/ul/li[1]/span[2]/text()')
        roe = self.extract(response, '//*[@id="JSID_stockInfo"]/div[3]/div/div[1]/ul/li[2]/span[2]/text()')
        earning_yield = self.extract(response, '//*[@id="JSID_stockInfo"]/div[3]/div/div[1]/ul/li[3]/span[2]/text()')

        self.writer.writerow([
            code_val, sector, name, price, per, yield_rate,
            pbr, roe, earning_yield
        ])

    def closed(self, reason):
        self.outfile.close()
