import os
import sqlite3
from datetime import datetime
from typing import List, Tuple

import scrapy

DB_PATH = os.path.abspath(
    "/market_data.db"
)

class NikkeiReportSpider(scrapy.Spider):
    name = "nikkeireport"

    # 礼儀優先の保守的デフォルト（必要に応じて起動時 -s で上書きOK）
    custom_settings = {
        "ROBOTSTXT_OBEY": True,
        "CONCURRENT_REQUESTS": 2,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "DOWNLOAD_DELAY": 2,
        "RANDOMIZE_DOWNLOAD_DELAY": True,

        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 3,
        "AUTOTHROTTLE_MAX_DELAY": 30,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 0.5,

        "RETRY_ENABLED": True,
        "RETRY_TIMES": 3,
        "RETRY_HTTP_CODES": [500, 502, 503, 504, 522, 524, 408, 429],

        "HTTPCACHE_ENABLED": True,  # 同一URLの再取得を減らして礼儀＋体感速度UP
        "HTTPCACHE_DIR": "httpcache",
        "DOWNLOAD_TIMEOUT": 25,

        "USER_AGENT": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
        ),
        "DEFAULT_REQUEST_HEADERS": {"Referer": "https://www.google.com"},
        "LOG_LEVEL": "INFO",
    }

    def __init__(self, target_date=None, mode="missing", batch_size=50, *args, **kwargs):
        """
        :param target_date: YYYYMMDD（必須：検証は start_requests で実施）
        :param mode: 'missing'（未取得のみ） / 'all'（全件） 既定: missing
        :param batch_size: バッチ保存件数（既定: 50）
        """
        super().__init__(*args, **kwargs)
        self.target_date = target_date
        self.mode = mode if mode in ("missing", "all") else "missing"
        try:
            self.batch_size = int(batch_size) if batch_size else 50
            if self.batch_size <= 0:
                self.batch_size = 50
        except Exception:
            self.batch_size = 50

        # DB
        self.conn = sqlite3.connect(DB_PATH)
        self.cur = self.conn.cursor()
        self.cur.execute("PRAGMA journal_mode=WAL;")
        self.cur.execute("PRAGMA synchronous=NORMAL;")
        self.conn.commit()
        self._init_db()

        self._buf: List[Tuple] = []
        self._total = 0
        self._queued = 0
        self._parsed = 0

    def _init_db(self):
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS nikkei_reports (
                target_date   TEXT,
                code          TEXT,
                sector        TEXT,
                name          TEXT,
                price         TEXT,
                per           TEXT,
                yield_rate    TEXT,
                pbr           TEXT,
                roe           TEXT,
                earning_yield TEXT,
                UNIQUE(code, target_date)
            )
        """)
        self.conn.commit()

    def start_requests(self):
        # 必須引数の検証（scrapy list 互換のためここで）
        if not self.target_date:
            raise ValueError("実行時に -a target_date=YYYYMMDD の形式で日付を指定してください")
        datetime.strptime(self.target_date, "%Y%m%d")

        # 対象抽出：missing は未保存のみ、all は全件
        if self.mode == "missing":
            self.cur.execute("""
                SELECT c.code, c.nikkeiurl
                  FROM consensus_url c
             LEFT JOIN nikkei_reports n
                    ON n.code = c.code AND n.target_date = c.target_date
                 WHERE c.target_date = ? AND n.code IS NULL
            """, (self.target_date,))
        else:
            self.cur.execute("""
                SELECT code, nikkeiurl
                  FROM consensus_url
                 WHERE target_date = ?
            """, (self.target_date,))

        rows = [(c, u) for c, u in self.cur.fetchall() if u]
        self._total = len(rows)
        if not rows:
            self.logger.info(f"[INIT] 対象0件（date={self.target_date}, mode={self.mode}）")
            return

        self.logger.info(
            f"[INIT] date={self.target_date} mode={self.mode} total={self._total} batch={self.batch_size}"
        )
        for code, url in rows:
            self._queued += 1
            if self._queued % 50 == 0 or self._queued == self._total:
                self.logger.info(f"[QUEUE] {self._queued}/{self._total}")
            yield scrapy.Request(url, callback=self.parse, meta={"code": code}, dont_filter=True)

    # ------- utils -------
    def _get(self, resp: scrapy.http.Response, xp: str) -> str:
        try:
            v = resp.xpath(xp).get()
            return v.strip() if v else "N/A"
        except Exception:
            return "N/A"

    def _pct(self, s: str) -> str:
        s = (s or "").strip()
        if not s:
            return "N/A"
        return s if s.endswith("%") else s + "%"

    # ------- parse -------
    def parse(self, response: scrapy.http.Response):
        code = response.meta["code"]

        sector = self._get(response, '//*[@id="CONTENTS_MAIN"]/div[1]/span[1]/a/text()')
        name   = self._get(response, '//*[@id="CONTENTS_MAIN"]/div[3]/div/div/h1/text()')
        price  = self._get(response, '//*[@id="CONTENTS_MAIN"]/div[4]/dl[1]/dd/text()')

        per = self._get(response,
            '//*[@id="JSID_stockInfo"]/div[1]/div[1]/div[1]/div[2]/ul/li[2]/span[2]/text()')
        yield_rate = self._pct(self._get(response,
            '//*[@id="JSID_stockInfo"]/div[1]/div[1]/div[1]/div[2]/ul/li[3]/span[2]/text()'))

        pbr = self._get(response,
            '//*[@id="JSID_stockInfo"]/div[3]/div/div[1]/ul/li[1]/span[2]/text()')
        roe = self._pct(self._get(response,
            '//*[@id="JSID_stockInfo"]/div[3]/div/div[1]/ul/li[2]/span[2]/text()'))
        earning_yield = self._pct(self._get(response,
            '//*[@id="JSID_stockInfo"]/div[3]/div/div[1]/ul/li[3]/span[2]/text()'))

        self._buf.append((
            self.target_date, code, sector, name, price, per,
            yield_rate, pbr, roe, earning_yield
        ))
        self._parsed += 1

        if len(self._buf) >= self.batch_size:
            self._flush()

        if self._parsed % 50 == 0 or self._parsed == self._total:
            self.logger.info(f"[PARSE] {self._parsed}/{self._total}")

    def _flush(self):
        if not self._buf:
            return
        self.cur.executemany("""
            INSERT OR IGNORE INTO nikkei_reports (
                target_date, code, sector, name, price, per,
                yield_rate, pbr, roe, earning_yield
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, self._buf)
        self.conn.commit()
        self.logger.info(f"[DB] commit {len(self._buf)} rows")
        self._buf.clear()

    def closed(self, reason):
        try:
            self._flush()
        finally:
            try:
                self.conn.close()
            except Exception:
                pass
        self.logger.info(f"[CLOSE] reason={reason} total={self._total} parsed={self._parsed}")
