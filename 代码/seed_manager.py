import redis
from urllib.parse import urlparse
import logging

logging.basicConfig(level=logging.INFO)

class SeedManager:
    def __init__(self):
        try:
            # Docker 环境中使用服务名 'redis'
            self.conn = redis.Redis(host='redis', port=6379, db=0)
            self.seen_key = "seen_urls"
            self.conn.ping()  # 测试连接
        except redis.exceptions.ConnectionError as e:
            logging.error(f"无法连接到 Redis: {e}")
            raise
        self.seed_key = "seed_urls"

    def add_seed(self, url):
        # URL标准化处理
        parsed = urlparse(url)
        clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}".lower()

        if self.conn.sadd(self.seen_key, clean_url) == 1:
            self.conn.sadd(self.seed_key, clean_url)
            logging.info(f"新增种子URL: {clean_url}")
        else:
            logging.info(f"跳过重复URL: {clean_url}")

    def get_seeds(self):
        seeds = self.conn.smembers(self.seed_key)
        logging.info(f"当前种子: {seeds}")
        return seeds