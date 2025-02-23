import re
import asyncio
from urllib.parse import urlparse
from datetime import datetime, timedelta
import aiohttp
import logging
from lxml import etree
from typing import Dict, Any

logging.basicConfig(
    format="[%(asctime)s] %(levelname)s: %(message)s",
    level=logging.INFO
)


class RobotsCache:
    def __init__(self):
        self.cache: Dict[str, Dict[str, Any]] = {}
        self.lock = asyncio.Lock()
        self.session: aiohttp.ClientSession = None
        self.default_crawl_delay = 5
        self.connector = aiohttp.TCPConnector(limit_per_host=10)  # 连接池限制

    async def __aenter__(self):
        await self._init_session()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self._close_session()

    async def _init_session(self):
        """异步初始化客户端会话"""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(
                connector=self.connector,
                timeout=aiohttp.ClientTimeout(total=15)
            )

    async def _close_session(self):
        """安全关闭会话"""
        if self.session and not self.session.closed:
            await self.session.close()
            self.session = None

    async def get_rules(self, url: str) -> Dict[str, Any]:
        """获取域名robots规则"""
        if not self.session:
            await self._init_session()

        domain = urlparse(url).netloc
        if not domain:
            return self._default_rules()

        async with self.lock:
            cache_entry = self.cache.get(domain)
            if cache_entry and datetime.now() < cache_entry["expire"]:
                return cache_entry["rules"]

            try:
                rules = await self._fetch_robots_txt(domain)
                self.cache[domain] = {
                    "expire": datetime.now() + timedelta(hours=24),
                    "rules": rules
                }
                return rules
            except Exception as e:
                logging.error(f"Robots.txt fetch failed for {domain}: {str(e)}")
                return self._default_rules()

    def _default_rules(self) -> Dict[str, Any]:
        """默认允许所有访问的规则"""
        return {
            "disallow": [],
            "allow": [],
            "crawl_delay": self.default_crawl_delay,
            "sitemaps": []
        }

    async def _fetch_robots_txt(self, domain: str) -> Dict[str, Any]:
        """获取并解析robots.txt"""
        robots_url = f"http://{domain}/robots.txt"
        try:
            async with self.session.get(robots_url) as resp:
                if resp.status == 200:
                    content = await resp.text()
                    return self._parse_robots(content)
                return self._default_rules()
        except aiohttp.ClientError as e:
            logging.warning(f"Network error fetching {robots_url}: {str(e)}")
            return self._default_rules()

    def _parse_robots(self, content: str) -> Dict[str, Any]:
        """解析robots.txt内容"""
        rules = {
            "disallow": [],
            "allow": [],
            "crawl_delay": self.default_crawl_delay,
            "sitemaps": []
        }

        current_ua = None
        for line in content.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            parts = line.split(":", 1)
            if len(parts) != 2:
                continue

            key = parts[0].strip().lower()
            value = parts[1].strip()

            if key == "user-agent":
                current_ua = value.lower()
            elif current_ua == "*" or current_ua is None:
                if key == "disallow":
                    pattern = re.compile(value.replace("*", ".*") + ".*")
                    rules["disallow"].append(pattern)
                elif key == "allow":
                    pattern = re.compile(value.replace("*", ".*") + ".*")
                    rules["allow"].append(pattern)
                elif key == "crawl-delay":
                    try:
                        rules["crawl_delay"] = max(int(value), self.default_crawl_delay)
                    except ValueError:
                        pass
                elif key == "sitemap":
                    rules["sitemaps"].append(value)

        return rules

    async def is_allowed(self, url: str) -> bool:
        try:
            rules = await self.get_rules(url)
            path = urlparse(url).path

            # 添加详细日志
            logging.info(f"Checking access for: {url}")
            logging.debug(f"Disallow patterns: {rules['disallow']}")
            logging.debug(f"Allow patterns: {rules['allow']}")

            # ...原有判断逻辑...
        except Exception as e:
            logging.error(f"Robots检查异常: {str(e)}")
            return True

            # 检查禁止规则



# 使用示例
async def main():
    async with RobotsCache() as cache:
        test_urls = [
            "https://example.com/private",
            "https://google.com/search",
            "https://twitter.com/private"]

        for url in test_urls:
            allowed = await cache.is_allowed(url)
            status = "ALLOWED" if allowed else "BLOCKED"
            print(f"{url}: {status}")


if __name__ == "__main__":
    asyncio.run(main())
