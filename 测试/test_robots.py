import asyncio
from robots_parser import RobotsCache


async def test_robots_compliance():
    """测试robots协议合规性"""
    test_cases = [
        ("https://twitter.com/private", False),
        ("https://github.com/public", True),
        ("https://example.com/robots.txt", True)
    ]

    async with RobotsCache() as cache:
        for url, expected in test_cases:
            actual = await cache.is_allowed(url)
            status = "ALLOWED" if actual else "BLOCKED"
            print(f"{url}: {status}")
            assert actual == expected


if __name__ == "__main__":
    asyncio.run(test_robots_compliance())
