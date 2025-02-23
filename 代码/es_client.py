from elasticsearch import Elasticsearch, helpers
from tenacity import retry, wait_exponential, stop_after_attempt
import logging
from datetime import datetime
from typing import List, Dict, Optional
from urllib.parse import urlparse

# 配置日志记录
logging.basicConfig(
    format='[%(asctime)s] %(levelname)s: %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


class ESClient:
    def __init__(self):
        """初始化 Elasticsearch 客户端（修复版）"""
        self.index_alias = "web_data"
        self.elastic = Elasticsearch(
            hosts=["http://localhost:9200"],
            request_timeout=30,
            max_retries=3
        )
        self._ensure_index()

    def _ensure_index(self):
        """确保索引和别名正确存在"""
        try:
            # 存在别名则跳过创建
            if self.elastic.indices.exists_alias(name=self.index_alias):
                return

            # 创建带时间戳的新索引
            index_name = f"{self.index_alias}-{datetime.now().strftime('%Y%m%d%H%M%S')}"
            self.elastic.indices.create(
                index=index_name,
                body=self._get_index_mapping(),
                timeout="2m"
            )
            # 绑定别名
            self.elastic.indices.put_alias(
                index=index_name,
                name=self.index_alias
            )
            logger.info(f"创建新索引 {index_name} 并绑定别名")
        except Exception as e:
            logger.error(f"索引初始化失败: {str(e)}")
            raise

    @staticmethod
    def _get_index_mapping() -> Dict:
        """优化后的索引映射配置"""
        return {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "analysis": {
                    "normalizer": {
                        "lowercase": {
                            "type": "custom",
                            "filter": ["lowercase"]
                        }
                    }
                }
            },
            "mappings": {
                "properties": {
                    "url": {"type": "keyword"},
                    "content": {
                        "type": "text",
                        "analyzer": "ik_max_word",
                        "search_analyzer": "ik_smart"
                    },
                    "timestamp": {"type": "date"},
                    "domain": {
                        "type": "keyword",
                        "normalizer": "lowercase"  # 自动小写化
                    }
                }
            }
        }

    @retry(
        wait=wait_exponential(multiplier=1, max=10),
        stop=stop_after_attempt(3),
        reraise=True
    )
    def bulk_index(self, docs: List[Dict]) -> tuple[int, list]:
        """修复版批量写入方法"""
        actions = []
        for doc in docs:
            try:
                # 处理无效URL
                parsed = urlparse(doc["url"])
                domain = parsed.netloc or "invalid_domain"

                actions.append({
                    "_op_type": "index",
                    "_index": self.index_alias,
                    "_source": {
                        "url": doc["url"],
                        "content": doc["content"],
                        "timestamp": datetime.now().isoformat(),
                        "domain": domain.lower().strip()  # 统一小写
                    }
                })
            except Exception as e:
                logger.error(f"文档处理失败: {str(e)}")

        try:
            success, failed = helpers.bulk(
                self.elastic,
                actions,
                stats_only=False,
                request_timeout=60
            )
            # 写入后强制刷新
            self.elastic.indices.refresh(index=self.index_alias)
            return success, failed
        except Exception as e:
            logger.error(f"批量写入异常: {str(e)}")
            return 0, []

    def search(self, query: Dict) -> Optional[Dict]:
        """修复版搜索方法"""
        try:
            return self.elastic.search(
                index=self.index_alias,
                body={"query": query},  # 修正查询结构
                filter_path=["hits.hits._source", "hits.total.value"]
            )
        except Exception as e:
            logger.error(f"搜索失败: {str(e)}")
            return None


# 测试用例
if __name__ == "__main__":
    client = ESClient()

    # 测试数据
    test_data = [{
        "url": "http://test.com",
        "content": "Elasticsearch集成测试内容"
    }]

    # 写入测试
    success, errors = client.bulk_index(test_data)
    if success == 1:
        logger.info("数据写入成功")

        # 查询测试
        result = client.search({
            "term": {
                "domain": "test.com"  # 直接使用keyword字段
            }
        })
        logger.info(f"搜索结果: {result['hits']['total']['value']}")
    else:
        logger.error("数据写入失败")
