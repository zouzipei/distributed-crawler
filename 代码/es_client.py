# es_client.py
from elasticsearch import Elasticsearch, helpers
from tenacity import retry, wait_exponential, stop_after_attempt
import logging
from datetime import datetime
from typing import List, Dict
from urllib.parse import urlparse

logging.basicConfig(level=logging.INFO)


class ESClient:
    def __init__(self):
        # 使用固定别名 + 时间戳索引模式
        self.index_alias = "web_data"  # 对外暴露的固定别名
        self.elastic = Elasticsearch(
            hosts=["http://kafka:9200"],
            verify_certs=False,
            ssl_show_warn=False,
            request_timeout=30  # 新参数名
        )

        # 初始化时确保别名存在
        if not self._check_alias_exists():
            self._create_initial_index()

    def _check_alias_exists(self) -> bool:
        """检查别名是否存在"""
        try:
            return self.elastic.indices.exists_alias(name=self.index_alias)
        except Exception as e:
            logging.error(f"检查别名失败: {str(e)}")
            return False

    def _create_initial_index(self):
        """创建初始索引并设置别名"""
        index_name = f"{self.index_alias}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        try:
            # 创建索引
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
            logging.info(f"初始索引 {index_name} 创建成功，别名 {self.index_alias} 已绑定")
        except Exception as e:
            logging.error(f"初始化索引失败: {str(e)}")
            raise

    def _get_index_mapping(self) -> Dict:
        return {
            "mappings": {
                "properties": {
                    "url": {
                        "type": "keyword",  # 确保keyword类型
                        "ignore_above": 256  # 防止长URL被截断
                    },
                    "content": {
                        "type": "text",
                        "analyzer": "ik_analyzer",
                        "fields": {  # 添加子字段
                            "keyword": {"type": "keyword"}
                        }
                    },
                    # 其他字段保持不变...
                }
            }
        }

        """获取索引映射配置"""
        return {
            "settings": {
                "number_of_shards": 3,
                "number_of_replicas": 1,
                "analysis": {
                    "analyzer": {
                        "ik_analyzer": {
                            "type": "custom",
                            "tokenizer": "ik_max_word"
                        }
                    }
                }
            },
            "mappings": {
                "properties": {
                    "url": {"type": "keyword"},
                    "content": {
                        "type": "text",
                        "analyzer": "ik_analyzer",
                        "search_analyzer": "ik_smart"
                    },
                    "timestamp": {"type": "date"},
                    "domain": {"type": "keyword"}
                }
            }
        }
    @retry(
        wait=wait_exponential(multiplier=1, max=10),
        stop=stop_after_attempt(5),
        reraise=True
    )
    def rotate_index(self):
        """滚动创建新索引（用于定期维护）"""
        try:
            # 1. 创建新索引
            new_index = f"{self.index_alias}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
            self.elastic.indices.create(
                index=new_index,
                body=self._get_index_mapping()
            )

            # 2. 切换别名
            self.elastic.indices.update_aliases({
                "actions": [
                    {"remove": {"alias": self.index_alias, "index": f"{self.index_alias}_*"}},
                    {"add": {"alias": self.index_alias, "index": new_index}}
                ]
            })
            logging.info(f"索引已滚动更新至 {new_index}")
            return new_index
        except Exception as e:
            logging.error(f"索引滚动失败: {str(e)}")
            raise

    @retry(
        wait=wait_exponential(multiplier=1, max=10),
        stop=stop_after_attempt(3),
        reraise=True
    )
    def bulk_index(self, docs: List[Dict]):
        """批量写入数据（使用别名）"""
        actions = [{
            "_op_type": "index",
            "_index": self.index_alias,  # 使用别名写入
            "_source": {
                "url": doc["url"],
                "content": doc["content"],
                "timestamp": datetime.now().isoformat(),
                "domain": urlparse(doc["url"]).netloc
            }
        } for doc in docs]

        try:
            success, failed = helpers.bulk(
                self.elastic,
                actions,
                stats_only=False,
                raise_on_error=False,
                request_timeout=60  # 增加超时
            )

            if failed:
                logging.error(f"批量写入失败数量: {len(failed)}")
                for err in failed[:3]:  # 打印前三个错误
                    logging.error(f"错误示例: {err}")
            return success, failed
        except Exception as e:
            logging.error(f"批量写入操作异常: {str(e)}")
            raise

    def search(self, query: Dict, index: str = None):
        """搜索接口（供其他模块调用）"""
        try:
            return self.elastic.search(
                index=index or self.index_alias,
                body=query
            )
        except Exception as e:
            logging.error(f"搜索失败: {str(e)}")
            raise


if __name__ == "__main__":
    try:
        es = ESClient()
        logging.info("=== 测试数据写入 ===")

        # 测试数据
        test_docs = [{
            "url": "http://example.com",
            "content":"Elasticsearch 全文搜索测试数据"
        }]


        success, failed = es.bulk_index(test_docs)
        if not failed:
            logging.info("数据写入成功！")
        else:
            logging.warning(f"部分写入失败: {len(failed)}")

        # 测试搜索
        logging.info("=== 测试搜索 ===")
        resp = es.search({
            "query": {
                "match": {"content": "搜索测试"}
            }
        })
        logging.info(f"搜索到 {resp['hits']['total']['value']} 条结果")

    except Exception as e:
        logging.error(f"测试失败: {str(e)}")
        import traceback

        traceback.print_exc()
