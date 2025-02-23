import json
import time
import logging
from confluent_kafka import Producer, Consumer
from elasticsearch import Elasticsearch
from es_client import ESClient

# 配置信息
KAFKA_BROKER = "kafka:9092"
ES_HOST = "http://localhost:9200"
TEST_URLS = [
    "http://test.com",
    "http://example.com",
    "http://demo.org"
]

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)


def test_kafka_distribution():
    """测试Kafka消息分发"""
    producer = Producer({"bootstrap.servers": KAFKA_BROKER})

    # 发送测试数据
    for url in TEST_URLS:
        producer.produce("crawler_urls", json.dumps({"url": url}))
    producer.flush()

    # 验证消息分发
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": "test-group",
        "auto.offset.reset": "earliest"
    })
    consumer.subscribe(["crawler_tasks_0", "crawler_tasks_1", "crawler_tasks_2"])

    received = {0: 0, 1: 0, 2: 0}
    start_time = time.time()

    try:
        while time.time() - start_time < 10:
            msg = consumer.poll(1.0)
            if not msg:
                continue
            if msg.error():
                logger.error(f"消费错误: {msg.error()}")
                continue

            try:
                topic_num = int(msg.topic().split('_')[-1])
                received[topic_num] += 1
            except ValueError:
                logger.warning(f"无法解析主题: {msg.topic()}")

        logger.info(f"Kafka分发结果: {received}")
        assert sum(received.values()) == len(TEST_URLS) * 2
    finally:
        consumer.close()


def test_es_data_persistence():
    """测试ES数据持久化（最终修复版）"""
    es = Elasticsearch([ES_HOST])

    # 清理所有测试相关数据
    try:
        # 删除所有以web_data开头的索引
        if es.indices.exists(index="web_data*"):
            es.indices.delete(index="web_data*")
            logger.info("已清理所有测试索引")

        # 安全删除别名（如果存在）
        if es.indices.exists_alias(name="web_data"):
            es.indices.delete_alias(index='*', name="web_data")
    except Exception as e:
        logger.warning(f"清理阶段出现可忽略错误: {str(e)}")

    # 初始化客户端（会自动创建新索引）
    es_client = ESClient()

    # 写入测试数据
    test_docs = [{
        "url": "http://test.com",
        "content": "测试内容"
    }]
    success_count, _ = es_client.bulk_index(test_docs)
    assert success_count == len(test_docs)

    # 验证数据
    result = es_client.search({
        "term": {
            "domain": "test.com"
        }
    })
    logger.info(f"搜索结果: {result['hits']['total']['value']}")
    assert result['hits']['total']['value'] >= 1


if __name__ == "__main__":
    try:
        test_kafka_distribution()
        time.sleep(2)
        test_es_data_persistence()
    except Exception as e:
        logger.error(f"测试失败: {str(e)}")
        raise
