import json
import time
from confluent_kafka import Producer, Consumer
from elasticsearch import Elasticsearch
import requests
from elasticsearch.exceptions import ConnectionError


# 配置信息
KAFKA_BROKER = "kafka:9092"
ES_HOST = "http://localhost:9200"
# 确保测试数据包含所有预期URL
TEST_URLS = [
    "http://example.com",
    "http://test.com/robots-disallowed",
    "http://example.com/duplicate"
]

# 生成测试文档时验证URL格式


def test_kafka_distribution():
    """测试Kafka消息分发"""
    producer = Producer({"bootstrap.servers": KAFKA_BROKER})

    # 发送测试URL
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
    try:
        while sum(received.values()) < len(TEST_URLS) * 2:  # 两种分发策略
            msg = consumer.poll(1.0)
            if msg is None: continue
            if msg.error(): continue

            # 解析主题编号
            topic_num = int(msg.topic().split("_")[-1])
            received[topic_num] += 1

        print("[Kafka分发测试结果]")
        print(f"节点0接收数: {received[0]}")
        print(f"节点1接收数: {received[1]}")
        print(f"节点2接收数: {received[2]}")
        assert sum(received.values()) == len(TEST_URLS) * 2
    finally:
        consumer.close()


def test_es_data_persistence():
    """测试数据存储到ES（增强版）"""
    # ================== 初始化连接 ==================
    es = Elasticsearch(
        hosts=[ES_HOST],
        request_timeout=30,
        max_retries=3
    )

    # ================== 连接重试机制 ==================
    connected = False
    for attempt in range(3):
        try:
            if es.ping():
                connected = True
                break
            print(f"ES连接失败，第{attempt + 1}次重试...")
            time.sleep(2 ** attempt)
        except Exception as e:
            print(f"连接异常: {str(e)}")
    if not connected:
        raise ConnectionError("无法连接到Elasticsearch")

    # ================== 索引管理 ==================
    index_name = "web_data"

    # 删除旧索引（防止残留数据干扰）
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)

    # 创建新索引（显式定义映射）
    mapping = {
        "mappings": {
            "properties": {
                "url": {"type": "keyword"},
                "content": {"type": "text"},
                "timestamp": {"type": "date"}
            }
        }
    }
    es.indices.create(index=index_name, body=mapping)
    print(f"√ 已创建索引 {index_name}")

    # ================== 数据验证增强 ==================
    try:
        # 写入测试数据
        test_doc = {
            "url": "http://test.com",
            "content": "测试内容",
            "timestamp": "2023-10-25T10:00:00"
        }
        es.index(index=index_name, body=test_doc, refresh=True)

        # 重试查询机制
        found = False
        for retry in range(5):
            result = es.search(
                index=index_name,
                body={"query": {"match_all": {}}}
            )
            if result['hits']['total']['value'] > 0:
                found = True
                break
            time.sleep(1)

        if not found:
            raise AssertionError("数据持久化验证失败")

        print("√ 数据验证成功")

    finally:
        # 测试后清理
        es.indices.delete(index=index_name)

if __name__ == "__main__":
    test_kafka_distribution()
    test_es_data_persistence()
