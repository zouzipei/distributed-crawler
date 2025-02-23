from confluent_kafka import Producer, Consumer
import json
import hashlib
import time

# 新增时间模块用于测试超时控制

# Kafka 配置
#容器内运行使用 kafka:9092，宿主机运行使用 localhost:9092
KAFKA_BROKER = 'localhost:9092'  # 如果分发器在 Docker 内运行
# 或 # 如果分发器在宿主机运行
INPUT_TOPIC = "crawler_urls"        # 输入主题
OUTPUT_TOPIC_HASH = "crawler_tasks_hash"
OUTPUT_TOPIC_RR = "crawler_tasks_rr"

def hash_strategy(url, num_nodes=3):
    """哈希分发策略（改用SHA1优化）"""
    return int(hashlib.sha1(url.encode()).hexdigest(), 16) % num_nodes  # 替换为SHA1

class RoundRobinStrategy:
    """轮询分发策略"""
    def __init__(self):
        self.counter = 0
        self.nodes = 3  # 爬虫节点数量

    def get_node(self):
        self.counter = (self.counter + 1) % self.nodes
        return self.counter

def create_producer():
    # 优化生产者配置
    return Producer({
        "bootstrap.servers": KAFKA_BROKER,
        "linger.ms": 50,    # 批量等待时间
        "batch.size": 8192   # 批次大小
    })

def create_consumer():
    return Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": "url_dispatcher",
        "auto.offset.reset": "earliest"
    })

def distribute_urls():
    producer = create_producer()
    consumer = create_consumer()
    consumer.subscribe([INPUT_TOPIC])

    round_robin = RoundRobinStrategy()
    BATCH_SIZE = 50  # 批量处理阈值
    msg_count = 0  # 消息计数器

    try:
        while True:
            msg = consumer.poll(0.1)  # 缩短超时为0.1秒
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            url_task = json.loads(msg.value().decode())
            url = url_task["url"]

            # 哈希分发
            node_id_hash = hash_strategy(url)
            producer.produce(f"crawler_tasks_{node_id_hash}", json.dumps({"url": url}))

            # 轮询分发
            node_id_rr = round_robin.get_node()
            producer.produce(f"crawler_tasks_{node_id_rr}", json.dumps({"url": url}))
            msg_count +=1
            if msg_count >= BATCH_SIZE:  # 每处理50条批量提交
                producer.flush()
                msg_count = 0

    except KeyboardInterrupt:  # 修正异常名称（确保正确）
        print("Stopping URL dispatcher...")
    finally:
        consumer.close()
        producer.flush()  # 最终提交剩余消息



if __name__ == "__main__":
    distribute_urls()


