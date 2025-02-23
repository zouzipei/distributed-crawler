from confluent_kafka import Consumer
import json
from url_dispatcher_kafka import create_producer, INPUT_TOPIC
from url_dispatcher_kafka import KAFKA_BROKER
import time
from url_dispatcher_kafka import OUTPUT_TOPIC_HASH, OUTPUT_TOPIC_RR
# 新增时间模块用于测试超时控制

def test_distribute_urls():
    """优化后的测试函数（含超时控制）"""
    test_producer = create_producer()
    test_url = "https://juejin.cn/"  # 修正空格问题
    print(f"发送测试URL: {test_url}")  # 添加发送日志

    # 使用回调确认发送状态
    def delivery_report(err, msg):
        if err is not None:
            print(f'消息发送失败: {err}')
        else:
            print(f'消息发送成功: {msg.topic()} [{msg.partition()}]')

    test_producer.produce(
        INPUT_TOPIC,
        json.dumps({"url": test_url}),
        callback=delivery_report  # 添加回调
    )
    test_producer.flush()

    # 启动消费者
    test_consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": f"test_dispatcher_{time.time()}",  # 唯一消费者组
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })


    test_consumer.subscribe([OUTPUT_TOPIC_HASH, OUTPUT_TOPIC_RR])

    max_wait_time = 10  # 增加测试超时阈值（秒）
    start_time = time.time()
    received = False

    try:
        while time.time() - start_time < max_wait_time:
            msg = test_consumer.poll(0.1)
            if msg is None:
                continue

            data = json.loads(msg.value().decode())
            if data["url"] == test_url:
                received = True
                break  # 收到消息后立即退出

    finally:
        test_consumer.close()
        assert received, f"在 {max_wait_time} 秒内未接收到分发结果: {INPUT_TOPIC}→{test_url} 未被正常分发"


def test_distribute_urls():
    """优化后的测试函数"""
    # 发送测试数据
