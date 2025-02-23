from aiokafka import AIOKafkaConsumer
from data_parser import parse_html
from es_client import ESClient
import logging
import json

async def process_data():
    es = ESClient()
    await es.create_index()

    consumer = AIOKafkaConsumer(...)
    await consumer.start()

    try:
        async for msg in consumer:
            try:  # 正确的异常处理嵌套
                logging.info("收到原始消息: %s", msg.value.decode()[:100])
                data = json.loads(msg.value)
                parsed = parse_html(data["content"])

                logging.info("解析结果 -> URL: %s, 内容长度: %d",
                            data["url"], len(parsed["text"]))

                logging.info("准备写入ES，文档数: 1")
                success, failed = await es.bulk_index([{
                    "url": data["url"],
                    "content": parsed["text"]
                }])

                if failed:
                    logging.error("写入失败详情: %s", failed.get("error", "未知错误"))
                else:
                    logging.info("写入成功")

            except json.JSONDecodeError as e:
                logging.error("JSON解析失败: %s", str(e))
            except Exception as e:
                logging.error("处理消息异常: %s", str(e))

    finally:
        await consumer.stop()