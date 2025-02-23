import multiprocessing
import asyncio
import aiohttp
import json
import random
import time
import signal
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from urllib.parse import urlparse
from robots_parser import RobotsCache
import logging

logging.basicConfig(
    format="[%(asctime)s] %(levelname)s: %(message)s",
    level=logging.INFO
)


class CrawlerWorker(multiprocessing.Process):
    def __init__(self, node_id):
        super().__init__()
        self.node_id = node_id
        self.proxy_pool = ["http://proxy1:8080", "http://proxy2:8080"]
        self.domain_delays = {}  # 域名: (最后访问时间, 爬取间隔)
        self._shutdown_flag = asyncio.Event()

    async def fetch(self, session: aiohttp.ClientSession, url: str) -> str:
        """遵守robots协议执行抓取"""
        # ...保持原有fetch方法不变...
    async def run_async(self):
        """异步运行主逻辑（增强版）"""
        consumer = None
        producer = None
        try:
            async with RobotsCache(), \
                    aiohttp.ClientSession() as session:

                # 初始化Kafka客户端
                consumer = AIOKafkaConsumer(
                    f"crawler_tasks_{self.node_id}",
                    bootstrap_servers="kafka:9092",  # 容器内使用kafka:9092
                    group_id=f"crawler_group_{self.node_id}",
                    enable_auto_commit=False,
                    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                    # 新增重试配置
                    retry_backoff_ms=1000,
                    max_poll_interval_ms=300000,
                    session_timeout_ms=10000,
                    heartbeat_interval_ms=3000,
                    # 关键配置：禁用元数据刷新
                    metadata_max_age_ms=30000
                )

                producer = AIOKafkaProducer(
                    bootstrap_servers="kafka:9092",
                    compression_type="gzip"
                )

                await consumer.start()
                await producer.start()
                logging.info(f"Worker {self.node_id} 启动完成")

                # 消费循环
                commit_counter = 0
                while not self._shutdown_flag.is_set():
                    try:
                        msg = await asyncio.wait_for(consumer.getone(), timeout=1.0)
                        url = msg.value['url']

                        content = await self.fetch(session, url)
                        if content:
                            await producer.send(
                                "parsed_data",
                                json.dumps({"url": url, "content": content[:500]}).encode()
                            )
                            commit_counter += 1

                            # 批量提交
                            if commit_counter >= 50:
                                await consumer.commit()
                                commit_counter = 0

                    except asyncio.TimeoutError:
                        continue
                    except asyncio.CancelledError:
                        logging.info("收到关闭信号，终止消费循环")
                        break
                    except Exception as e:
                        logging.error(f"消息处理失败: {str(e)}")

                # 最终提交
                if commit_counter > 0:
                    await consumer.commit()

        except Exception as e:
            logging.error(f"运行时异常: {str(e)}")
        finally:
            # 确保资源关闭
            logging.info(f"Worker {self.node_id} 正在关闭...")
            if consumer:
                await consumer.stop()
            if producer:
                await producer.stop()
            logging.info(f"Worker {self.node_id} 已安全关闭")


    def run(self):
        """进程入口点（增强事件循环管理）"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        def shutdown_handler(signum, frame):
            logging.info(f"Worker {self.node_id} 收到关闭信号")
            self._shutdown_flag.set()
            for task in asyncio.all_tasks(loop):
                task.cancel()

        signal.signal(signal.SIGTERM, shutdown_handler)

        try:
            loop.run_until_complete(self.run_async())
        except KeyboardInterrupt:
            logging.info("接收到键盘中断，启动优雅关闭")
        except Exception as e:
            logging.error(f"进程异常: {str(e)}")
        finally:
            # 清理事件循环
            pending = asyncio.all_tasks(loop=loop)
            for task in pending:
                task.cancel()
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.close()
            logging.info(f"Worker {self.node_id} 事件循环已关闭")


if __name__ == "__main__":
    processes = []
    try:
        # 启动工作进程
        for i in range(3):
            p = CrawlerWorker(i)
            p.start()
            processes.append(p)
            logging.info(f"启动爬虫节点 #{i} (PID: {p.pid})")

        # 监控进程状态
        while True:
            time.sleep(1)
            for p in processes[:]:  # 创建副本遍历
                if not p.is_alive():
                    logging.warning(f"进程 {p.pid} 异常退出，重新启动...")
                    processes.remove(p)
                    new_p = CrawlerWorker(p.node_id)
                    new_p.start()
                    processes.append(new_p)
                    logging.info(f"新进程 PID: {new_p.pid} 已启动")

    except KeyboardInterrupt:
        logging.info("\n正在终止所有工作进程...")
        for p in processes:
            if p.is_alive():
                p.terminate()
            p.join(timeout=5)
            if p.exitcode is None:
                logging.warning(f"进程 {p.pid} 未正常退出，强制终止")
                p.kill()
        logging.info("系统已安全关闭")
