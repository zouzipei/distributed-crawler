import socket
import asyncio
from concurrent.futures import ThreadPoolExecutor

class AsyncDNSResolver:
    # 定义一个名为 AsyncDNSResolver 的类，该类的主要功能是实现异步的 DNS 解析。

    def __init__(self, max_workers=4):
        # 类的构造函数，当创建该类的实例时会自动调用。
        # 接收一个可选参数 max_workers，默认值为 4，表示线程池中的最大线程数。
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        # 创建一个 ThreadPoolExecutor 实例，并将其赋值给实例属性 executor。
        # 这个线程池将用于执行 DNS 解析任务，以避免阻塞异步事件循环。

    async def resolve(self, url):
        # 定义一个异步方法 resolve，用于异步地解析给定 URL 的域名。
        # 异步方法使用 async 关键字定义，意味着它可以在执行过程中暂停和恢复。
        """异步解析域名"""
        loop = asyncio.get_event_loop()
        # 获取当前的异步事件循环对象。事件循环是 asyncio 的核心，
        # 它负责调度和执行异步任务。
        # 关键行：这行代码为后续将任务提交到事件循环中执行做准备。

        domain = url.split("//")[-1].split("/")[0]
        # 从传入的 URL 中提取出域名部分。
        # 首先使用 split("//") 将 URL 按 "//" 分割，取最后一部分，
        # 然后再按 "/" 分割，取第一部分，得到域名。

        try:
            return await loop.run_in_executor(
                self.executor,
                socket.gethostbyname,
                domain
            )
            # 使用事件循环的 run_in_executor 方法将 DNS 解析任务提交到线程池中执行。
            # self.executor 是之前创建的线程池实例。
            # socket.gethostbyname 是 socket 模块中用于将域名解析为 IP 地址的函数。
            # domain 是要解析的域名。
            # await 关键字用于等待线程池中的任务完成，并返回解析结果。
            # 关键行：这行代码实现了将阻塞的 DNS 解析操作放到线程池中异步执行。

        except socket.gaierror:
            return None
        # 捕获可能的 socket.gaierror 异常，当域名解析失败时会抛出该异常。
        # 如果发生异常，返回 None 表示解析失败。

async def main():
    # 定义一个异步函数 main，作为程序的入口点。
    resolver = AsyncDNSResolver()
    # 创建 AsyncDNSResolver 类的一个实例。

    print(await resolver.resolve("https://example.com"))
    # 调用 resolver 实例的 resolve 方法，异步地解析 "https://example.com" 的域名。
    # 使用 await 等待解析结果，并将结果打印输出。
    # 关键行：这行代码触发了整个异步 DNS 解析流程。

if __name__ == "__main__":
    # 这是 Python 脚本的常见入口判断语句，确保代码作为脚本直接运行时才会执行下面的代码。
    asyncio.run(main())
    # 使用 asyncio.run 函数来运行异步函数 main。
    # 它会自动创建一个事件循环，启动异步任务，并在任务完成后关闭事件循环。
    # 关键行：这行代码启动了整个异步程序。