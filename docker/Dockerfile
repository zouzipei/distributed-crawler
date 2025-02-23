# 使用官方 Python 基础镜像
FROM python:3.10

# 设置工作目录
WORKDIR /app

# 复制项目文件到工作目录
COPY . .

# 安装依赖
RUN pip install --no-cache-dir -r requirements.txt

# 定义启动命令
CMD ["python", "seed_manager.py"]