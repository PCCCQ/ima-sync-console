FROM python:3.11-slim

WORKDIR /app

# 仅标准库，无 pip 依赖
COPY ima_sync.py webapp.py index.html /app/

# 数据目录(挂载卷): SQLite 数据库与日志
VOLUME ["/app/data"]

ENV IMA_DB_DIR=/app/data
EXPOSE 8765

CMD ["python3", "-u", "webapp.py", "--port", "8765"]
