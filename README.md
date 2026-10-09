# ima 同步控制台

把本地文件夹（NAS / 电脑）定时同步到腾讯 [ima 知识库](https://ima.qq.com) 的轻量 Web 控制台。

- **多任务**：每个文件夹一个任务，各自设置每日同步时间、目标知识库/文件夹、扩展名过滤、启停
- **全类型**：PDF / Word / PPT / Excel / Markdown / 图片 / TXT / Xmind / 录音(mp3 m4a wav aac) / HTML / EPUB
- **增量 + 内容感知**：新文件自动上传；文件内容变化（sha256）自动重传新版本；仅时间戳变化不重传
- **可视化配置**：浏览器里点选文件夹（服务端目录浏览）、下拉选知识库（OpenAPI 自动拉取）
- **限流试传**：「试传 10 个」真实上传每个任务的前 10 个待传文件，小批量验证后再全量
- **记录可查**：SQLite 持久保存任务历史与文件级记录，支持关键词搜索 / 状态筛选 / 分页
- **零依赖**：后端纯 Python 标准库（含内置每日定时调度器），前端单文件 HTML

## 快速开始

```bash
python3 webapp.py --port 8765
# 浏览器打开 http://<host-ip>:8765
```

1. 「任务与设置」填入 Client ID / API Key（在 [ima.qq.com/agent-interface](https://ima.qq.com/agent-interface) 微信扫码创建）→「测试连接」
2. 「＋ 新建任务」→ 点「📁 浏览」选文件夹 → 拉取并选择知识库 → 设定每日同步时间 → 保存
3. 到点自动同步；也可「立即同步」全量上传，或「试传 10 个」先小批量验证

## Docker 部署（NAS 推荐）

```bash
docker build -t ima-sync-console .
docker run -d --name ima-sync --restart=always \
  -p 8799:8765 \
  -e TZ=Asia/Shanghai \
  -v /etc/localtime:/etc/localtime:ro \
  -v /vol1/1000/ima-sync/data:/app/data \
  -v /vol1:/vol1:ro \
  ima-sync-console
```

> `-e TZ` 与 `/etc/localtime` 必须挂载，否则定时同步按 UTC 触发（差 8 小时）。

## 命令行模式（无需常驻服务）

```bash
python3 ima_sync.py --dir /vol1/我的录音 --kb <知识库ID> \
  --client-id <CID> --api-key <KEY>            # 全部支持类型
python3 ima_sync.py --dir /vol1/文档 --kb <KB> --ext pdf,docx ...
```

配合 crontab / fnOS 计划任务做定时；增量状态存各文件夹内 `.ima_sync_state.json`。

## 详细文档

见 [部署说明.md](部署说明.md)：原理、Docker/计划任务两种部署、常见问题排查。

## 安全说明

- 凭据保存在本机（SQLite / 状态文件），仓库不含任何真实凭据
- Web 服务监听 0.0.0.0，建议在「任务与设置」页设置页面访问口令
- ima OpenAPI 凭据可在 agent-interface 页面随时轮换

## License

MIT
