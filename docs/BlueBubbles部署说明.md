# BlueBubbles 部署说明

## 项目现状

当前项目已经精简成只做一件事：

- 调用本机 BlueBubbles Server 的 REST API 发送 iMessage
- 可选启动一个本地 HTTP 服务，对外暴露统一发送接口

已移除：

- 领星业务流程
- 发送风控配置

## 脚本用法

```bash
python main.py --recipient +8619358409763 --message "Hi from BlueBubbles"
```

如果不传 `--message`，会使用 `.env` 中的 `IMESSAGE_TEXT`。

也可以显式写成：

```bash
python main.py send --recipient +8619358409763 --message "Hi from BlueBubbles"
```

## HTTP 服务用法

启动本地服务：

```bash
python main.py serve
```

默认监听：

- `http://127.0.0.1:8787`

健康检查：

```bash
curl http://127.0.0.1:8787/health
```

单发：

```bash
curl -X POST http://127.0.0.1:8787/send \
  -H 'Content-Type: application/json' \
  -d '{"recipient":"+8619358409763","message":"Hi from BlueBubbles"}'
```

批量发送：

```bash
curl -X POST http://127.0.0.1:8787/send/batch \
  -H 'Content-Type: application/json' \
  -d '{"recipients":["+8619358409763","+8613800000000"],"message":"Hi from BlueBubbles"}'
```

## 环境变量

最少需要：

```env
BLUEBUBBLES_BASE_URL=http://127.0.0.1:1234
BLUEBUBBLES_PASSWORD=your_bluebubbles_server_password
IMESSAGE_TEXT=Hi
```

可选：

```env
BLUEBUBBLES_AUTH_PARAM_NAME=guid
BLUEBUBBLES_SEND_TIMEOUT_SECONDS=60
BLUEBUBBLES_READ_TIMEOUT_SECONDS=30
BLUEBUBBLES_RECENT_MESSAGES_LIMIT=25
BLUEBUBBLES_VERIFY_SSL=true
IMESSAGE_DELIVERY_CHECK_TIMEOUT_SECONDS=180
API_HOST=127.0.0.1
API_PORT=8787
```

## BlueBubbles Server 安装

官方来源：

- GitHub Releases: <https://github.com/BlueBubblesApp/bluebubbles-server/releases>
- REST API 文档: <https://docs.bluebubbles.app/server/developer-guides/rest-api-and-webhooks>

截至 2026-04-15，我核对到最新稳定版是 `v1.9.9`，Apple Silicon 安装包为：

- <https://github.com/BlueBubblesApp/bluebubbles-server/releases/download/v1.9.9/BlueBubbles-1.9.9-arm64.dmg>

## 首次启动后要做的事

1. 打开 BlueBubbles Server。
2. 在 macOS 系统设置里给它需要的权限。
3. 确认这台 Mac 的 Messages.app 已登录 Apple ID，且能手工发送 iMessage。
4. 在 BlueBubbles Server 中设置 server password。
5. 确认 REST API 已启用，并记下本地端口。

## 验证

接口连通后，运行：

```bash
python main.py --recipient +8619358409763 --message "Hi from BlueBubbles"
```

如果成功，脚本会输出：

- `confirmed_in_bluebubbles`

或：

- `sent_not_confirmed`

后者表示发送请求成功，但在确认窗口内没有再次从 recent messages 查询到匹配记录。

对于 HTTP 服务，成功时会返回：

- `ok: true`
- `results[].status`

目前推荐以 `confirmed_in_bluebubbles` 作为成功判定。
