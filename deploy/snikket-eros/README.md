# snikket-eros 部署配置

beijing-01 上 eros-engine 的实际部署配置。**代码**在本仓库，**配置**原来只存在于服务器，
这个目录把它收进来，避免「改了机器就丢了」。

当前线上：`c1921/eros-engine:59a65db`（= 提交 `59a65db`，含多气泡提示词强化）。

## 目录内容

| 文件 | 说明 |
| --- | --- |
| `compose.yml` | postgres + engine 两个服务；engine 用 host 网络，监听 `127.0.0.1:18080` |
| `model.toml` | 挂载为 `/etc/eros-engine/snikket-model.toml`：模型、温度、任务开关 |
| `engine.env.example` | 环境变量模板；真实 `engine.env` 含密钥，不入库 |
| `personas/` | Aimee / Maia / snikket 三个人设，挂载为 `/etc/eros-engine/snikket-personas` |
| `deploy.sh` | 在服务器上切镜像并重启 engine |

## 服务器上的目录布局

```
/etc/snikket-eros/
├── compose.yml            ← 本目录同名文件
├── model.toml             ← 本目录同名文件
├── personas/              ← 本目录 personas/
├── engine.env             ← 本目录 engine.env.example + 真实密钥
├── db.env                 ← POSTGRES_USER / POSTGRES_DB / POSTGRES_PASSWORD
├── prompt-logs/           ← PROMPT_LOG_DIR 的输出（可选）
├── jwt_secret             ← Eros JWT 签名密钥（与 aimee-eros / snikket-omemo 共用）
├── caller_secret          ← caller 鉴权密钥（同上）
├── database_password      ← 数据库口令（与 db.env 对应）
└── active-genome          ← 当前使用的人设 genome UUID
```

`jwt_secret`、`caller_secret`、`database_password`、`db.env`、`engine.env`、`prompt-logs/`
都**不进 git**。

## 构建镜像

服务器没有 Rust 工具链、磁盘也紧张，所以在开发机构建再传：

```bash
docker build -f docker/Dockerfile -t c1921/eros-engine:<sha> .
docker save -o eros-engine-<sha>.tar c1921/eros-engine:<sha>
scp eros-engine-<sha>.tar beijing-01:/tmp/
ssh beijing-01 'docker load -i /tmp/eros-engine-<sha>.tar'
```

镜像不大（约 186MB，tar 约 45MB）。`docker/Dockerfile` 是两阶段构建，依赖层有缓存时
整个 release 编译约 1m40s。

## 上线

```bash
scp deploy/snikket-eros/deploy.sh beijing-01:/tmp/
ssh beijing-01 'sudo cp /tmp/deploy.sh /etc/snikket-eros/ && sudo /etc/snikket-eros/deploy.sh <sha>'
```

`deploy.sh` 会备份旧 `compose.yml`、改镜像标签、`up -d --no-deps engine`。

## 多气泡分段（`CHAT_REPLY_PACING_DISABLED`）

- `false` = 启用。引擎在 system 消息末尾追加 `REPLY_PROMPT`
  （`crates/eros-engine-server/src/pipeline/reply_pacing.rs`），要求模型在自然停顿处
  用空行分成 2–3 条、且气泡末尾不加句号；然后按空行拆、按字数算节奏逐条落库与发送。
- `true` = 关闭，退回单条整段回复。
- 拆分上限是 `MAX_REPLY_BUBBLES = 3`（引擎侧）。客户端（bridge / 微信插件）虽然允许 5 条，
  但永远触发不到。

## 提示词日志（`PROMPT_LOG_DIR`）

设成 `/var/log/eros-prompts`（容器内路径，compose 挂载 `./prompt-logs`）后，每次聊天都会
把组装好的完整 prompt 写成文本文件，用于核对提示词是否真的到达模型。

```bash
# 核对最新一次请求里的多气泡提示
grep -o '除非只有一句话.*' /etc/snikket-eros/prompt-logs/$(ls -t /etc/snikket-eros/prompt-logs | head -1)
```

只用于排障：默认注释掉，需要时临时打开，用完记得清空目录。
