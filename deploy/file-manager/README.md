# OpenDAL 文件协议测试环境

本目录为 [issue #16](https://github.com/lizhian/dbx/issues/16) 提供最小化的本地协议服务和 Rust 连接测试。协议服务全部使用固定版本的公开镜像，不构建自定义镜像。

一个 Compose 项目覆盖五种产品连接类型和六个协议入口：

| 产品连接类型 | 协议实现 | 本地地址 |
| --- | --- | --- |
| FTP | FTP | `ftp://127.0.0.1:2121` |
| SFTP | SFTP | `ssh://127.0.0.1:2222` |
| S3 | S3（MinIO） | `http://127.0.0.1:9000` |
| WebDAV | WebDAV | `http://127.0.0.1:8080` |
| HDFS | WebHDFS | `http://127.0.0.1:9870` |
| HDFS | HDFS Native | `hdfs://127.0.0.1:19000` |

## 环境要求

- Docker 和 Docker Compose
- `ssh-keygen`
- Rust 工具链（运行 OpenDAL 测试时需要）

Hadoop 和 WebDAV 镜像仅提供 `linux/amd64` 版本。在 Apple Silicon 上由 Docker 模拟运行，首次启动会较慢。

## 启动服务

在仓库根目录执行：

```bash
cd deploy/file-manager
./setup.sh
docker compose up -d
docker compose ps
```

`setup.sh` 生成 SFTP 测试密钥：

- `runtime/sftp/id_ed25519`
- `runtime/sftp/id_ed25519.pub`

运行时密钥位于 `.gitignore` 中，不提交到仓库。长期运行的六个容器应显示为 `healthy`，一次性容器 `s3-init` 应以退出码 `0` 完成。

## 运行 Rust/OpenDAL 测试

测试程序是位于 `tests/` 的独立 Rust crate，固定使用 Apache OpenDAL `0.57.0`。它不会加入仓库根 workspace，也不会引入 DBX 产品代码。

```bash
cargo run --locked --manifest-path deploy/file-manager/tests/Cargo.toml
```

每个协议使用 `tests/fixtures/source.txt` 验证以下操作：

- `write`
- `read`，并核对文件内容
- `stat`，并核对文件类型和大小
- `list`，并核对目录内容
- `delete`，并确认文件已不存在
- 同连接 `copy`
- 同连接 `rename`

测试优先调用 OpenDAL 原生 `copy` 和 `rename`。后端返回 `Unsupported` 时，测试按 issue #16 的产品语义执行 OpenDAL 回退：

- `copy`：`read + write`
- `rename`：`copy + delete`；如果原生 `copy` 也不支持，则为 `read + write + delete`

测试会输出每个协议实际采用的原生或回退路径。任何协议失败时程序返回非零退出码，但会继续验证其他协议，便于一次看到完整结果。

## OpenDAL 连接参数

| 协议 | 参数 |
| --- | --- |
| FTP | endpoint `ftp://127.0.0.1:2121`，root `/ftp/dbx/`，用户名 `dbx`，密码 `dbx-password` |
| SFTP | endpoint `ssh://127.0.0.1:2222`，root `/config`，用户名 `dbx`，私钥 `runtime/sftp/id_ed25519`，known-hosts 策略 `Accept` |
| S3 | endpoint `http://127.0.0.1:9000`，region `us-east-1`，bucket `dbx`，root `/root/`，access key `dbx-access-key`，secret key `dbx-secret-key`，path-style |
| WebDAV | endpoint `http://127.0.0.1:8080`，root `/`，Basic 用户名 `dbx`，密码 `dbx-password` |
| WebHDFS | endpoint `http://127.0.0.1:9870`，root `/`，simple user `dbx` |
| HDFS Native | NameNode `hdfs://127.0.0.1:19000`，root `/`，Hadoop config directory `deploy/file-manager/config/hadoop/client`（包含 `dfs.client.use.datanode.hostname=true`） |

FTP 镜像默认不 chroot 用户，因此 OpenDAL 的连接 root 必须使用服务端真实目录 `/ftp/dbx/`。客户端测试路径仍然全部相对于该 root。

## 当前验证结果

六个协议均已通过 OpenDAL `0.57.0` 的连接和全部七项文件操作。

| 协议 | Copy | Rename |
| --- | --- | --- |
| FTP | `read + write` 回退 | `read + write + delete` 回退 |
| SFTP | 原生 | 原生 |
| S3 | 原生 | `copy + delete` 回退 |
| WebDAV | 原生 | 原生 |
| WebHDFS | `read + write` 回退 | `read + write + delete` 回退 |
| HDFS Native | `read + write` 回退 | 原生 |

不属于此最小本地环境的范围：

- Kerberos、HDFS HA
- 真实 AWS 和外部 WebDAV/Hadoop
- 桌面应用打包后的端到端测试
- 生产性能和故障注入

## 停止和重置

```bash
cd deploy/file-manager
docker compose down
```

环境不挂载协议数据卷。删除容器后，测试数据随容器一起清除；再次执行 `docker compose up -d` 即可得到干净环境。
