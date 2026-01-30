# kel 部署与开发环境

## xxl-job 调度中心

用于 kel 开发测试的 xxl-job 调度中心（MySQL + Admin），按官方 [5.24 Docker Compose 快速部署](https://www.xuxueli.com/xxl-job/#5.24%20Docker%20Compose%20%E5%BF%AB%E9%80%9F%E9%83%A8%E7%BD%B2) 放置于 `xxl-job/`。MySQL 在 Docker 容器内运行，无需本机安装 MySQL。

### 启动

```bash
cd xxl-job
docker compose up -d
```

未设置 `MYSQL_PATH` 时，MySQL 数据持久化到当前目录下 `mysql-data/`（conf、logs、data）。首次启动会自动执行 `doc/db/tables_xxl_job.sql` 初始化库表。

### 停止

```bash
cd xxl-job
docker compose down
```

清空数据：`docker compose down -v`。

### 访问

- 控制台：<http://localhost:8080/xxl-job-admin>
- 默认账号：`admin` / `123456`

### 连接容器内 MySQL（可选）

本机执行 `mysql` 会报 `Can't connect to local MySQL server through socket`，是因为本机未跑 MySQL 服务。容器内 MySQL 已映射到本机 3306：

```bash
mysql -h 127.0.0.1 -P 3306 -uroot -p
# 密码与 compose 中 MYSQL_ROOT_PASSWORD 一致，默认 root_pwd
```

或进容器：`docker exec -it xxl-job-mysql mysql -uroot -p`。

### 说明

- 当前 `docker-compose.yml` 已改为使用 Docker Hub 镜像 `xuxueli/xxl-job-admin:2.4.2`，无需 Maven 构建。
- 示例执行器已注释，仅启动 MySQL + xxl-job-admin，避免占用 9999 端口；后续 kel 集成 xxl-job-executor 后作为执行器接入即可。

### 拉取镜像超时

若 `docker compose up -d` 拉取镜像报 `context deadline exceeded`，多为无法直连 Docker Hub。需让 **Docker 守护进程**走代理（与终端里 `sshwall` 仅影响当前 shell 不同）：

- **Docker Desktop（Mac/Windows）**：Settings → Resources → Proxies，填写 `http://127.0.0.1:8001`（或你的代理地址），Apply 后重试。
- **Linux 使用 systemd**：为 dockerd 配置 `HTTP_PROXY`/`HTTPS_PROXY` 后重启 Docker 服务。
- 或使用可用的 Docker Hub 镜像加速源，在 Docker 配置中设置 `registry-mirrors`。
