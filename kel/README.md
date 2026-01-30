# kel

Kingbase 数据抽取/加载多模块项目。

## xxl-job 集成概览

开发测试用 xxl-job 调度中心已通过 Docker Compose 部署在 `deploy/xxl-job/`，Kel 作为执行器接入 xxl-job 调度中心。

### 调度中心（Admin）

Admin 启动后：

- 在浏览器打开 <http://localhost:8080/xxl-job-admin>
- 用账号 **admin**、密码 **123456** 登录即可使用调度中心（密码请用英文输入、勿带空格；若提示账号或密码错误，可尝试无痕模式或清除该站 Cookie 后重试）

启动与停止命令、连接容器内 MySQL 等详见 [deploy/README.md](deploy/README.md)。

### Kel 执行器（Executor）

- 依赖：`kel-start` 已引入 `com.xuxueli:xxl-job-core:2.4.2`，配置类为 `org.csits.kel.config.XxlJobConfig`。
- 核心配置（`kel-start/src/main/resources/application.yml`）：

  ```yaml
  xxl:
    job:
      accessToken: default_token
      admin:
        addresses: http://localhost:8080/xxl-job-admin
      executor:
        appname: kel-executor
        port: 9999
        logpath: logs/xxl-job/jobhandler
        logretentiondays: 30
  ```

- 启动 Kel 执行器（常驻进程）：

  ```bash
  cd kel/kel-start
  java -jar target/kel-start-0.0.1-SNAPSHOT.jar
  ```

  启动后：

  - Kel 本身 Web 端口：`http://localhost:8081/`
  - xxl-job 执行器端口：`9999`
  - Admin 的“执行器管理”中会看到 `AppName=kel-executor` 的执行器在线。

### Kel 任务类型与 xxl-job 任务配置

Kel 目前有四种作业类型（`JobConfig.job.type`，见 `JobType` 枚举）：

| 类型枚举            | 含义                 | Kel 内部调用 |
|---------------------|----------------------|-------------|
| `EXTRACT_KINGBASE`  | 金仓数据库卸载       | `executeExtract` |
| `FILE_EXTRACT`      | 文件/目录采集       | `executeExtract` |
| `KINGBASE_LOAD`     | 金仓数据库加载       | `executeLoad`    |
| `FILE_LOAD`         | 文件加载入库         | `executeLoad`    |

在 xxl-job 中统一使用 `JobHandler = kelJobHandler`，通过 **执行参数** 区分卸载/加载及作业编码：

- `extract:<jobCode>`：执行对应作业的卸载/采集流程（`executeExtract`）
- `load:<jobCode>`：执行对应作业的加载流程（`executeLoad`）
- `<jobCode>`：等价于 `extract:<jobCode>`（默认卸载）

### 卸载与加载的部署关系

卸载侧与加载侧视为**两个独立网络**：不共享配置、不共享数据目录。因此：

- **无 `source_extract_job` 配置**：加载作业不会从“源卸载作业”自动取批次或路径，相关配置与逻辑已移除。
- **加载任务批次号**：不填时，根据**本作业配置**的 `input_directory` 在本机下扫描，取最新批次目录（目录名格式 `yyyyMMdd_NNN`）；填写则加载指定批次。
- **加载任务输入路径**：`input_directory` 在作业配置中必填（包所在根目录），实际路径 = `input_directory + "/" + 批次号`。生产环境填加载侧路径；单机联调可与卸载侧目录名一致（如 `exchange/bss_file_extract`），但仍是本侧配置，与卸载侧无共享。

示例：demo 作业（`conf/dev/jobs/demo_extract.yaml`，`job.type=EXTRACT_KINGBASE`）：

- 卸载 demo（EXTRACT_KINGBASE）：执行器选 `kel-executor`，`JobHandler=kelJobHandler`，执行参数 `extract:demo`
- 若后续新增 demo 加载作业（`demo_load.yaml`，`job.type=KINGBASE_LOAD`），则加载任务执行参数为 `load:demo`

> 提示：新增其他 `jobCode` 时，仅需增加对应 YAML 配置并在 xxl-job 中新增任务，参数按上述规则填写即可。 Kel 会根据 YAML 中的 `job.type` 自动选择对应插件与流程。

