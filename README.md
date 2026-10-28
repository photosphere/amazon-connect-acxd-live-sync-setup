# ACXD 设备故障自助报修 + Agent Workspace 弹屏

一套 Python 部署脚本：用 Amazon Connect **Agentic CX Designer（ACXD）** 做自助服务，依次收集客户的 **设备名称（DeviceName）**、**设备型号（DeviceModel）** 和 **故障描述（FaultDescription）**，转人工时在 agent workspace 弹出 Detail 视图显示这三项；入站流还会按来电号码查询 **Customer Profiles**，弹屏在设备三项之前显示客户的 **姓名**、**来电号码** 和 **邮箱**（见“客户资料弹屏”）。弹屏的实现方式参考了 [photosphere/connect-cdk-voice-channel](https://github.com/photosphere/connect-cdk-voice-channel/blob/main/examples/flows/ivr_survey_screenpop_flow.json) 中的 `ivr_survey_screenpop_flow.json` 与 `screenpop_message_flow.json`。

> ⚠️ 本项目尚未在真实环境端到端部署验证（已完成只读探测与 `--preflight`，见“真实环境验证结果（只读探测）”）。Agentic CX 块的 flow-language JSON 没有公开文档，但其结构已从实网 Connect 流读出（`ConnectParticipantWithAgenticCX`，假设 A1），脚本默认按 workspaceId / applicationId / deploymentAlias 直接生成该块，不再需要控制台参考流（参考流只作为可选覆盖）。

## 快速开始（只需两个输入）

只需提供 **Connect 实例 ARN** 和 **ACXD API key**，一条命令即可完成 ACXD 上下文变量、ACXD flow、应用、build、部署，以及 Connect screenpop 流和入站流的创建与配置。其他输入都会自动推导或使用默认值（见“自动推导规则”）。

```bash
cd <PROJECT_DIR>
python3 -m pip install -r requirements.txt

# 方式 1：环境变量（推荐）
export ACXD_API_KEY='<ACXD API key>'
python3 deploy.py --instance-arn arn:aws:connect:<REGION>:<ACCOUNT_ID>:instance/<INSTANCE_ID>

# 方式 2：key 文件（文件只含 key 一行，建议 chmod 600）
python3 deploy.py --instance-arn <实例ARN> --acxd-api-key-file /path/to/acxd_api_key

# 方式 3：不提供 key，在交互终端中运行，脚本会用 getpass 提示输入（不回显）
python3 deploy.py --instance-arn <实例ARN>
```

建议先用同样的两个输入做预演和只读预检：

```bash
# 预演：不发起任何网络调用，工作区和队列使用占位值，产物写入 output/dry-run/
python3 deploy.py --dry-run --instance-arn <实例ARN>
python3 tools/check_flows.py output/dry-run

# 只读预检：只调用 GET / List / Describe，列出真实运行将执行的写操作，产物写入 output/preflight/
export ACXD_API_KEY='<ACXD API key>'
python3 deploy.py --preflight --instance-arn <实例ARN>
```

### 方式 4：交互模式

在终端中直接运行 `python3 deploy.py`（可以不带参数，也可以只给一部分），脚本会逐项询问缺少的参数，直接回车接受 `[ ]` 中的默认值：

```bash
python3 deploy.py              # 真实运行：询问缺少的参数，写入前显示摘要并确认
python3 deploy.py --dry-run    # 预演：只询问实例 ARN（自由输入）、部署环境、语言和资源名前缀，不联网
```

- 优先级：命令行参数 > 环境变量 > 交互提示 > 默认值 / 自动推导。只询问前两者都没有给出的值；已给出的值（即使格式错误）不会再问，仍按原样校验。
- 会询问的参数：Connect 实例（connect / all 阶段）、区域（还确定不了时）、目标队列（connect / all，非 dry-run）、ACXD 部署环境、对话语言（acxd / all）、资源名前缀（没有 `--name-prefix` 也没有环境变量 `NAME_PREFIX` 时）、ACXD API key（getpass，不回显）。资源名前缀的提示为 `资源名前缀 [AgenticCare]: `，上方先列出默认前缀派生的 5 个名称（ACXD flowId、Generative Journey 节点、ACXD 应用名、两个 Connect 流），直接回车接受 `AgenticCare`；输入的前缀按 `--name-prefix` 的同一规则校验，不合法时重新询问。实例、队列、区域、部署环境、语言使用编号列表：实例来自只读 `ListInstances`（只列 ACTIVE），队列来自只读 `ListQueues`（STANDARD，默认 `BasicQueue`）；列表调用失败时改为自由输入，也可以直接粘贴 ARN / 区域名。ACXD 工作区仍然自动发现，只有原本会报错的情况（多个工作区且 `deploy_state.json` 中没有记录，或 ListWorkspaces 返回 401/403）才改为让你选择或输入。build 超时、轮询间隔、输出目录、Agentic CX 覆盖等高级参数不询问，沿用默认值或命令行参数。
- 输入无效时提示原因并重新询问（连续 5 次无效时退出 2）；Ctrl-C / EOF 立即退出，退出码 130，不执行任何写操作。
- 每次运行（交互、`--non-interactive`、dry-run、`--preflight`、`--yes`）在参数校验后都会打印一次实际使用的前缀及其来源和派生名称，例如 `资源名前缀: AgenticCare（默认值）`，来源为 `命令行 --name-prefix` / `环境变量 NAME_PREFIX` / `交互输入` / `默认值`。只列出本次阶段涉及的名称。`--preflight` 的输出和交互式真实运行的确认摘要还会标出每个名称是 `将更新（覆盖）`（同名资源已存在）还是 `将新建`。这一判断只用只读的 ListFlows / ListApplications / ListContactFlows，某个列表调用失败时只省略对应标记（WARN），不影响运行；dry-run 不联网，所以不显示标记。
- 真实运行在第一次写 ACXD / Connect 之前打印参数摘要（API key 只显示掩码）并询问 `确认执行真实部署？[y/N]`，回答 `y` 才继续，否则退出 1；dry-run 和 `--preflight` 不需要确认。**注意：** 在交互终端中，即使参数都已给全，真实运行也会要求确认；加 `--yes`（`-y`）跳过确认，或加 `--non-interactive` 恢复原来的完全非交互行为。
- 只有 stdin 和 stdout 都是终端时才进入交互模式。CI、管道、重定向（例如 `| tee log`）以及 `--non-interactive` 都不询问、不确认，缺少必需参数时直接以退出码 2 报错，与以前相同。唯一例外是 API key 的 getpass 提示，它沿用原规则（stdin 是终端即可，例如 `| tee log` 时仍会提示）；`--non-interactive` 时它也不提示。

ACXD API key 的来源优先级：环境变量 `ACXD_API_KEY` > `--acxd-api-key-file <文件>` > `--acxd-api-key <key>` > 交互提示。`--acxd-api-key` 会让 key 进入 shell 历史和进程列表，使用时脚本会打印警告，不推荐。同时提供了多个来源时，只使用优先级最高的一个，并在日志中列出被忽略的来源名称（不含值）。

### 自动推导规则

| 项 | 默认来源 | 覆盖方式 |
|---|---|---|
| 区域 | `--instance-arn` 中的区域。`AWS_REGION` 与 ARN 不一致时以 ARN 为准，只打 INFO | `--region`（与 ARN 不一致时报错，退出 2） |
| 实例 ID / 账号 | `--instance-arn` | `--instance-id`（与 ARN 不一致时报错，退出 2） |
| ACXD endpoint | `https://api.acxd.connect.<区域>.amazonaws.com`（区域取自 ARN，假设 A2） | `--acxd-region` / `--acxd-endpoint` |
| ACXD 工作区 | 只读调用 ACXD `ListWorkspaces`（`GET /sdk/workspaces`，账户级操作，不带 `x-workspace-id`；SDK 与实网证据见 `.agents/tasks/two-input-evidence/acxd_list_workspaces.json`）。只有 1 个工作区时直接使用；有多个时使用 `deploy_state.json` 中记录的那个，否则退出 2 并列出全部工作区；0 个或 401/403 时退出 2 并提示改用 `--acxd-workspace-id`。不会猜测工作区 | `--acxd-workspace-id` / `ACXD_WORKSPACE_ID`（给出后不再调用 ListWorkspaces） |
| 队列 | `BasicQueue`；实例中没有时，取按名称排序后的第一个 STANDARD 队列（打 WARN）；一个 STANDARD 队列都没有时退出 2 | `--queue-arn` 或 `--queue-name`（最多提供一个） |
| ACXD 应用 | 新建（重跑时按名称更新）专用应用 `AgenticCareSelfService`，不会修改 workshop1014 等已有应用 | `--reuse-acxd-application --acxd-application-id <ID>` |
| 部署环境 / deploymentAlias | `production`；deploymentAlias 取自本次创建 / 更新的部署（GetApplicationDeployment），自动写入入站流的 `ConnectParticipantWithAgenticCX` 块 | `--acxd-environment` |
| 语言 | `zh-CN`；未显式指定 `--language` 时，如果 ACXD 以语言原因拒绝（假设 A21），会自动改用 `en-US` 重新执行 flow / 应用 / build，Connect 流也随之使用 en-US 提示语和声音 | `--language zh-CN` / `--language en-US`（显式指定时不回退） |
| TTS 声音 | 按最终语言：zh-CN `Zhiyu`、en-US `Joanna`，引擎 `Neural` | `--tts-voice` / `--tts-engine` |

### 顺序保证

真实运行（`--phase all`）按以下固定顺序执行：

1. 校验参数（不联网）；
2. 读取 `deploy_state.json`，丢弃区域 / 实例 / 工作区与本次不一致的旧状态；
3. 发现 ACXD 工作区（GET）；
4. Connect 只读预检：DescribeInstance（核对返回的实例 ARN 与 `--instance-arn` 一致）和队列解析。实例、队列或凭证有误时，在这里退出，ACXD 中不会留下半成品；
5. ACXD 写操作：上下文变量 → flow → 应用 → build（轮询到 `BUILT`）→ 部署（轮询到 `deployed`）；
6. 检查是否拿到了 applicationId 和 deploymentAlias。缺任意一个就以退出码 1 停止，不写任何 Connect 流；
7. Connect 写操作：screenpop 流 → 入站流；
8. 打印最终摘要。

### 幂等重跑

重复运行同一条命令不会产生重复资源：

- 上下文变量按名称查找，已存在就跳过；
- ACXD flow 按 flowId 查找，应用按名称查找，部署按 environment 查找，已存在就更新（PATCH）；
- 同名资源保护：按前缀派生名称的四个资源，只有确认是本脚本创建的才会被更新（覆盖）：
  - ACXD 应用 `<前缀>SelfService` 与 ACXD flow `<前缀>Intake`：`metadata.tags` 含 `device-fault-selfservice`（脚本每次创建 / 更新都会写入）；
  - 两个 Connect 流：资源标签 `acxd-setup=device-fault-selfservice`（`CreateContactFlow` 时写入）；在加入标签之前由本脚本创建的流，按内容的 `StartAction` 是否为本脚本模板的固定 Identifier 识别。未发布的流读取 `:$SAVED` 版本。
  - 其他同名资源会让部署在任何写操作之前以退出码 2 停止（Connect 流在 Connect 只读预检中检查，ACXD 资源在 ACXD 阶段第一次写入之前检查；写入前还会再检查一次，防止期间有人新建同名资源），并提示改用 `--name-prefix`（ACXD 应用还可以用 `--reuse-acxd-application`）；确认要覆盖时加 `--overwrite-untagged-resources`。有 2 个以上同名应用时以退出码 1 停止。`--preflight` 与交互确认摘要会提前显示这一情况；
- Connect 流按名称查找，已存在就用 `UpdateContactFlowContent` 更新已发布流的内容，否则用 `CreateContactFlow(Status=PUBLISHED)` 新建；
- build 不可变，每次运行都会新建一个 build，然后由部署切换到新 build；
- `deploy_state.json` 中的区域、实例或工作区与本次不一致时，会打 WARN 并忽略整个文件。

### 最终摘要与控制台链接

成功结束时（以及 `--preflight` 结束时）会打印中文摘要，内容包括：

- 实例 ARN（alias、区域）；
- 工作区 `name (id)` 及来源；
- 应用名称与 applicationId；
- ACXD flow `AgenticCareIntake`；
- buildId 与 version；
- environment 与 deploymentAlias；
- 语言（发生回退时显示 `zh-CN → en-US`）；
- 队列名称、ARN 及来源；
- 两个 Connect 流的名称、ID 和 ARN。

控制台链接以 DescribeInstance 返回的 `InstanceAccessUrl` 为基础：

- Connect 管理网站；
- ACXD 应用页：`<InstanceAccessUrl>/acxd/w/<工作区ID>/applications/<应用ID>`，与 ACXD 控制台的应用 URL 格式一致；
- 两个流的编辑页：`<InstanceAccessUrl>/contact-flows/edit?id=<流ARN>`。这是便利链接，没有 API 可以验证。

## 架构

```
来电 / 聊天
  └─ 入站流（AgenticCare Inbound ACXD Flow）
       01 开启流日志
       02 设置 DefaultAgentUI 事件流 = screenpop 流
       03 设置 TTS 声音（只影响 08/09 两条 Connect 提示）
       0b Customer Profiles 查询（GetCustomerProfile，_phone = $.CustomerEndpoint.Address）
       │   找到 ─► 0c 写 CustomerName / CustomerPhone / CustomerEmail
       │            → 12 姓名为空白（" "）？是则 13 写“未提供”
       │            → 0d/0e 邮箱含 @？否则改用 BusinessEmailAddress（0f），都没有写“未提供”（10）
       │   NoneFound / MultipleFound / Error ─► 11 写“未找到” + 来电号码
       │   所有分支 ─► 04（查询失败不影响通话）
       04 Agentic CX 块 ──► ACXD 应用 AgenticCareSelfService
       │     ACXD flow AgenticCareIntake：欢迎语 → Generative Journey（对话收集三项并复述确认）
       │       → 3 个 Generative Text（从对话记录提取 DeviceName / DeviceModel / FaultDescription）
       │       → Escalate（转接提示 + 三个 set-context），不使用 User input 节点
       │     三项写入上下文变量，经 $.AgenticCX.ContextVariables.* 回传
       │   Default / Escalation / NoMatchingCondition ─► 05
       │   NoMatchingError ─► 08   InputTimeLimitExceeded ─► 09
       05 写联系人属性 DeviceName / DeviceModel / FaultDescription
       06 设置工作队列 → 07 转队列
       08 错误提示 / 09 超时提示 → 0a 挂断
座席接起
  └─ screenpop 流（AgenticCare ScreenPop Flow，DefaultAgentUI 事件流）
       ShowView（AWS 托管 Detail 视图）：AttributeBar + 表格显示 $.Attributes.<客户三项 + 设备三项>
```

## 客户资料弹屏

- 前提：实例已启用 Customer Profiles（默认域名形如 `amazon-connect-<INSTANCE_ALIAS>`）。块的写法（`GetCustomerProfile`、`_phone`、结果在 `$.Customer.*`、三个错误分支）取自实例中的已发布流与官方文档（A22）。
- 顺序：弹屏共 6 项，默认客户姓名 / 来电号码 / 邮箱在前，设备三项在后，全部可复制。调整顺序只需改 `config.SCREENPOP_FIELDS`（属性栏与表格同时生效）；`config.FIELDS` 仍只代表 ACXD 收集的三项。
- 来电号码始终取 `$.CustomerEndpoint.Address`，即使没有找到客户资料。
- 姓名：zh-CN 显示“姓 名”，en-US 显示“First Last”，格式在 `acxd/messages.json` 的 `screenpop.customer.name_format`。zh-CN 中间保留一个空格：只有空格分隔的两个引用经过运行时验证，两个引用紧挨着（`{LastName}{FirstName}`）未经验证，校验器会拒绝。要去掉空格，先拨测确认紧挨着的引用能被解析，再放开 `flowlib.customer_name_expression` 中的检查。
- 邮箱：优先 `EmailAddress`；不含 `@` 时改用 `BusinessEmailAddress`；都没有时显示“未提供” / “Not provided”。`PersonalEmailAddress` 无法由该块请求，不使用。
- 找不到资料、匹配到多个资料或查询出错时，姓名与邮箱显示“未找到” / “Not found”。
- 找到资料但资料中没有姓和名时（例如只按电话建档的资料），0c 写入的姓名只剩分隔空格；随后的 Compare（12，`$.Attributes.CustomerName` Equals `" "`）把它改为“未提供” / “Not provided”（13，`screenpop.customer.no_name`）。比较值由 `name_format` 去掉引用后得出。临时发布流已确认 Connect 接受该写法且保存时不去掉空格；运行时是否命中尚未拨测，不命中时姓名仍显示空白，通话不受影响。
- 实例数据提示：验证用实例上的已发布流 `voicedemo ScreenPop Flow` 会为陌生来电创建 `FirstName = $.CustomerEndpoint.Address` 的资料。经过该流的来电者，弹屏姓名会显示为其电话号码（en-US 为“+1555… ”，zh-CN 为“ +1555…”，带一个空格）。这是实例中已有的资料，不是本流程的问题。

## 前提

- Amazon Connect **Customer** 实例（Agentic CX 块只在 Customer 实例中可用），位于 ACXD 可用区域：us-east-1、us-west-2、eu-central-1、eu-west-2、ap-southeast-2、ap-northeast-2、ap-northeast-1、ap-southeast-1、ca-central-1。
- 一个已有的 STANDARD 队列（默认使用 `BasicQueue`）。
- 由 Account Admin 在 Admin Hub **手工**创建：ACXD 工作区、programmatic user（在该工作区有可编辑与部署的角色，并能列出工作区）以及它的 API key（形如 `<prefix>.<secret>`，只显示一次）。脚本不创建这些。
- Python ≥ 3.10，安装依赖：`python3 -m pip install -r requirements.txt`（只依赖 `boto3==1.43.49`，ACXD 部分只用标准库 `urllib`）。
- AWS 凭证（标准 boto3 凭证链）需要以下 Connect 权限：`connect:DescribeInstance`、`DescribeQueue`、`ListQueues`、`ListContactFlows`、`DescribeContactFlow`、`CreateContactFlow`、`UpdateContactFlowContent`、`TagResource`（新建流时写入归属标签需要）。

## 文件说明

| 文件 | 作用 |
|---|---|
| `deploy.py` | CLI 入口与编排（阶段、dry-run、状态文件） |
| `acxd_client.py` | ACXD REST 客户端：API key 认证、重试策略、错误解析、分页 |
| `flowlib.py` | 模板加载、占位符替换、流校验器、Agentic CX action 挑选与分支重建 |
| `config.py` | 全部常量与假设 A1–A22（`FIELDS` 是三个字段名的唯一来源；`CUSTOMER_FIELDS` / `SCREENPOP_FIELDS` 是客户资料字段与弹屏顺序；两输入的默认值 `DEFAULT_LANGUAGE` / `FALLBACK_LANGUAGE` / `DEFAULT_QUEUE_NAME` 也在这里） |
| `acxd/acxd_flow.json` | ACXD flow 节点模板（9 个节点：start、欢迎语、Generative Journey、重试 loop + 提示、3 个 Generative Text、Escalate） |
| `acxd/messages.json` | zh-CN / en-US 提示语、Connect 错误提示、弹屏标签 |
| `flows/inbound_acxd_flow.json` | Connect 入站流模板 |
| `flows/screenpop_flow.json` | Connect 弹屏流模板 |
| `flows/fragments/agentic_cx_action.unverified.json` | 未验证的 Agentic CX 占位片段（只用于 dry-run） |
| `tools/check_flows.py` | 独立的流引用完整性检查器（不 import 项目代码） |
| `tests/` | 单元测试（无网络）；`tests/fixtures/agentic_cx_action_real.json` 为实网流中的 Agentic CX action，`tests/fixtures/acxd_application_workshop1014.json` 为实网应用 workshop1014 的 GetApplication 响应（两份 fixture 中的标识符均已替换为假值） |

## 参数

| 参数 / 环境变量 | 说明 |
|---|---|
| `--instance-arn` / `CONNECT_INSTANCE_ARN` | **两个必需输入之一**：Connect 实例 ARN，区域、账号和实例 ID 都从中推导。格式错误（包括误传了队列或流的 ARN）时退出 2 |
| `ACXD_API_KEY` / `--acxd-api-key-file` / `--acxd-api-key` | **两个必需输入之一**：ACXD API key。优先级为环境变量 > 文件 > CLI 参数 > 交互提示。acxd / all 阶段（包括 `--preflight`）必需；交互终端中缺失时用 getpass 提示，非终端时退出 2。`--acxd-api-key-file` 的文件只能有一行，权限不是 600 时会警告。`--acxd-api-key` 会进入 shell 历史和进程列表，使用时会警告。单独的 `--phase connect` 不提示输入，但如果提供了 key，会用它发现工作区并查询应用；dry-run 只用它检查产物中不含 key |
| `--phase` | `acxd` / `connect` / `all`（默认） |
| `--dry-run` | 不发起任何网络调用，产物写入 `<output-dir>/dry-run/`。没有工作区 ID 时使用占位值 `DRYRUN-WORKSPACE-ID`；没有队列参数时使用占位 ARN `.../queue/DRYRUN-QUEUE`，显示名为 `BasicQueue` |
| `--preflight` | 只读预检：对真实环境只调用 GET / List / Describe（客户端层强制：ACXD 非 GET、Connect 非 Describe/List/Get/Search 的调用在发出前就抛错），把真实值渲染的全部请求体与流 JSON 写入 `<output-dir>/preflight/`，并在 `preflight_report.json` 中列出真实运行将执行的写操作。只用两个输入即可运行（工作区、队列同样自动解析）；不能与 `--dry-run` 同用；不写 `deploy_state.json`（只读取它）。见“真实环境验证结果（只读探测）” |
| `--region` / `AWS_REGION` | 可选覆盖：Connect 实例区域，默认取自 `--instance-arn`；没有 ARN 时必填 |
| `--instance-id` / `CONNECT_INSTANCE_ID` | 可选覆盖：Connect 实例 ID，默认取自 `--instance-arn`；没有 ARN 时 connect 阶段必填 |
| `--queue-arn` / `CONNECT_QUEUE_ARN` 或 `--queue-name` / `CONNECT_QUEUE_NAME` | 可选覆盖，最多提供一个；都不提供时使用 `BasicQueue`（没有则取按名称排序后的第一个 STANDARD 队列）。ARN 中的实例 ID 必须与实例一致 |
| `--acxd-workspace-id` / `ACXD_WORKSPACE_ID` | 可选覆盖：默认通过只读 ListWorkspaces 自动发现（见“自动推导规则”） |
| `--acxd-region` / `ACXD_REGION` | 默认等于实例区域 |
| `--acxd-endpoint` / `ACXD_ENDPOINT` | 覆盖 endpoint，必须为 https |
| `--acxd-environment` | `development` / `qa` / `staging` / `production`（默认）。交互模式下未指定时会询问 |
| `--acxd-application-id` | connect 阶段单独运行时用于生成 / 校验 Agentic CX action；配合 `--reuse-acxd-application` 时指定要复用的现有应用。在 acxd / all 阶段不配合 `--reuse-acxd-application` 时会被忽略（打 WARN），仍使用 `AgenticCareSelfService` |
| `--acxd-deployment-alias` / `ACXD_DEPLOYMENT_ALIAS` | 已部署环境的 `deploymentAlias`（Agentic CX 块的 `Alias`，随机字符串，不是环境名），connect 阶段单独运行时使用；须匹配 `^[A-Za-z0-9_-]{1,128}$` |
| `--reuse-acxd-application` | ACXD 阶段把 flow 部署进 `--acxd-application-id`（必须是 UUID）指定的现有应用，而不是新建 `AgenticCareSelfService`：追加 flow、把 Welcome 默认流改为本 flow，并替换该应用 `--acxd-environment` 的部署。`--language` 必须在该应用的 `languageCodes` 中。与 `--phase connect` 一起使用时无效果（只告警）。见“复用现有 ACXD 应用” |
| `--language` | 可选覆盖：`zh-CN` / `en-US`。不指定时默认 `zh-CN`，并在 ACXD 以语言原因拒绝时自动改用 `en-US`（复用模式除外）；显式指定时不回退。单独运行 `--phase connect` 且不指定时，使用 `deploy_state.json` 中记录的最终语言 |
| `--name-prefix` / `NAME_PREFIX` | 资源名前缀，默认 `AgenticCare`。优先级：命令行参数 > 环境变量 `NAME_PREFIX` > 交互提示 > 默认值。必须字母开头，共 3–31 位字母或数字（`^[A-Za-z][A-Za-z0-9]{2,30}$`），保证所有派生名称合法。派生名称：ACXD flowId `<前缀>Intake`、Generative Journey 节点名 `<前缀>Journey`、ACXD 应用 `<前缀>SelfService`、Connect 流 `<前缀> ScreenPop Flow` / `<前缀> Inbound ACXD Flow`。用同一前缀重跑会更新（覆盖）这些同名资源，换新前缀会另建一套。上下文变量（`DeviceName` 等）及其 metadata（`/device-fault`、`device-fault-selfservice`）不随前缀变化，所有前缀共用。交互模式下未指定时会询问 |
| `--overwrite-untagged-resources`（旧名 `--overwrite-untagged-application` 仍可用） | 允许覆盖不是本脚本创建的同名 ACXD 应用 / ACXD flow / Connect 流（默认拒绝，退出 2，见“幂等重跑”的同名资源保护）。会替换这些资源的内容，优先改用 `--name-prefix` |
| `--tts-voice` / `--tts-engine` | Connect 提示语声音，默认 zh-CN `Zhiyu`、en-US `Joanna`，引擎 `Neural` |
| `--agentic-cx-action-file` | 可选覆盖：Agentic CX action JSON（单个 action 或完整流 JSON） |
| `--agentic-cx-reference-flow-id` | 可选覆盖：控制台中手工配置了 Agentic CX 块的参考流 ID 或 ARN |
| `--agentic-cx-action-identifier` | 按 Identifier 指定参考流中的 action |
| `--allow-unverified-agentic-cx-action` | 允许非 dry-run 使用占位片段（Connect 会拒绝，只用于排查） |
| `--build-timeout` / `--poll-interval` | 轮询超时 / 间隔秒数，默认 600 / 5 |
| `--output-dir` | 默认 `./output` |
| `--verbose` | DEBUG 日志（不含请求头与密钥） |
| `--non-interactive` | 关闭交互模式：不询问任何参数（包括 API key 的 getpass 提示），也不要求确认；缺少必需参数时直接退出 2。见“方式 4：交互模式” |
| `--yes` / `-y` | 交互模式下跳过真实运行前的参数确认（缺少的参数仍会询问）；非交互时没有作用 |

退出码：0 成功；1 运行或服务端错误（包括在确认提示处回答 N）；2 配置错误（包括连续 5 次输入无效）；130 在交互提示处按 Ctrl-C / EOF 取消。

## 分步运行（可选，旧写法仍然可用）

两输入命令已经覆盖下面所有步骤。只有需要分阶段执行或显式指定每个值时，才需要这些写法。

1. 预演（不需要任何凭证）：
   ```bash
   python3 deploy.py --dry-run --region us-east-1 \
     --instance-id 11111111-2222-4333-8444-555555555555 \
     --queue-arn arn:aws:connect:us-east-1:123456789012:instance/11111111-2222-4333-8444-555555555555/queue/66666666-7777-4888-9999-000000000000 \
     --acxd-workspace-id 67898477-f36f-4b06-bdca-0d9a1f2eb9cc
   python3 tools/check_flows.py output/dry-run
   ```
2. 部署 ACXD 部分：
   ```bash
   export ACXD_API_KEY='<ACXD API key>'   # 或 --acxd-api-key-file <文件>，或者不设置、运行时按提示输入
   python3 deploy.py --phase acxd --region us-east-1 --acxd-workspace-id <工作区ID>
   ```
   脚本会创建上下文变量、ACXD flow `AgenticCareIntake`、应用 `AgenticCareSelfService`，新建 build 并部署到 `--acxd-environment`。
3. 部署 Connect 部分（`--phase all` 时自动接着第 2 步执行，无需额外参数）。单独运行时：
   ```bash
   python3 deploy.py --phase connect --region us-east-1 \
     --instance-id <实例ID> --queue-arn <队列ARN> --acxd-workspace-id <工作区ID> \
     [--acxd-application-id <应用ID> --acxd-deployment-alias <deploymentAlias>]
   ```
   脚本按 workspaceId / applicationId / deploymentAlias 生成 `ConnectParticipantWithAgenticCX` 块（`agenticCxActionSource: generated`）。applicationId 与 deploymentAlias 的来源依次为：CLI 参数 → 设置了 `ACXD_API_KEY` 时通过 ListApplications（按名称 `AgenticCareSelfService`）与 ListApplicationDeployments（`--acxd-environment` 对应的 deployment）查询 → 上次真实运行的 `output/deploy_state.json`。

   Agentic CX action 来源优先级：`--agentic-cx-action-file` > `--agentic-cx-reference-flow-id` > generated > 已部署入站流中的 action > 未验证占位片段。分支映射（与实网流一致）：

   | Agentic CX 分支 | 去向 |
   |---|---|
   | Default（NextAction）/ Escalation / NoMatchingCondition | 05 写联系人属性 → 设置队列 → 转队列 |
   | NoMatchingError | 08 错误提示 |
   | InputTimeLimitExceeded（Idle chat timeout） | 09 超时提示 |
4. **手工关联电话号码**（脚本结束时会打印示例命令）：
   ```bash
   aws connect associate-phone-number-contact-flow --region us-east-1 \
     --instance-id <实例ID> --phone-number-id <电话号码ID> --contact-flow-id <入站流ID>
   ```

无法生成 action（缺 workspaceId / applicationId / deploymentAlias）且没有其他来源时，非 dry-run 的 connect 阶段会在任何 Connect 写操作之前以退出码 2 停止，并打印获取方法。可选覆盖方式：在 Connect 控制台建一个含 Agentic CX 块的流并发布，用 `--agentic-cx-reference-flow-id` 传入；或用 `--agentic-cx-action-file` 提供 action JSON。

### 复用现有 ACXD 应用

默认会新建专用应用 `AgenticCareSelfService`，不影响已有应用（推荐）。如需把 flow 部署进已有应用（例如 workshop1014），显式加 `--reuse-acxd-application`：

```bash
export ACXD_API_KEY='<ACXD API key>'
python3 deploy.py --phase acxd --region <REGION> \
  --acxd-workspace-id <WORKSPACE_ID> \
  --acxd-application-id <APPLICATION_ID> --reuse-acxd-application --language en-US
```

脚本先 GetApplication（应用不存在或 `--language` 不在应用语言中时，在任何写操作之前退出 2），然后只 PATCH `flows` 与 `settings`：

- `flows` 追加 `AgenticCareIntake`；
- `settings` 原样保留，只把 `defaultFlows["ACXD.Welcome"]` 从 `Maintplink` 改为 `AgenticCareIntake`（Fallback / Unknown 等不变）；
- 新 build 会替换该应用 production 部署（alias `<DEPLOYMENT_ALIAS>` 下原来的 build `<BUILD_ID>`）。

修改前的应用写入 `output/acxd_application_before.json`，原 Welcome flowId 记录在 `deploy_state.json` 的 `acxd.previousWelcomeFlowId`。回滚（手工）：在 ACXD 控制台把应用的 Welcome 默认流改回 `Maintplink`，再把 production 部署更新回原来的 build `<BUILD_ID>`。dry-run 无法读取实网应用，`acxd_application.json` 只是按空 settings 合并的示意。

## 产物

- 真实运行：`output/` 下的请求体与流 JSON（每次上传前写入），以及 `output/deploy_state.json`。
- dry-run：全部写入 `output/dry-run/`，不会读写 `output/deploy_state.json`。
- preflight：全部写入 `output/preflight/`（与 dry-run 同名的请求体与流 JSON，复用模式另有 `acxd_application_before.json`，以及 `preflight_report.json`：`resolved` 解析到的实例 / 队列 / 应用 / deploymentAlias / 已有流 ID / action 来源，`plannedWrites` 计划写操作 `{service, operation, target}`，`warnings`）。不会写 `output/deploy_state.json`；`--phase connect` 时只读取它来查找 applicationId / deploymentAlias。尚不存在的资源用占位值渲染：`PREFLIGHT-APPLICATION-PENDING`、`PREFLIGHT-ALIAS-PENDING`、`PREFLIGHT-BUILD-PENDING`、`.../contact-flow/PREFLIGHT-SCREENPOP`。
- `deploy_state.json`（`schemaVersion: 1`）：`dryRun`、`region`；`acxd` 段（workspaceId、workspaceName、workspaceSource、endpoint、contextVariables、flowId、applicationId、flowNodeIdsRemapped、buildId、buildVersion、deploymentId、environment、deploymentAlias、applicationReused、applicationName、previousWelcomeFlowId、language、languageFallbackFrom）；`connect` 段（instanceId、instanceArn、instanceAlias、instanceAccessUrl、queueArn、queueName、screenpop / 入站流的 ID 与 ARN、agenticCxActionSource）；`completedSteps`（本次运行完成的步骤，不重复）。每步成功后增量写入。区域 / 实例 / 工作区与本次不一致的旧状态会被忽略。

## ⚠️ 假设清单（未经验证，均可调整）

| ID | 假设 | 调整位置 |
|---|---|---|
| A1 | 已按实网流确认：Agentic CX 块 `Type` 为 `ConnectParticipantWithAgenticCX`，`Parameters` 为 AgentConfiguration + SpeechRecognitionConfiguration + AudioFillerConfiguration，分支见上表 | `config.AGENTIC_CX_ACTION_TYPE` / `AGENTIC_CX_STATIC_PARAMETERS` / `AGENTIC_CX_TRANSITIONS` / `AGENTIC_CX_BRANCH_KEYWORDS` |
| A2 | ACXD API 区域与 Connect 实例区域相同 | `--acxd-region` / `--acxd-endpoint` |
| A3 | 已按参考应用确认：generative_text 的输出（`generativeText.name`）以 `{DeviceName:NLX.Local}` 读取 | `config.LOCAL_VAR_TEMPLATE` |
| A4 | escalate 节点的 stateModifications 把三个本地变量写入同名工作区上下文变量 | `config.state_mod_set_context()` |
| A5 | 不使用 user_input 节点：Generative Journey 接管客户的每一轮对话，退出条件 `detailsCollected` / `humanHandoff` 按 `System.gjConditionIndex` 0 / 1 路由 | `config.GJ_EXIT_CONDITION_NAMES`、`acxd/acxd_flow.json` |
| A6 | 所有 Generative Journey / Generative Text 节点使用 `anthropic.claude-haiku-4-5`（取自参考流 Maintplink）；zh-CN 下 Haiku 的对话效果未经实测 | `config.GENERATIVE_MODEL_TYPE` |
| A7 | 已按实网数据修正：escalate 节点带一个无 nodeId、条件为 `node_status eq continuation` 的子节点 | `acxd/acxd_flow.json` |
| A8 | 已按实网数据修正：消息中用 `{DeviceName:NLX.Context}` 引用上下文变量 | `config.CONTEXT_VAR_MESSAGE_PLACEHOLDER` |
| A9 | 工作区级上下文变量即 `$.AgenticCX.ContextVariables` 的来源 | `config.FLOW_SCOPED_CONTEXT_VARIABLES` |
| A10 | DefaultAgentUI 事件流能通过 `$.Attributes.<key>` 读到原联系属性 | 模板；真实环境验证第 6 步 |
| A11 | zh-CN 在 agentic voice 下可用；默认 zh-CN，未显式指定 `--language` 时 ACXD 以语言原因拒绝会自动改用 en-US | `--language`、`config.DEFAULT_LANGUAGE` / `FALLBACK_LANGUAGE` |
| A12 | Deployment 成功状态为 `deployed`，含 `fail` 视为失败 | `config.DEPLOYMENT_SUCCESS_STATUSES` / `DEPLOYMENT_FAILURE_MARKER` |
| A13 | `oneClickDeployEnabled=false` 阻止 build 后自动部署 | `config.APPLICATION_DEPLOYMENT_SETTINGS` |
| A14 | 已按实网数据修正：`languageSettings` 条目为 `{"languageCode": ..., "useLex3pAsr": false}`（SDK 形状；文档中的 useNativeLanguage/region 在实网不存在） | `config.LANGUAGE_SETTING_EXTRA` |
| A15 | 客户端提供的 UUIDv4 nodeId 被接受 | 不一致时只 WARN，记录到 `flowNodeIdsRemapped` |
| A16 | 错误类型可能只在 `x-amzn-ErrorType` 头中 | `acxd_client._parse_error` |
| **A17** | ACXD 对话使用应用默认 TTS 声音 | `config.LANGUAGE_SETTING_EXTRA = {"voice": ...}` 或 `config.ACXD_NODE_VOICE_SETTINGS` |
| A18 | 已按实网流确认：`AgentConfiguration.Alias` 即 `deploymentAlias`（随机字符串），环境名只出现在 ActionMetadata 的 displayName 中 | `--acxd-deployment-alias`；采集的 action 只用于挑选与告警 |
| **A19** | Detail 视图会转义表格中代入的属性值 | 不转义时设 `config.SCREENPOP_SECTIONS_SHOW_VALUES = False`（六项只在纯文本属性栏显示） |
| **A20** | Escalate 节点不设 `sendContext` 时上下文变量仍会回传 | 联系人属性为空时设 `config.ESCALATE_NODE_METADATA = {"name": "EscalateToAgent", "sendContext": True}` 后重跑 `--phase acxd` |
| A21 | ACXD 以语言原因拒绝时，错误信息（400）或 FAILED build 对象中（先去掉 `languageSettings` / `languageCodes` / `languageCode` / `mainLanguageCode` 等回显字段）含 `language` / `locale` / `zh-cn`。误判时最多多做一次 en-US 尝试，失败后照常报错，不写任何 Connect 流；漏判时按原样失败，并提示 `--language en-US` | `config.LANGUAGE_FAILURE_KEYWORDS` / `LANGUAGE_ECHO_KEYS` |
| A22 | 已按实网流与文档确认 `GetCustomerProfile` 写法；zh-CN 姓名保留空格（紧挨着的两个引用未经运行时验证）；`Compare` 的 `TextContains` 与空白姓名的 `Equals " "` 已由临时发布流确认 Connect 接受，运行时是否命中未拨测 | `config.CUSTOMER_PROFILE_NAME_TOKENS`、`messages.json` 的 `screenpop.customer` |

## 真实环境验证步骤

1. `--phase acxd` 后确认 build 为 `BUILT`、deployment 为 `deployed`；若返回 400 且指向 `languageSettings`，按 A14 调整后重跑。
2. 在 ACXD 控制台打开 `AgenticCareIntake` 画布检查连线；用应用的 Test 标签页测试，确认流经过 Escalate 节点后 DeviceName / DeviceModel / FaultDescription 三个上下文变量都已写入（A3–A6 不成立时 build 会失败或值为空）。
3. 确认日志中 `Agentic CX action source: generated`，以及 applicationId / deploymentAlias 的来源正确。
4. 在 Connect 控制台打开入站流，确认 Agentic CX 块显示所选工作区 / 应用 / 环境，并核对日志里的分支映射。
5. 关联电话号码后拨测，按助手的提问说出三项并确认复述；确认 ACXD 提示语用所选语言播报，不对时按 A17 调整后重跑 `--phase acxd`。
6. 座席接起后 agent workspace 弹出 Detail 视图，客户三项与设备三项显示正确：已有客户资料的号码显示资料中的姓名和邮箱，未知号码显示“未找到”和来电号码，有资料但没有姓名的号码姓名显示“未提供”（A22）；在联系人详情中确认 CustomerName / CustomerPhone / CustomerEmail 与设备三个属性存在。属性存在但弹屏为空 → A10 不成立；属性本身为空 → 按 A20 设置 `sendContext` 后重跑 `--phase acxd`。
7. 弹屏注入检查（上线前必做，A19）：用聊天渠道在故障描述中输入 `<b>x</b><a href="https://example.com">y</a>`，并用一个姓名为 `<b>x</b>`、邮箱含 `<i>` 的测试客户资料来电（客户资料可能由其他流或外部集成写入），座席端两处都应原样显示；若被渲染成粗体或链接，把 `config.SCREENPOP_SECTIONS_SHOW_VALUES` 设为 `False` 后重跑 `--phase connect`。
8. 在 CloudWatch 流日志中确认 Error 分支没有被触发。

## 真实环境验证结果（只读探测）

对实例 `<INSTANCE_ALIAS>`（`<INSTANCE_ID>`，`<REGION>`）与 ACXD 工作区 `<WORKSPACE_ID>` 只做了 GET / List / Describe 调用，没有创建、修改、build、部署或删除任何资源。完整证据与计划见 `.agents/tasks/live-verification-plan.md`。

假设状态（已确认 / 已推翻并已修正 / 仍未验证）：

- A1 已确认：Agentic CX 块为 `ConnectParticipantWithAgenticCX`，结构取自实例中的真实流，脚本据此生成。
- A2 已确认：`https://api.acxd.connect.<REGION>.amazonaws.com` + `x-api-key` + `x-workspace-id` 返回 200。
- A3 已按参考应用确认：参考应用 workshop1014 的流 `Maintplink`（证据 `.agents/tasks/genjourney-evidence/ref_flow_Maintplink.json`）中 generative_text `name: modelNumber` 的输出在 escalate 节点以 `{modelNumber:NLX.Local}` 写入上下文变量，本项目照搬这一写法。
- A4 已确认：Set 上下文变量的 stateModifications 写法与参考流一致。
- A5 已按参考应用确认结构：参考流为 start → basic 欢迎语 → generative_journey → generative_text → escalate，中间没有 user_input；退出条件 i 的出边条件为 `System.gjConditionIndex eq i`，另有 `node_status` timeout / failure 两条出边。
- A6 已按参考应用确认：参考流的 Generative Journey 与 Generative Text 的 `modelType` 均为字符串 `anthropic.claude-haiku-4-5`。参考流只有 en-US，zh-CN + Haiku 以及“3 个 Generative Text 逐项提取”的效果要在真实 build 与拨测后才能确认。
- A7 已推翻并修正：escalate 节点加无 nodeId 的 continuation 子节点。
- A8 已推翻并修正：消息中写作 `{DeviceName:NLX.Context}`。
- A9 仍未验证（强支持）：实网生产流从 `$.AgenticCX.ContextVariables.<变量>` 读取工作区变量。
- A10 仍未验证（强支持）：实例中的 DefaultAgentUI 流读写 `$.Attributes.*`，未直接证明能读到入站流写的属性。
- A11 仍未验证（有风险）：工作区中只有 en-US 应用，没有 zh-CN 证据；失败时改用 `--language en-US`。
- A12 已确认：deployment 成功状态为 `deployed`。
- A13 仍未验证（结构已确认）：`deploymentSettings` 结构与实网一致，`oneClickDeployEnabled=false` 的效果无法只读验证。
- A14 已确认：`languageSettings` 条目为 `{"languageCode": ..., "useLex3pAsr": false}`。
- A15 仍未验证（强支持）：实网 flow 中存在人工构造的 nodeId。
- A16 已确认：未知路由 403 且类型只在 `x-amzn-ErrorType` 头中；不存在的应用 404 `ResourceNotFoundException`。
- A17 仍未验证：实网语音流在 Agentic CX 块前设置 `connect:agentic` TTS，本项目暂未加。
- A18 已确认（applicationId）/ 已推翻并修正（alias）：`Alias` 是 deploymentAlias 随机串，不是环境名。
- A19 仍未验证：Detail 视图的转义行为只能人工检查。
- A20 仍未验证（强支持）：实网 escalate 节点都没有 `sendContext`，生产流仍能读到上下文变量。

实例中的真实 Agentic CX 块（流 `AI Agent - ACXD`）：`Parameters` = `AgentConfiguration {WorkspaceId, ApplicationId, Alias=<deploymentAlias>}` + `SpeechRecognitionConfiguration {SpeechRecognitionEngine: AMAZON_AGENTIC_VOICE}` + `AudioFillerConfiguration {Enabled: true, AudioType: MELODY_PONDERING_PONG, ...}`；分支为 Default（NextAction）、Condition `Escalation`、Errors `NoMatchingError` / `NoMatchingCondition` / `InputTimeLimitExceeded`。实例里的两个 Agentic CX 流都指向别的应用，只作为结构证据，不要作为参考流传入。用户现有应用 workshop1014 只挂了 flow `Maintplink`，不收集这三项，所以默认新建 `AgenticCareSelfService`（复用方式见上文）。

两输入只读预检命令（key 在同一条命令中从 `.secrets/` 读入环境变量，不回显、不写入任何文件）：

```bash
cd <PROJECT_DIR> && export ACXD_API_KEY="$(cat <ACXD_API_KEY_FILE>)" && \
python3 deploy.py --preflight --instance-arn arn:aws:connect:<REGION>:<ACCOUNT_ID>:instance/<INSTANCE_ID> \
  --output-dir <PREFLIGHT_OUTPUT_DIR>
```

实测结果：

- 退出码 0；ACXD 只发出 4 个 GET（`/sdk/workspaces`、`/sdk/context-variables`、`/sdk/flows`、`/sdk/applications`），Connect 只调用了 Describe / List，没有任何写调用；
- 工作区通过 ListWorkspaces 发现为 `<WORKSPACE_NAME> (<WORKSPACE_ID>)`；
- 队列默认解析为 `BasicQueue`（`.../queue/<QUEUE_ID>`，来源 `default:BasicQueue`）；
- 实例 alias 为 `<INSTANCE_ALIAS>`，`InstanceAccessUrl` 为 `https://<INSTANCE_ALIAS>.my.connect.aws`；
- `plannedWrites` 为新应用路径：CreateContextVariable ×3（DeviceName / DeviceModel / FaultDescription）、CreateFlow `AgenticCareIntake`、CreateApplication `AgenticCareSelfService`、CreateApplicationBuild、CreateApplicationDeployment（production）、CreateContactFlow ×2（`AgenticCare ScreenPop Flow`、`AgenticCare Inbound ACXD Flow`）。

三条警告都是预期的：

- zh-CN 尚未验证（A11），真实运行被语言拒绝时会自动改用 en-US；
- applicationId / deploymentAlias 要在 ACXD 阶段后才存在，入站流先用 PENDING 占位渲染；
- screenpop 流尚不存在，先用占位 ARN 渲染。

改用 `--acxd-api-key-file <ACXD_API_KEY_FILE>`（不设置 `ACXD_API_KEY`）时结果相同。

需要用户批准后才执行的写步骤（预检不会执行）。推荐只执行一条两输入命令：

```bash
cd <PROJECT_DIR> && export ACXD_API_KEY="$(cat <ACXD_API_KEY_FILE>)" && \
python3 deploy.py --instance-arn arn:aws:connect:<REGION>:<ACCOUNT_ID>:instance/<INSTANCE_ID>
```

它按“顺序保证”执行上面 `plannedWrites` 中的全部写操作：新建应用 `AgenticCareSelfService`，不影响现有应用。重跑时按名称更新，不会产生重复资源。回滚需要手工操作：在控制台删除 `AgenticCareSelfService`、`AgenticCareIntake`、3 个上下文变量和两个 Connect 流。

下表是可选的分步写法，与上面的命令等价或作为替代：

| 步骤 | 命令 | 影响 |
|---|---|---|
| W1 ACXD 阶段（推荐：新应用） | `python3 deploy.py --phase acxd --region <REGION> --acxd-workspace-id <WORKSPACE_ID> --acxd-environment production --language zh-CN` | 新建 3 个工作区变量、flow、应用 `AgenticCareSelfService`、build 与 production 部署；不影响现有应用。build 必须为 `BUILT`，语言失败时改用 `--language en-US` |
| W1' ACXD 阶段（复用 workshop1014，替代 W1） | `python3 deploy.py --phase acxd --region <REGION> --acxd-workspace-id <WORKSPACE_ID> --acxd-application-id <APPLICATION_ID> --reuse-acxd-application --language en-US` | 修改用户应用：追加 flow，Welcome 从 `Maintplink` 改为 `AgenticCareIntake`，替换 production 部署；回滚方法见“复用现有 ACXD 应用” |
| W3 Connect 阶段 | `python3 deploy.py --phase connect --region <REGION> --instance-id <INSTANCE_ID> --queue-name BasicQueue --acxd-workspace-id <WORKSPACE_ID>` | 新建并发布两个 CONTACT_FLOW，不改动现有流；Connect 是否接受流 JSON 在这一步才能确认 |
| W4 端到端拨测 | `aws connect associate-phone-number-contact-flow --region <REGION> --instance-id <INSTANCE_ID> --phone-number-id <电话号码ID> --contact-flow-id <W3 输出的入站流 ID>` | 改变该号码的路由（建议用空闲号码或先用聊天测试）；人工确认语言（A11/A17）、属性写入（A9/A20）、弹屏三项（A10）与转义（A19） |

两输入命令和 W1 / W1' / W3 都需要在同一条命令中先 `export ACXD_API_KEY="$(cat <ACXD_API_KEY_FILE>)"`（或改用 `--acxd-api-key-file`）。每一步前都可以先用相同参数加 `--preflight` 查看将执行的写操作。

## 本地验证

```bash
python3 -m py_compile deploy.py acxd_client.py flowlib.py config.py tools/check_flows.py
python3 -m unittest discover -s tests -v
python3 deploy.py --dry-run --instance-arn <实例ARN>   # 或“分步运行”第 1 步的旧写法
python3 tools/check_flows.py output/dry-run
```

## 局限

- 只做过只读探测与 `--preflight`，尚未执行任何写步骤：ACXD 是否接受新的 Generative Journey / Generative Text flow 与 build（结构取自参考流 Maintplink，由 W1 的 build 结果最终确认）、zh-CN 下 Haiku 的对话与提取效果（A6/A11）、默认 TTS 声音（A17）、弹屏渲染（A10）与转义（A19）仍未验证。
- 对话由 Generative Journey（Haiku）完成：助手逐项询问、复述三项并接受更正，最后统一播报一条转接提示（不再区分“已确认 / 未确认”两种转接提示）。
- 三项的值由 Generative Text 从对话记录中提取（例如把口述数字规整为型号 X50），不是客户原话的逐字记录；客户没有提供的项写为 `未提供`（zh-CN）/ `not provided`（en-US），弹屏中显示该词。Generative Journey 结束后、播报转接提示前会依次调用 3 次 Haiku。
- Connect 没有只校验（validate-only）的流 API（`CreateContactFlow --status SAVED` 也是写操作），所以 Connect 是否接受生成的流 JSON（包括 Agentic CX 块，A1 结构取自实网流）只有在 W3 才能证明；`--preflight` 只运行本地校验器。
- 不创建 Connect 实例、队列、座席、ACXD 工作区或 API key；不关联电话号码；不提供删除 / 回滚命令。
- 客户更正时助手最多再复述一次，之后仍有疑问的内容由坐席在弹屏中核对。

## 安全说明

- ACXD API key 按 环境变量 `ACXD_API_KEY` > `--acxd-api-key-file` > `--acxd-api-key` > 交互提示 的优先级读取。推荐使用环境变量或权限为 600 的 key 文件；`--acxd-api-key` 会进入 shell 历史和进程列表，使用时会警告。key 不写入任何输出文件或 `deploy_state.json`，每次写文件前（以及打印最终摘要前）都会检查内容中不含 key。日志只显示 `****<key ID 末 4 位>.****` 形式的掩码（既不含 secret，也不含 key 的固定前缀），DEBUG 日志也不打印请求头。key 文件读取失败时，错误信息只包含路径，不包含文件内容。
- 工作区发现只调用 ACXD `GET /sdk/workspaces`；`--preflight` 下 ACXD 客户端和 Connect 客户端都在发出请求前拦截任何写操作。
- 本项目只是部署脚本，不对外暴露任何网络服务。
- 弹屏 HTML 只由静态标签（已 HTML 转义）和固定的 `$.Attributes.<字段>` 令牌拼成；客户输入能否注入取决于 Detail 视图的转义行为，请按“真实环境验证步骤”第 7 步检查。

## Live Sync 产品报修演示（deploy_livesync.py）

**只支持英语（en-US）**：ACXD 应用、Connect 流（语言块 + agentic 声音 KATIE）、页面、邮件和弹屏标签都是英文；`--language` 只接受 `en-US`，交互模式不再询问对话语言，也没有语言回退。`deploy.py` 单独使用时仍支持 zh-CN / en-US。

客户先拨打热线（座机、手机都可以）。Connect 入站流调用 Lambda，用 SES 把报修页面的链接发到邮箱；客户在任意设备上打开链接，页面在 CloudFront 上，通过 Touchpoint 的 `external` 模式绑定到这通电话。电话里的语音助手一边说话一边驱动页面：

- 产品名称：页面上 3 个 dummy 产品（AquaWash Smart Washer / CoolBreeze Inverter AC / RoboClean Robot Vacuum），说出或点选，助手调用 `select_product` 选中；
- 产品型号：页面上 4 个 dummy 型号（X100 / X200 Pro / S300 / S500 Max），说出或点选，助手调用 `select_model` 选中；
- 故障描述：在电话里直接口述，页面不提供输入；
- 确认后转人工，弹屏显示 Product name / Product model / Fault description（沿用上下文变量 DeviceName / DeviceModel / FaultDescription）。
- 客户没有收到邮件或不方便看屏幕时，助手改为纯语音登记（产品和型号仍限定在上述选项中）。

```
来电 → 入站流：日志 → 弹屏事件流 → TTS 声音 + 语言 → 客户资料查询
     → InvokeLambdaFunction <前缀>-livesync-send-link（SES 发送 https://<CloudFront>/#t=<签名 token>）
     → linkSent = true 时播报“链接已发送到您的邮箱” → Agentic CX（Live Sync 节点）→ 转人工
客户打开链接 → CloudFront（S3 私有桶，OAC）→ 页面 POST /api/session（CloudFront → HTTP API → <前缀>-livesync-session）
     → 校验签名、有效期，并用 DescribeContact 确认通话仍在进行 → 返回 contactId 与 Live Sync key
     → Touchpoint input "external" + liveSync.contactId → 页面与电话同步
```

```bash
# 1. 部署（与 deploy.py 相同的两个输入，加一个测试邮件地址；交互模式下不给 --test-email 会询问）
export ACXD_API_KEY='<ACXD API key>'
python3 deploy_livesync.py --instance-arn arn:aws:connect:<REGION>:<ACCOUNT_ID>:instance/<INSTANCE_ID> \
  --test-email you@example.com

# 部署前脚本会按实例检查发件地址（见下文“发件地址检查”）；有多个可用地址时让你选择，一个都没有时提示并结束

# 2. 在 ACXD 控制台 应用 → Settings → Access 复制 Live Sync API key 与 Deployment key，写入 Secrets Manager：
export LIVE_SYNC_API_KEY='<Live Sync API key>' LIVE_SYNC_DEPLOYMENT_KEY='<Deployment key>'
python3 deploy_livesync.py --page-only

# 3. 把电话号码关联到入站流（脚本结束时打印示例命令），拨打热线，在测试邮箱打开“产品报修”邮件中的链接
```

| 参数 | 说明 |
| --- | --- |
| `--test-email` / `LIVE_SYNC_TEST_EMAIL` | 测试邮件地址：任何人打进来，链接都发到这里。不指定时发往来电号码在 Customer Profiles 中的邮箱 |
| `--email-from` / `LIVE_SYNC_EMAIL_FROM` | 发件地址，必须是 Connect 实例中可发信的邮件地址之一（见“发件地址检查”）。不指定时：交互模式下从列表中选择；非交互模式下只有一个可用地址时自动使用，多个时退出 2 并列出 |
| `--link-ttl-minutes` | 链接有效期，默认 30 分钟（5–240）；通话结束后链接立即失效 |
| `--page-only` | 不部署 ACXD / Connect：重新生成并上传页面，提供了 Live Sync key 时写入 Secrets Manager |
| `--live-sync-api-key-file` / `--live-sync-deployment-key-file` | 从文件读取两个 Live Sync key（环境变量优先） |
| `--touchpoint-js` | 使用本地 Touchpoint UMD 包（默认下载 `@amazon-connect-touchpoint/web@1.0.4` 并校验 npm integrity） |

**发件地址检查**（只读，在 Connect 预检中执行，早于交互确认和任何写操作；`--preflight` 同样执行）：

1. 实例区域的 SES 账号必须已出沙盒（`GetAccount` 的 `ProductionAccessEnabled` 为 true），且能正常发信；
2. 用 `SearchEmailAddresses` 列出该 Connect 实例的邮件地址（Email 渠道）；
3. 地址本身或其域名在 SES 中 `VerifiedForSendingStatus` 为 true 的，才是可选的发件地址。

任何一步不满足（仍在沙盒、实例没有邮件地址、地址都未验证）时，打印原因和处理方法并以退出码 2 结束，不执行任何写操作。脚本不再创建 SES 身份；收件地址（测试邮件地址或客户资料邮箱）不需要验证。dry-run 无法离线检查，栈模板中使用占位发件地址（可用 `--email-from` 指定）。

`--dry-run` / `--preflight` / 交互模式 / 同名资源保护 / 其他参数全部沿用 deploy.py。dry-run 额外写出 `dry-run/livesync_stack.json` 与本地页面；preflight 列出栈、SES、AssociateLambdaFunction、页面上传等计划写操作。

| 文件 | 作用 |
| --- | --- |
| `deploy_livesync.py` | 入口：处理邮件与 Live Sync key 参数，安装 Live Sync 配置后调用 `deploy.main` |
| `livesync.py` | Live Sync flow 渲染与校验、入站流的语言块与邮件链接块、Connect 阶段钩子（栈 / SES / 页面）、页面生成、Touchpoint 下载 |
| `livesync_infra.py` | CloudFormation 模板与栈生命周期、SES 身份、AssociateLambdaFunction、页面上传、Secrets Manager |
| `lambda/send_link.py` | 入站流调用：签发 token 并用 SES 发送链接（不记录 token、链接和完整邮箱） |
| `lambda/session.py` | `/api/session`：校验 token 与通话状态，返回 contactId 与 Live Sync key |
| `acxd/livesync_flow.json` / `acxd/livesync_messages.json` | ACXD flow 模板；en-US 的提示词、邮件文案、dummy 产品 / 型号、页面文案 |
| `web/` | 页面模板（index.html、app.js、styles.css） |
| `tests/test_livesync.py` | 单元测试（无网络） |

CloudFormation 栈 `<前缀>-livesync-page`（带标签 `acxd-setup=device-fault-selfservice`）包含：私有 S3 桶（仅 CloudFront OAC 可读）、CloudFront 分发（`/api/*` 转到 HTTP API，缓存关闭、托管安全响应头）、HTTP API（只有 `POST /api/session`，限流 5 rps）、两个 Lambda 与各自最小权限角色和日志组、Secrets Manager 密钥（CloudFormation 生成的签名密钥 + Live Sync key），以及允许该 Connect 实例调用 send-link 的资源策略。

额外需要的权限：CloudFormation 创建上述资源（含 `CAPABILITY_IAM`）、`ses:GetAccount` / `GetEmailIdentity`、`connect:SearchEmailAddresses` / `AssociateLambdaFunction` / `ListLambdaFunctions`、`s3:PutObject`、`secretsmanager:GetSecretValue` / `PutSecretValue`。

安全与限制：

- 页面是公开的静态文件，不含任何 key 或 contactId。Live Sync key 只在持有有效链接、且通话进行中时由 `/api/session` 返回（浏览器必须拿到它才能连接 Live Sync）。
- token 放在 URL 片段 `#t=` 中，不会发到 CloudFront / API Gateway，也不进入访问日志；页面打开后会从地址栏移除。
- 链接是持有即可用的凭证：邮件被转发时，别人在通话期间也能打开页面。演示可以接受，生产环境应加上一次性使用或客户身份校验。
- 发信走 SES（send-link Lambda），发件地址只借用 Connect 实例的邮件地址及其在 SES 中已验证的身份；邮件不会作为 Connect 联系出现在座席端。
- 未经验证的假设 L6 / L7：电话联系能否与 `external` 模式的页面同步、Live Sync 节点在页面打开前的表现，都需要拨测确认（`--dry-run` 会打印全部假设）。
- 回滚：删除 CloudFormation 栈（先清空 S3 桶），在 Connect 实例中解除 Lambda 关联，删除两个 Connect 流和 ACXD 资源。
