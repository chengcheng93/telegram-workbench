# TG 多账号工作台 V0.2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans. Track steps and evidence below; write tests before implementation. Independent modules may be delegated in parallel with explicit file ownership.

**Goal:** 实现用户完整 V0.2 文档的本地八账号工作台，保留旧功能，交付真实可登录两个账号的测试入口。

**Architecture:** 新程序独立存放于 outputs/telegram-workbench；旧程序及其运行数据保持可回退。单个后台事件循环管理每账号唯一 Telethon 连接，SQLite 保存以内部 account_id 为边界的数据和任务。浏览器显式传递 account_id，不在服务端维护隐式当前账号；持续流推送状态。

**Tech Stack:** Python 3.14、Telethon 1.45、SQLite、原生 HTML/CSS/JS、本机 HTTP。

**Spec:** `docs/source/requirements.md`、`docs/source/design.md` 与 `docs/source/` 的四张虚构数据设计图。

## Global Constraints

- 首批支持 8 个账号；手机号、验证码、两步验证；独立会话、身份校验；不自动导入旧账号。
- 主导航：聊天、监听、联系人、群组、任务中心、账号管理、设置。1440×900 可操作；1280 收辅助栏。
- 所有修改操作显式绑定 account_id；群组、消息、联系人、草稿、任务和去重记录不串号。
- 默认人工聊天，AI 仅草稿；确认前不执行任务，不把凭据发送给 AI。
- 单人邀请失败继续；整体限流、断网、身份与权限失效暂停；未知结果先核实，不重复提交。
- 清空必须停止本账号监听、排空在途、锁联系人写入口、准确读取范围、原子备份并校验、名称与两勾选确认、最终快照复核；失败不假称成功。
- 冷启动不开始实际添加、不续删；不补历史；删除保留成功去重；平台限制不换号绕过。
- 不自动对真实第三方执行消息、邀请、删除或添加验收；真人账号由用户在本机输入验证码测试。

## Review Focus

- 快速切账号与旧 HTTP 响应：身份、草稿、选择和确认不能串号（UI 与集成测试）。
- 同秒旧消息、断线补发、服务消息与重复跨群用户：不能误添加（监听测试）。
- 进程在 RPC 中途结束：任务恢复为未核实，不能自动重复写（任务恢复测试）。
- 清理快照变化、未知未注册范围、备份损坏：禁止执行（清理测试）。
- 附件、AI 响应和 CSV 含恶意文本/路径：不能跨号读取文件、执行脚本或泄露凭据（服务与 HTTP 测试）。

## Task 1: 独立存储与账号授权

Files: storage.py, accounts.py, tests/test_storage.py, tests/test_accounts.py。
Interfaces: 见 ../../api-contract.md，Store 与 AccountManager 是其他模块唯一依赖。
- [x] 先写失败测试：八账号上限、重名不改变 ID、独立 session 路径、重复真实身份拒绝、重登身份不符不覆盖、验证码/密码不出现在快照、恢复不运行监听。
- [x] 实现 SQLite 文档存储与私有目录；账号创建、授权、恢复、取消、断开、注销会话与移除。
- [x] 跑上述测试红→绿，报告命令与结果。

## Task 2: 独立监听、配置版本与持久去重

Files: listeners.py, tests/test_listeners.py。
Consumes: app.accounts.require(), app.store, app.contacts.add_person(), app.tasks.contact_lock()。
- [x] 先写两账号同时实际/预览、配置 CAS、运行中保存生效边界、跨群去重、停止排空、重连不补发、删除后不重加的失败测试。
- [x] 实现版本化配置、运行批次、受控队列、暂停/停止/全局暂停、账户独立等待、统计和结构化动态。
- [x] 跑模块测试红→绿；原文不写动作日志。

## Task 3: 聊天、媒体、草稿与 AI

Files: chats.py, ai.py, tests/test_chats.py, tests/test_ai.py。
Consumes: app.accounts.require(), Store；Produces: 对话/消息读取、实时保存、手工发送与附件、受限 AI 草稿和任务草稿。
- [x] 先写账号+会话主键、切换草稿、幂等发送、未知结果禁止盲重试、附件路径隔离、AI 凭据排除与不自动执行的失败测试。
- [x] 实现本地搜索与按需历史、回复/编辑/撤回权限、文字/图片/文件上传下载进度、取消、手动接管与 AI 服务配置。
- [x] 跑模块测试红→绿，离线或结果未知显示可检查状态。

## Task 4: 联系人、任务中心、邀请与严格清理

Files: contacts.py, tasks.py, group_invites.py, tests/test_contacts.py, tests/test_tasks.py。
Consumes: Store、AccountManager；与 listeners 共享每账号联系人写入锁。
- [x] 先写全部联系人刷新、CSV 校验预览、任务确认绑定、逐人结果、暂停恢复、重启未知、数量守恒的失败测试。
- [x] 实现联系人增删/元数据/导入、持久任务项、明确选择消息/链接任务、结果导出。
- [x] 实现邀请前成员预检、五类结果、未知重核与合法失败重试。
- [x] 实现准确未注册联系人范围读取（takeout 限制如实显示）、快照备份摘要、名称+两勾选、最终范围复核、停止、独占锁和最终验证。
- [x] 跑模块测试红→绿；平台不可验证能力必须在界面阻止并说明，不降格伪造完成。

## Task 5: 完整多页界面

Files: web/index.html, web/app.js, web/styles.css, tests/ui.test.cjs。
Consumes: docs/api-contract.md；四张设计图与详细界面设计。
- [x] 先写按账号+会话草稿、未保存配置、旧响应防护、邀请选择/同意失效、清理非 Enter 提交的 UI 状态测试。
- [x] 实现七导航、持久账号栏、登录抽屉、监听控制台、聊天四区、联系人三页、群组与任务、设置。
- [x] 实现状态流更新、固定停止入口、导出、部分成功、错误等待等真实状态；不使用示例数字冒充数据。
- [x] Node 状态测试与真实浏览器 1440/1280 操作测试。

## Task 6: 后台集成、迁移、部署与两账号验收

Files: app.py, server.py, migration.py, launch.py, 启动.command, README.md, tests/test_app.py, tests/test_http.py, tests/test_migration.py。
- [x] 先写 action 显式账号、全局暂停、跨账号并发、loopback/token/origin、文件授权与流事件的失败测试。
- [x] 集成 services，单实例、私有数据、面板锁定、备份、日志保留/清缓存与退出任务边界。
- [x] 旧版仅用户主动选择后复制备份、验证格式/身份、预览确认后导入；不原地升级。
- [x] 全模块测试、语法/依赖检查、真实浏览器 E2E 与设计截图核对；独立最终审查，修复重要发现。
- [x] 部署独立 Mac 启动入口并打开真实登录页；已验证真实本机服务、应用 API 配置与页面无 JavaScript 异常。
- [x] 用户在本机登录 A、B，实际核对双账号授权、身份、会话文件和缓存隔离、界面切号；仅使用 运营测试账号 做真实云端只读检查。开发与登录可测的交付目标完成。草稿及小范围收发/预览的后续用户可控测试范围与未执行项目在 verification.md 明确保留，不冒充已执行。

## Evidence ledger

- 2026-10-22：已读完整需求 345 行、详细设计 164 行、四张设计图；旧程序 111 测试通过（1.499s）。
- 当前目录不是 Git repo；以独立新目录隔离，禁止修改旧 runtime。文档已由用户作为开发依据提供，按既有授权继续，不重复征求开发许可。
- 原 contacts.contacts.saved_count 不能推导为未注册号码数；需要 getSaved/takeout 的可验证范围或明确阻止完整清空。

- 2026-10-22 最终集成：314 项 Python 测试通过（6.540 秒），48 项 Node 测试通过；依赖、JS 语法、Python AST、Git 空白检查通过。全部使用临时数据与模拟客户端，没有对真实账号添加、删除、发送或邀请。
- 独立终审 6 项 P1、3 项 P2 均修复并复验；任务 outbox 归属、未知 RPC 不重放、面板锁后响应、成功登录关闭、手机号解析与预览身份变化均有回归。
- 浏览器基线 26 项通过：1440/1280 七页、切号/草稿/迟到响应/邀请确认/清理确认/手动任务/群权限/任务消息入口等；追加媒体修复另列验证，不混作同一次完整运行。
- 媒体定向浏览器追加 6/6 通过，pageerror/console 均零；文件在前后保持相同。切号/聊天/锁定阻止迟到附件，下载保留中文与扩展名；全部 API 和媒体均拦截，不访问或重启真实服务。
- 新环境及 Mac 应用已部署、打开；只复用原应用 API 配置，不自动导入旧账号会话。真实服务当前一个待登录账号槽位、零已授权账号，未见登录错误。真实双账号登录和平台小范围验收待用户本机完成。
- 完整验收矩阵见 ../../verification.md；旧程序、旧运行数据保留。分支保留在本机，不推送远端。
- 2026-10-22 用户完成两号本机登录，指定个人号谨慎、真实测试优先 运营测试账号。实际核验两号均在线、独立身份/私有 session/缓存归属；运营测试账号 会话、群及历史消息读取成功，抽样群无邀请权限有明确反馈。真实界面 5/5 通过，未对个人号主动新增云端读取、未写 Telegram、未重启。独立复核确认无阻止登录可测交付的硬性开发缺口；其余平台写入测试由用户可控场景继续，详见验收记录。
