# Aria2 下载管理

将一个 Aria2 JSON-RPC 实例作为 MoviePilot v2 自定义下载器使用，覆盖下载、任务列表、整理状态回写、启停、限速、Tracker、文件列表和安全删除等下载器闭环。

## 版本要求

- MoviePilot `>= 2.15.1, < 3`
- Aria2 开启 JSON-RPC；建议配置 `rpc-secret`
- 当前版本只支持绑定一个 Aria2 RPC 实例和一个 MoviePilot 自定义下载器配置

## 配置步骤

1. 在 MoviePilot 的“设置 → 下载器”中新建自定义下载器。
2. 自定义下载器类型填写 `aria2managerrehtt`，为配置取一个唯一名称并启用。
3. 如果 MoviePilot 与 Aria2 看到的下载目录不同，在该下载器配置中填写路径映射。
4. 打开本插件，填写 Aria2 RPC 地址和 Secret，并选择上一步的下载器名称。
5. 保存并启用插件；详情页显示 `connection: ok` 即表示接入成功。

Docker 路径映射示例：

```text
MoviePilot 路径：/downloads
Aria2 路径：     /data/downloads
```

MoviePilot 传入 `/downloads/TV` 时，插件会向 Aria2 提交 `/data/downloads/TV`；Aria2 返回文件路径时执行反向映射。

## 任务标识与升级

- BT 任务对 MoviePilot 暴露规范化后的 40 位小写 infoHash。
- Magnet 在提交时直接从 BTIH 生成 infoHash，不等待 Aria2 下载元数据。
- HTTP、FTP 等非 BT 任务使用稳定的 40 位代理 ID，避免把 16 位 Aria2 GID 传入只接受种子 Hash 的 MoviePilot 接口。
- 远程 `.torrent` 在 Aria2 生成 BT 子任务后，会从代理 ID 原子迁移到真实 infoHash。
- v1.2 的 GID 格式数据按需迁移；插件数据、下载历史、下载文件和整理历史在同一数据库事务中更新。迁移失败会回滚，不会留下半迁移状态。

Magnet 和远程链接无法在提交瞬间可靠取得文件列表，因此指定选集时会非阻塞添加并按全集下载；本地种子文件仍支持提交前暂停、选集后恢复。

## 任务范围

普通任务列表只返回由本插件创建并记录的任务，避免接管同一 Aria2 实例中的手工任务。MoviePilot 明确传入绑定的下载器名称且要求 `include_all_tags` 时，才会显示或控制外部任务。

分类和标签保存在插件任务元数据中。Aria2 原生没有与 qBittorrent 完全等价的分类/标签模型。

## 删除安全策略

当 MoviePilot 请求“同时删除文件”时，插件采用严格白名单：

- 只接受绝对路径；
- 路径必须位于 MoviePilot 已配置的本地下载根目录内；
- 拒绝下载根目录本身、目录递归删除、根目录外路径和符号链接逃逸；
- 先校验本次任务的全部候选路径，再调用 Aria2 删除任务；
- 只逐个删除 Aria2 返回的文件和对应 `.aria2` 控制文件；
- 最后仅清理下载根目录以内的空目录。

任何候选路径校验失败时，本次任务和文件都不会删除。若 Aria2 已删除任务但本地文件删除失败，任务记录会保留为待重试状态。

## 支持的 MoviePilot v2 下载器能力

- `download`
- `list_torrents`（含 `include_all_tags`）
- `transfer_completed`
- `remove_torrents`
- `set_torrents_tag`
- `start_torrents` / `stop_torrents`
- `torrent_files`
- `update_torrent`
- `get_torrent_trackers`
- `downloader_info`

`update_torrent` 支持下载/上传限速、BT Tracker、分类和 Aria2 的做种比率/时间选项。保存目录只允许在任务尚未写入数据且处于等待或暂停状态时修改，插件不会搬移已经下载的文件。

## 远程控制

插件提供以下命令：

```text
/aria2_status
/aria2_pause_all
/aria2_unpause_all
/aria2_purge_done
```

API：

- `GET /api/v1/plugin/Aria2ManagerRehtt/status`
- `POST /api/v1/plugin/Aria2ManagerRehtt/action?action=pause_all`

API 均要求 MoviePilot API Key。`action` 支持 `pause_all`、`unpause_all` 和 `purge_done`。

## 开发验证

仓库根目录执行：

```bash
python -m unittest discover -s tests -p 'test_aria2manager_*.py' -v
```

测试覆盖任务标识、配置绑定、RPC 鉴权与分页、旧数据事务迁移、父子 GID 跟踪、MoviePilot v2 方法签名、任务范围和文件删除边界。
