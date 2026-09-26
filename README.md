# task26092670

身份同步引擎核心：若干外部目录（连接器）与中心身份库（sqlite3）之间的双向属性同步。
纯 Python 3 标准库，无外部服务。

## 文件

- `directory.py` — 进程内外部目录。记录以 employeeId 为键；任何写字段/删记录
  从单调递增、永不复用的计数器领修订号。引擎只能 `list_all` /
  `list_changes(since)`（保留最近 N 条，更早游标报 `CursorExpired`）/
  `write_field(..., expected_rev)` 条件写。`external_write` / `external_delete`
  为外部管理员入口，线程安全，Sync 期间不被阻塞。
- `engine.py` — `SyncEngine(db_path, connectors, mappings, priorities)`，
  接口 `sync_full()` / `sync_delta()`（均返回 `(中心写入数, 连接器写入数)`）
  与 `get(employee_id)`。全部状态（游标、字段缓存、中心值）存 sqlite，
  引擎实例丢弃后重建行为不变。
- `test_sync.py` — `python3 -m unittest test_sync -v`

## 关键设计

- **回写识别**：引擎把自己每次写拿到的字段修订号持久化为 `engine_rev`；
  字段当前修订号等于它即视为引擎所留（无断言），否则是外部断言。
  不做值比较——外部写入与回写逐字节相同的值会领新修订号，仍算外部写入。
- **中心值** = 优先级列表中第一个有断言者的断言，都无则为空串。
- **出站**：有出站映射、记录存在、且非胜出来源的连接器，以缓存中的字段
  修订号为条件写回中心值（过出站变换链）；冲突则 `list_changes` 追赶后
  重算，绝不覆盖尚未读到的外部写入。
- **增量**：只按游标拉变更、只处理受影响身份，开销与变更条数成正比；
  游标过期自动退化为全量。引擎自己的写经游标回放时按修订号命中缓存，
  不会形成回环。
