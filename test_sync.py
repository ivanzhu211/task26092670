"""身份同步引擎测试。测试侧可记录写入者，引擎不行。"""
import os
import random
import shutil
import tempfile
import threading
import time
import unittest

from directory import Directory
from engine import ATTRIBUTES, FieldMapping, SyncEngine, apply_chain

CONNS = ("hr", "ldap", "crm", "ad")

MAPPINGS = {
    "hr": [
        FieldMapping("name", "displayName", "both",
                     inbound=("trim",), outbound=("trim",)),
        FieldMapping("mail", "email", "both",
                     inbound=("trim",), outbound=("trim",)),
    ],
    "ldap": [
        FieldMapping("cn", "displayName", "both"),
        FieldMapping("dept", "department", "both",
                     outbound=(("default", "unknown"),)),
    ],
    "crm": [
        FieldMapping("title", "jobTitle", "both",
                     inbound=("trim", "upper")),
        # crm 不在 displayName 优先级列表里 -> 永无断言
        FieldMapping("fullname", "displayName", "in"),
    ],
    "ad": [
        FieldMapping("email", "email", "both"),
        FieldMapping("dn", "displayName", "in"),
    ],
}

PRIORITIES = {
    "displayName": ["hr", "ad", "ldap"],
    "email": ["hr", "ad"],
    "department": ["ldap", "hr"],
    "jobTitle": ["crm", "hr"],
}

MAPPED_FIELDS = {c: [m.field for m in ms] for c, ms in MAPPINGS.items()}
INBOUND = {}
OUTBOUND = {}
for _c, _ms in MAPPINGS.items():
    for _m in _ms:
        if _m.direction in ("in", "both") and (_c, _m.attribute) not in INBOUND:
            INBOUND[(_c, _m.attribute)] = _m
        if _m.direction in ("out", "both"):
            OUTBOUND.setdefault(_c, []).append(_m)

VALUES = ["", " ", "Alice", "bob", "Carol X", "  padded  ", "x@y.z",
          "ENGR", "mgr", "Sales", "  ", "d@e.f"]


class Model:
    """测试侧世界模型：记录每个字段最后写入者，按语义独立计算期望值。"""

    def __init__(self, retention=8):
        self.dirs = {c: Directory(c, change_retention=retention) for c in CONNS}
        self.writers = {}          # (conn, emp, field) -> "external" | "engine"
        self.exists = set()        # (conn, emp)
        self.lock = threading.Lock()
        for name, directory in self.dirs.items():
            orig = directory.write_field

            def make_wrapper(conn_name, orig_write):
                def wrapper(emp, field, value, expected_rev):
                    rev = orig_write(emp, field, value, expected_rev)
                    with self.lock:
                        self.writers[(conn_name, emp, field)] = "engine"
                    return rev
                return wrapper

            directory.write_field = make_wrapper(name, orig)

    # ---- 外部行为 ----
    def external_write(self, conn, emp, field, value):
        with self.lock:
            self.dirs[conn].external_write(emp, field, value)
            self.writers[(conn, emp, field)] = "external"
            self.exists.add((conn, emp))

    def external_delete(self, conn, emp):
        with self.lock:
            self.dirs[conn].external_delete(emp)
            self.exists.discard((conn, emp))
            for key in [k for k in self.writers if k[0] == conn and k[1] == emp]:
                del self.writers[key]

    # ---- 期望计算 ----
    def expected_assertion(self, conn, emp, attr):
        mapping = INBOUND.get((conn, attr))
        if mapping is None:
            return None
        if conn not in set(PRIORITIES.get(attr, [])):
            return None
        record = self.dirs[conn].debug_record(emp)
        if record is None:
            return None
        if self.writers.get((conn, emp, mapping.field)) == "engine":
            return None
        value = apply_chain(mapping.inbound, record.get(mapping.field, ""))
        return value if value != "" else None

    def expected_central(self, emp):
        values = {}
        winners = {}
        for attr in ATTRIBUTES:
            value, winner = "", None
            for conn in PRIORITIES.get(attr, []):
                assertion = self.expected_assertion(conn, emp, attr)
                if assertion is not None:
                    value, winner = assertion, conn
                    break
            values[attr] = value
            winners[attr] = winner
        return values, winners


def make_engine(model, db_path):
    return SyncEngine(db_path, dict(model.dirs), MAPPINGS, PRIORITIES)


def check_world(test, model, engine, employees):
    """逐字段比对：中心值、连接器出站字段、记录存在性。"""
    for emp in employees:
        values, winners = model.expected_central(emp)
        test.assertEqual(engine.get(emp), values, f"central {emp}")
        for conn in CONNS:
            actual = model.dirs[conn].debug_record(emp)
            for m in OUTBOUND.get(conn, []):
                if actual is None or winners[m.attribute] == conn:
                    continue
                expected = apply_chain(m.outbound, values[m.attribute])
                test.assertEqual(actual.get(m.field, ""), expected,
                                 f"outbound {conn}/{emp}/{m.field}")
    for conn in CONNS:
        expected_emps = sorted(e for (c, e) in model.exists if c == conn)
        test.assertEqual(model.dirs[conn].debug_employees(), expected_emps,
                         f"existence {conn}")


def apply_op(models, op, rng):
    """把同一外部操作施加到所有世界。"""
    kind = op[0]
    if kind == "write":
        _tag, conn, emp, field, value = op
        for model in models:
            model.external_write(conn, emp, field, value)
    elif kind == "delete":
        _tag, conn, emp = op
        for model in models:
            model.external_delete(conn, emp)
    elif kind == "echo":
        # 找 delta 世界模型里引擎写过的字段，外部写入逐字节相同的值
        candidates = [k for k, w in models[0].writers.items() if w == "engine"]
        if not candidates:
            return
        conn, emp, field = rng.choice(candidates)
        record = models[0].dirs[conn].debug_record(emp)
        if record is None:
            return
        value = record.get(field, "")
        for model in models:
            model.external_write(conn, emp, field, value)
    elif kind == "expire":
        # 用超过保留窗口的写入冲掉游标
        _tag, conn, retention = op
        for i in range(retention + 4):
            emp = f"e{rng.randrange(1, 40)}"
            field = rng.choice(MAPPED_FIELDS[conn] + ["notes"])
            for model in models:
                model.external_write(conn, emp, field, f"exp{i}")


def gen_ops(rng, employees, retention, count):
    ops = []
    for _ in range(count):
        r = rng.random()
        if r < 0.55:
            conn = rng.choice(CONNS)
            emp = rng.choice(employees)
            field = rng.choice(MAPPED_FIELDS[conn] + ["notes"])
            ops.append(("write", conn, emp, field, rng.choice(VALUES)))
        elif r < 0.70:
            ops.append(("delete", rng.choice(CONNS), rng.choice(employees)))
        elif r < 0.80:
            ops.append(("echo",))
        elif r < 0.90:
            ops.append(("expire", rng.choice(CONNS), retention))
        else:
            # 删后重建
            conn = rng.choice(CONNS)
            emp = rng.choice(employees)
            ops.append(("delete", conn, emp))
            ops.append(("write", conn, emp,
                        rng.choice(MAPPED_FIELDS[conn]), "rebuilt"))
    return ops


class TestRandomWorlds(unittest.TestCase):
    """同一随机外部写入历史：一套只跑增量、一套只跑全量，逐字段对拍。"""

    def test_delta_vs_full(self):
        for seed in (1, 2, 3):
            with self.subTest(seed=seed):
                self._run_seed(seed)

    def _run_seed(self, seed):
        rng = random.Random(seed)
        retention = 8
        employees = [f"e{i}" for i in range(1, 31)]
        tmp = tempfile.mkdtemp()
        try:
            model_delta = Model(retention)
            model_full = Model(retention)
            eng_delta = make_engine(model_delta, os.path.join(tmp, "d.db"))
            eng_full = make_engine(model_full, os.path.join(tmp, "f.db"))
            models = [model_delta, model_full]

            # 初始外部数据
            for op in gen_ops(rng, employees, retention, 40):
                apply_op(models, op, rng)

            for round_no in range(20):
                for op in gen_ops(rng, employees, retention,
                                  rng.randrange(3, 10)):
                    apply_op(models, op, rng)
                # 随机丢弃引擎实例、用同一库文件重建（要求 4）
                if rng.random() < 0.25:
                    eng_delta.close()
                    eng_delta = make_engine(
                        model_delta, os.path.join(tmp, "d.db"))
                if rng.random() < 0.25:
                    eng_full.close()
                    eng_full = make_engine(
                        model_full, os.path.join(tmp, "f.db"))

                eng_delta.sync_delta()
                eng_full.sync_full()

                check_world(self, model_delta, eng_delta, employees)
                check_world(self, model_full, eng_full, employees)
                # 两套世界逐字段相同（要求 2）
                for emp in employees:
                    self.assertEqual(eng_delta.get(emp), eng_full.get(emp))
                    for conn in CONNS:
                        self.assertEqual(
                            model_delta.dirs[conn].debug_record(emp),
                            model_full.dirs[conn].debug_record(emp),
                            f"dir {conn}/{emp} round {round_no}")
                # 紧接的第二次 Sync 零写入（要求 1）
                self.assertEqual(eng_delta.sync_delta(), (0, 0),
                                 f"delta not idempotent round {round_no}")
                self.assertEqual(eng_full.sync_full(), (0, 0),
                                 f"full not idempotent round {round_no}")
            eng_delta.close()
            eng_full.close()
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


class TestConcurrentWrites(unittest.TestCase):
    """另一线程在多轮增量 Sync 期间持续外部写入，停止后结果须等于期望。"""

    def test_concurrent_external_writes(self):
        rng = random.Random(99)
        employees = [f"e{i}" for i in range(1, 21)]
        tmp = tempfile.mkdtemp()
        try:
            model = Model(retention=64)
            engine = make_engine(model, os.path.join(tmp, "c.db"))
            for op in gen_ops(rng, employees, 64, 30):
                apply_op([model], op, rng)
            engine.sync_full()

            stop = threading.Event()
            errors = []

            def writer():
                wrng = random.Random(1234)
                try:
                    while not stop.is_set():
                        conn = wrng.choice(CONNS)
                        emp = wrng.choice(employees)
                        field = wrng.choice(MAPPED_FIELDS[conn] + ["notes"])
                        if wrng.random() < 0.1:
                            model.external_delete(conn, emp)
                        else:
                            model.external_write(
                                conn, emp, field, wrng.choice(VALUES))
                        time.sleep(0.0002)
                except Exception as exc:  # noqa: BLE001
                    errors.append(exc)

            thread = threading.Thread(target=writer)
            thread.start()
            try:
                for _ in range(25):
                    engine.sync_delta()
            finally:
                stop.set()
                thread.join()
            self.assertEqual(errors, [])

            engine.sync_delta()
            check_world(self, model, engine, employees)
            self.assertEqual(engine.sync_delta(), (0, 0))
            engine.close()
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


class TestRaceBetweenReadAndWrite(unittest.TestCase):
    """外部写入确定性地插在 Sync 读到某字段之后、写回该字段之前。

    该写入若被看到就会胜出（hr 是 displayName 最高优先级）。
    全量、增量各跑一遍，下一次 Sync 后中心值须为它。
    """

    def _prepare(self, tmp):
        model = Model(retention=16)
        engine = make_engine(model, os.path.join(tmp, "r.db"))
        # hr 记录存在但 name 为空（无断言）；ldap 提供 displayName
        model.external_write("hr", "e1", "name", "")
        model.external_write("ldap", "e1", "cn", "Bob")
        engine.sync_full()
        # 引擎已把 "Bob" 写回 hr.name（引擎所写，无断言）
        self.assertEqual(model.writers.get(("hr", "e1", "name")), "engine")
        # 中心推进到 "Carol"，引擎下一轮将尝试把 "Carol" 写回 hr.name
        model.external_write("ldap", "e1", "cn", "Carol")
        return model, engine

    def _install_hook(self, model):
        fired = []

        def hook(_kind):
            model.dirs["hr"].after_read_hook = None
            fired.append(True)
            # 插在引擎读完 hr 之后、写回 hr.name 之前
            model.external_write("hr", "e1", "name", "Winner")

        model.dirs["hr"].after_read_hook = hook
        return fired

    def _run(self, tmp, sync):
        model, engine = self._prepare(tmp)
        fired = self._install_hook(model)
        sync(engine)                      # 竞态发生的这一轮
        sync(engine)                      # 下一次 Sync
        self.assertTrue(fired)
        self.assertEqual(engine.get("e1")["displayName"], "Winner")
        check_world(self, model, engine, ["e1"])
        self.assertEqual(sync(engine), (0, 0))
        engine.close()

    def test_race_full(self):
        tmp = tempfile.mkdtemp()
        try:
            self._run(tmp, lambda e: e.sync_full())
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_race_delta(self):
        tmp = tempfile.mkdtemp()
        try:
            self._run(tmp, lambda e: e.sync_delta())
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


class TestPerformance(unittest.TestCase):
    """10,000 身份、4 个连接器的全量 Sync 须在 30 秒内完成。"""

    def test_full_sync_10k(self):
        n = 10000
        employees = [f"e{i}" for i in range(1, n + 1)]
        tmp = tempfile.mkdtemp()
        try:
            model = Model(retention=1000)
            rng = random.Random(7)
            for emp in employees:
                model.external_write("hr", emp, "name", f"Name {emp}")
                model.external_write("hr", emp, "mail", f"{emp}@x.com")
                model.external_write("ldap", emp, "cn", f"  L{emp} ")
                model.external_write("ldap", emp, "dept",
                                     rng.choice(["Eng", "Sales", ""]))
                model.external_write("crm", emp, "title",
                                     rng.choice(["mgr", "dev", ""]))
                model.external_write("ad", emp, "email", f"{emp}@corp")
            engine = make_engine(model, os.path.join(tmp, "p.db"))
            start = time.monotonic()
            engine.sync_full()
            elapsed = time.monotonic() - start
            self.assertLess(elapsed, 30.0,
                            f"full sync took {elapsed:.1f}s")
            # 抽样校验语义 + 幂等
            for emp in rng.sample(employees, 50):
                values, _winners = model.expected_central(emp)
                self.assertEqual(engine.get(emp), values)
            self.assertEqual(engine.sync_full(), (0, 0))
            self.assertEqual(engine.sync_delta(), (0, 0))
            engine.close()
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
