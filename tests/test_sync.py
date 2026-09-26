import os
import random
import sys
import tempfile
import threading
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from connector import Directory
from engine import SyncEngine
from mapping import ATTRS, FieldMapping, apply_chain

VALUES = ["", "Ada", "Bob", "  padded  ", "ada@x", "bob@y", "Eng",
          "Sales", "Dev", "Mgr", "X", "ada"]


def make_dirs(changelog_size=200):
    return {
        "hr": Directory("hr", ["name", "mail", "dept", "title"], changelog_size),
        "ldap": Directory("ldap", ["cn", "mail", "ou", "title"], changelog_size),
        "crm": Directory("crm", ["fullname", "email", "dept", "job"], changelog_size),
        "portal": Directory("portal", ["dn", "email", "department", "jt"], changelog_size),
    }


MAPPINGS = {
    "hr": {
        "name": FieldMapping("displayName", inbound=["trim"], outbound=["trim"]),
        "mail": FieldMapping("email", inbound=[], outbound=[]),
        "dept": FieldMapping("department", inbound=[], outbound=[]),
        "title": FieldMapping("jobTitle", inbound=[], outbound=None),
    },
    "ldap": {
        "cn": FieldMapping("displayName", inbound=[], outbound=[]),
        "mail": FieldMapping("email", inbound=[], outbound=[]),
        "ou": FieldMapping("department", inbound=None, outbound=["upper"]),
        "title": FieldMapping("jobTitle", inbound=[], outbound=[]),
    },
    "crm": {
        "fullname": FieldMapping("displayName", inbound=["trim"], outbound=["trim"]),
        "email": FieldMapping("email", inbound=[], outbound=[]),
        "dept": FieldMapping("department", inbound=[], outbound=[]),
        "job": FieldMapping("jobTitle", inbound=[], outbound=None),
    },
    "portal": {
        "dn": FieldMapping("displayName", inbound=None, outbound=[("default", "?")]),
        "email": FieldMapping("email", inbound=[], outbound=[]),
        "department": FieldMapping("department", inbound=[], outbound=[]),
        "jt": FieldMapping("jobTitle", inbound=[], outbound=[]),
    },
}

PRIORITIES = {
    "displayName": ["hr", "crm", "ldap"],
    "email": ["crm", "hr", "ldap", "portal"],
    "department": ["hr", "crm", "portal"],
    "jobTitle": ["ldap", "crm", "hr", "portal"],
}


class Checker:
    """Test-side oracle. Unlike the engine it knows who wrote every field:
    engine writes are observed by wrapping Directory.write_field."""

    def __init__(self, dirs):
        self.dirs = dirs
        self.engine_revs = {name: set() for name in dirs}
        self.engine_values = {}  # (conn, emp, field) -> last engine value
        for name, d in dirs.items():
            orig = d.write_field
            def wrap(orig=orig, name=name):
                def write_field(emp, field, value, expected_rev):
                    rev = orig(emp, field, value, expected_rev)
                    if rev is not None:
                        self.engine_revs[name].add(rev)
                        self.engine_values[(name, emp, field)] = value
                    return rev
                return write_field
            d.write_field = wrap()

    def _assertion(self, conn, emp, attr, snaps):
        rec = snaps[conn].get(emp)
        if rec is None:
            return None
        for field, fm in MAPPINGS[conn].items():
            if fm.attr != attr or fm.inbound is None:
                continue
            val, rev = rec.get(field, ("", 0))
            if rev in self.engine_revs[conn]:
                return None
            v = apply_chain(val, fm.inbound)
            return v if v != "" else None
        return None

    def check(self, engine, emps):
        snaps = {name: d.list_all()[0] for name, d in self.dirs.items()}
        for emp in emps:
            expected, winners = {}, {}
            for attr in ATTRS:
                value, winner = "", None
                for conn in PRIORITIES[attr]:
                    a = self._assertion(conn, emp, attr, snaps)
                    if a is not None:
                        value, winner = a, conn
                        break
                expected[attr] = value
                winners[attr] = winner
            got = engine.get(emp)
            assert got == expected, f"central {emp}: got {got} want {expected}"
            for conn, fmap in MAPPINGS.items():
                rec = snaps[conn].get(emp)
                if rec is None:
                    continue
                for field, fm in fmap.items():
                    if fm.outbound is None or winners[fm.attr] == conn:
                        continue
                    want = apply_chain(expected[fm.attr], fm.outbound)
                    actual = rec.get(field, ("", 0))[0]
                    assert actual == want, (
                        f"{conn}.{emp}.{field}: got {actual!r} want {want!r}")


def snapshot_values(dirs):
    return {name: {emp: {f: v for f, (v, _r) in rec.items()}
                   for emp, rec in d.list_all()[0].items()}
            for name, d in dirs.items()}


class SyncTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def db_path(self, tag):
        return os.path.join(self.tmp.name, tag + ".sqlite")

    def make_engine(self, tag, dirs, mappings=MAPPINGS, priorities=PRIORITIES):
        engine = SyncEngine(self.db_path(tag), dirs, mappings, priorities)
        self.addCleanup(engine.close)
        return engine


class RandomWorldsTest(SyncTestBase):
    """Same external write history applied to two identical worlds; one only
    runs sync_full, the other only sync_delta. States must match field by
    field and match the independent oracle after every round."""

    def test_full_vs_delta(self):
        seed = 20260927
        dirs_a, dirs_b = make_dirs(), make_dirs()
        eng_a = self.make_engine("a", dirs_a)
        eng_b = self.make_engine("b", dirs_b)
        chk_a, chk_b = Checker(dirs_a), Checker(dirs_b)
        emps = ["E%03d" % i for i in range(1, 26)]
        rng = random.Random(seed)
        conns = sorted(MAPPINGS)

        def gen_ops(n):
            ops = []
            for _ in range(n):
                k = rng.random()
                emp = rng.choice(emps)
                conn = rng.choice(conns)
                field = rng.choice(sorted(MAPPINGS[conn]))
                if k < 0.50:
                    ops.append(("write", conn, emp, field, rng.choice(VALUES)))
                elif k < 0.62:
                    ops.append(("delete", conn, emp))
                elif k < 0.74:  # delete then recreate with one field
                    ops.append(("recreate", conn, emp, field, rng.choice(VALUES)))
                elif k < 0.86:  # external rewrite of the engine's exact value
                    ops.append(("echo", conn, emp, field))
                else:  # flood one connector to expire the delta cursor
                    ops.append(("churn", conn))
            return ops

        def apply(dirs, chk, ops):
            for op in ops:
                if op[0] == "write":
                    _, conn, emp, field, val = op
                    dirs[conn].admin_write(emp, field, val)
                elif op[0] == "delete":
                    dirs[op[1]].admin_delete(op[2])
                elif op[0] == "recreate":
                    _, conn, emp, field, val = op
                    dirs[conn].admin_delete(emp)
                    dirs[conn].admin_write(emp, field, val)
                elif op[0] == "echo":
                    _, conn, emp, field = op
                    val = chk.engine_values.get((conn, emp, field))
                    if val is not None:
                        dirs[conn].admin_write(emp, field, val)
                elif op[0] == "churn":
                    field = sorted(MAPPINGS[op[1]])[0]
                    for i in range(250):  # changelog_size is 200
                        dirs[op[1]].admin_write("ECHURN", field, "churn%d" % i)

        all_emps = set(emps) | {"ECHURN"}
        for rnd in range(12):
            ops = gen_ops(rng.randint(5, 40))
            apply(dirs_a, chk_a, ops)
            apply(dirs_b, chk_b, ops)
            if rnd % 4 == 3:  # drop engine instances, rebuild on same DB files
                eng_a = self.make_engine("a", dirs_a)
                eng_b = self.make_engine("b", dirs_b)
            eng_a.sync_full()
            eng_b.sync_delta()
            for emp in all_emps:
                self.assertEqual(eng_a.get(emp), eng_b.get(emp),
                                 f"round {rnd} emp {emp}")
            self.assertEqual(snapshot_values(dirs_a), snapshot_values(dirs_b),
                             f"round {rnd} connector state")
            chk_a.check(eng_a, all_emps)
            chk_b.check(eng_b, all_emps)
            self.assertEqual(eng_a.sync_full(), (0, 0), f"round {rnd} full not stable")
            self.assertEqual(eng_b.sync_delta(), (0, 0), f"round {rnd} delta not stable")


class ConcurrentWritesTest(SyncTestBase):
    def test_writer_thread_during_delta_syncs(self):
        dirs = make_dirs()
        engine = self.make_engine("c", dirs)
        chk = Checker(dirs)
        emps = ["E%03d" % i for i in range(1, 16)]
        stop = threading.Event()
        errors = []

        def writer():
            rng = random.Random(7)
            conns = sorted(MAPPINGS)
            try:
                while not stop.is_set():
                    conn = rng.choice(conns)
                    emp = rng.choice(emps)
                    field = rng.choice(sorted(MAPPINGS[conn]))
                    k = rng.random()
                    if k < 0.8:
                        dirs[conn].admin_write(emp, field, rng.choice(VALUES))
                    elif k < 0.9:
                        dirs[conn].admin_delete(emp)
                    else:
                        dirs[conn].admin_delete(emp)
                        dirs[conn].admin_write(emp, field, rng.choice(VALUES))
                    time.sleep(0.0002)
            except Exception as e:  # pragma: no cover
                errors.append(e)

        t = threading.Thread(target=writer)
        t.start()
        for _ in range(40):
            engine.sync_delta()
        stop.set()
        t.join()
        self.assertFalse(errors)
        engine.sync_delta()
        chk.check(engine, emps)
        self.assertEqual(engine.sync_delta(), (0, 0))


class InterleavingTest(SyncTestBase):
    """An external write lands deterministically between the engine's read
    of a field and its conditional write-back, and would win if seen."""

    def _case(self, sync_kind):
        c1 = Directory("c1", ["name"], changelog_size=100)
        c2 = Directory("c2", ["name"], changelog_size=100)
        mappings = {
            "c1": {"name": FieldMapping("displayName", inbound=[], outbound=[])},
            "c2": {"name": FieldMapping("displayName", inbound=[], outbound=[])},
        }
        priorities = {"displayName": ["c2", "c1"]}
        c1.admin_write("E1", "name", "A")
        c2.admin_write("E1", "name", "")
        engine = self.make_engine("i" + sync_kind, {"c1": c1, "c2": c2},
                                  mappings, priorities)
        orig = c2.write_field
        injected = []

        def write_field(emp, field, value, expected_rev):
            if not injected:
                injected.append(True)
                c2.admin_write(emp, field, "B")  # external write post-read
            return orig(emp, field, value, expected_rev)

        c2.write_field = write_field
        sync = engine.sync_full if sync_kind == "full" else engine.sync_delta
        sync()
        snap, _ = c2.list_all()
        self.assertEqual(snap["E1"]["name"][0], "B",
                         "engine must not overwrite the unread external write")
        sync()
        self.assertEqual(engine.get("E1")["displayName"], "B")
        self.assertEqual(sync(), (0, 0))

    def test_interleaved_write_full(self):
        self._case("full")

    def test_interleaved_write_delta(self):
        self._case("delta")


class PerformanceTest(SyncTestBase):
    def test_full_sync_10k_identities_4_connectors(self):
        dirs = make_dirs(changelog_size=100)
        n = 10000
        for i in range(n):
            emp = "E%05d" % i
            dirs["hr"].admin_write(emp, "name", "Person %d" % i)
            dirs["hr"].admin_write(emp, "mail", "p%d@x" % i)
            dirs["hr"].admin_write(emp, "dept", "Dept%d" % (i % 20))
            dirs["ldap"].admin_write(emp, "cn", "p%d" % i)
            dirs["ldap"].admin_write(emp, "title", "Title%d" % (i % 50))
            dirs["crm"].admin_write(emp, "email", "p%d@crm" % i)
            dirs["portal"].admin_write(emp, "department", "Dept%d" % (i % 20))
        engine = self.make_engine("p", dirs)
        start = time.monotonic()
        engine.sync_full()
        elapsed = time.monotonic() - start
        self.assertLess(elapsed, 30.0, f"full sync took {elapsed:.1f}s")
        self.assertEqual(engine.sync_full(), (0, 0))
        self.assertEqual(engine.sync_delta(), (0, 0))
        g = engine.get("E00000")
        self.assertEqual(g["displayName"], "Person 0")
        self.assertEqual(g["email"], "p0@crm")  # crm outranks hr for email


if __name__ == "__main__":
    unittest.main()
