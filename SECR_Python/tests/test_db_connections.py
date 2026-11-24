"""EsgDb: per-process connection cap + waiting for the database to accept a connection."""
from __future__ import annotations

import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from secr.config import DbConfig  # noqa: E402
from secr.db import EsgDb  # noqa: E402


class TooMany(Exception):
    """Same shape as pymysql's OperationalError(1040, 'Too many connections')."""


class FakeConn:
    def __init__(self, server):
        self.server = server

    def close(self):
        with self.server.lock:
            self.server.open -= 1


class FakeServer:
    """A MySQL server that allows ``limit`` connections and refuses more with error 1040."""

    def __init__(self, limit, code=1040):
        self.limit, self.code = limit, code
        self.open = self.refused = 0
        self.lock = threading.Lock()

    def connect(self):
        with self.lock:
            if self.open >= self.limit:
                self.refused += 1
                raise TooMany(self.code, "Too many connections")
            self.open += 1
            return FakeConn(self)


def _db(server, retry=0.2, cap=20):
    db = EsgDb(DbConfig(host="fake", connect_retry_seconds=retry), cap)
    db._open = server.connect
    return db


def test_session_waits_until_the_database_accepts_a_connection():
    server = FakeServer(limit=2)
    db = _db(server, retry=30)  # long retry: the wake-up must come from our own connection closing
    holding, got = threading.Event(), []

    def hold():
        with db.session(), db.session():
            holding.set()
            time.sleep(0.5)

    t = threading.Thread(target=hold)
    t.start()
    holding.wait(2)
    started = time.monotonic()
    with db.session():
        got.append(time.monotonic() - started)
    t.join()
    assert server.refused >= 1
    assert 0.3 < got[0] < 5  # waited for the holder, then got in as soon as it closed
    assert server.open == 0


def test_session_waits_for_connections_closed_by_other_clients():
    server = FakeServer(limit=1)
    server.open = 1  # taken by another application
    db = _db(server, retry=0.1)

    def other_client_disconnects():
        time.sleep(0.4)
        with server.lock:
            server.open -= 1

    threading.Thread(target=other_client_disconnects).start()
    with db.session():
        pass
    assert server.refused >= 2


def test_per_user_limit_is_also_waited_for():
    server = FakeServer(limit=0, code=1203)
    db = _db(server, retry=0.1)
    threading.Timer(0.3, lambda: setattr(server, "limit", 1)).start()
    with db.session():
        pass
    assert server.refused >= 1


def test_try_session_never_waits():
    server = FakeServer(limit=1)
    db = _db(server, retry=30)
    with db.session():
        started = time.monotonic()
        with db.try_session() as extra:
            assert extra is None
        assert time.monotonic() - started < 1
    with db.try_session() as s:
        assert s is not None


def test_other_connect_errors_are_raised_not_waited_for():
    db = _db(FakeServer(limit=1))

    def broken():
        raise TooMany(2003, "Can't connect to MySQL server")
    db._open = broken
    try:
        with db.session():
            pass
    except TooMany as exc:
        assert exc.args[0] == 2003
    else:
        raise AssertionError("expected the connect error")


def test_process_never_opens_more_than_its_cap_and_waits_for_a_free_slot():
    server = FakeServer(limit=1000)  # the database has plenty of room
    db = _db(server, cap=20)
    peak, lock = [0], threading.Lock()

    def work():
        with db.session():
            with lock:
                peak[0] = max(peak[0], server.open)
            time.sleep(0.05)

    threads = [threading.Thread(target=work) for _ in range(60)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(10)
    assert peak[0] == 20  # 60 workers, never more than 20 connections at once
    assert server.refused == 0 and server.open == 0


def test_try_session_returns_none_when_the_process_cap_is_reached():
    server = FakeServer(limit=1000)
    db = _db(server, cap=2)
    with db.session(), db.session():
        with db.try_session() as extra:
            assert extra is None
    with db.try_session() as s:
        assert s is not None
    assert server.open == 0


def test_slot_is_given_back_when_connecting_fails():
    db = _db(FakeServer(limit=1), cap=1)

    def broken():
        raise TooMany(2003, "Can't connect to MySQL server")
    real, db._open = db._open, broken
    for _ in range(3):
        try:
            with db.session():
                pass
        except TooMany:
            pass
    db._open = real
    with db.session():  # the only slot is free again
        pass
