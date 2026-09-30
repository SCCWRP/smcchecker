"""Fixtures for the sample-tracking-tool tests.

Nothing here touches a real database or the full smcchecker app. proj/__init__.py
pulls in arcgis, shapely, etc. and needs env vars, so we register an empty stand-in
for the `proj` package and import only proj/admin.py, then mount its blueprint on a
tiny Flask app whose g.eng is an in-memory FakeEngine.
"""
import os
import sys
import types
from contextlib import contextmanager

import pytest
from flask import Blueprint, Flask, g
from sqlalchemy.exc import IntegrityError

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROJ_DIR = os.path.join(ROOT, "proj")

if "proj" not in sys.modules:
    _pkg = types.ModuleType("proj")
    _pkg.__path__ = [PROJ_DIR]
    sys.modules["proj"] = _pkg

from proj import admin as admin_module  # noqa: E402


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return list(self._rows)

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _Row(tuple):
    """Tuple that also allows attribute access, like a SQLAlchemy row."""

    def __new__(cls, **kw):
        obj = super().__new__(cls, kw.values())
        obj.__dict__.update(kw)
        return obj


class FakeEngine:
    """In-memory stand-in for g.eng. Dispatches on the SQL text the app sends."""

    def __init__(self, stations=(), owners=None, tracker=None):
        self.stations = set(stations)
        self.owners = dict(owners or {})  # agencycode -> agencyname
        self.tracker = list(tracker or [])  # dicts: stationcode, participant, year, ...
        self.fail_with = None  # exception to raise on INSERT
        self.insert_attempts = 0

    def execute(self, clause, params=None):
        sql = str(clause)
        params = params or {}
        if "FROM sde.lu_stations" in sql:
            return _Result([(c,) for c in params["codes"] if c in self.stations])
        if "FROM sde.sample_tracker" in sql and "stationcode IN" in sql:
            hits = sorted(
                (r for r in self.tracker
                 if r["year"] == params["year"] and r["stationcode"] in params["codes"]),
                key=lambda r: r["stationcode"],
            )
            return _Result([_Row(stationcode=r["stationcode"], participant=r["participant"]) for r in hits])
        if "SELECT 1 FROM sde.lu_dataowner" in sql:
            return _Result([(1,)] if params["p"] in self.owners else [])
        if "SELECT agencyname FROM sde.lu_dataowner" in sql:
            return _Result([(self.owners[params["p"]],)] if params["p"] in self.owners else [])
        if "SELECT agencycode, agencyname FROM sde.lu_dataowner" in sql:
            return _Result([_Row(agencycode=k, agencyname=v) for k, v in self.owners.items()])
        raise AssertionError(f"FakeEngine: unexpected SQL: {sql}")

    @contextmanager
    def begin(self):
        pending = []

        class Conn:
            def execute(conn, clause, params=None):
                assert "INSERT INTO sde.sample_tracker" in str(clause)
                self.insert_attempts += 1
                if self.fail_with is not None:
                    raise self.fail_with
                pending.append(dict(params))
                return _Result([])

        yield Conn()  # an exception here skips the commit below (rollback)
        self.tracker.extend(pending)

    def dispose(self):
        pass


def make_integrity_error(msg):
    return IntegrityError("INSERT ...", {}, Exception(msg))


@pytest.fixture
def engine():
    return FakeEngine(
        stations={"401M02845", "402M00155", "403M00001", "A", "B"},
        owners={"SCCWRP": "Southern California Coastal Water Research Project", "LACFCD": "LA County Flood Control"},
    )


@pytest.fixture
def app(engine):
    app = Flask("sample_tracker_test", template_folder=os.path.join(PROJ_DIR, "templates"))
    app.secret_key = "test"
    app.config["TESTING"] = True
    app.register_blueprint(admin_module.admin)

    # The template links to scraper.lookuplists; stub just that endpoint so url_for resolves.
    stub = Blueprint("scraper", "scraper_stub")
    stub.add_url_rule("/lookuplists/<layer>/<action>", "lookuplists", lambda layer, action: "")
    app.register_blueprint(stub)

    @app.before_request
    def _set_eng():
        g.eng = engine

    return app


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.fixture
def authed(client):
    with client.session_transaction() as s:
        s["SAMPLE_TRACKER_AUTHORIZED"] = True
    return client
