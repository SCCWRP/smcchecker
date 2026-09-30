"""Tests for the validation checks in the sample tracking tool (proj/admin.py).

Run from the repo root:  python -m pytest tests
"""
from urllib.parse import parse_qs, urlparse

import pytest

from conftest import make_integrity_error
from proj.admin import SAMPLE_TRACKER_ERROR_SEP as SEP

GOOD = {
    "participant": "SCCWRP",
    "year": "2027",
    "stationcode": "401M02845",
    "purpose": ["status_and_trend"],
    "effortequivalent": "1",
    "workplan": "SMC_2027_2031_v1",
    "details": "",
    "login_email": "a@b.org",
}


def submit(client, **over):
    data = {**GOOD, **over}
    data = {k: v for k, v in data.items() if v is not None}
    return client.post("/sample-tracking-tool/submit", data=data)


def errors_of(resp):
    assert resp.status_code == 302
    q = parse_qs(urlparse(resp.headers["Location"]).query)
    assert "error" in q, f"expected an error redirect, got {resp.headers['Location']}"
    return q["error"][0].split(SEP)


def assert_no_insert(engine):
    assert engine.tracker == []


# ---------- password gate ----------

@pytest.mark.parametrize("method,path", [
    ("get", "/sample-tracking-tool"),
    ("post", "/sample-tracking-tool/submit"),
    ("get", "/sample-tracking-tool/export"),
])
def test_unauthenticated_redirects_to_login(client, engine, method, path):
    resp = getattr(client, method)(path, data=GOOD) if method == "post" else client.get(path)
    assert resp.status_code == 302
    assert resp.headers["Location"].endswith("/sample-tracking-tool/login")
    assert_no_insert(engine)


def test_login_wrong_password_does_not_authorize(client):
    resp = client.post("/sample-tracking-tool/login", data={"password": "nope"})
    assert resp.status_code == 200 and b"Incorrect password." in resp.data
    assert client.get("/sample-tracking-tool").status_code == 302


def test_login_right_password_authorizes(client):
    resp = client.post("/sample-tracking-tool/login", data={"password": "sccwrp"})
    assert resp.status_code == 302 and resp.headers["Location"].endswith("/sample-tracking-tool")
    assert client.get("/sample-tracking-tool").status_code == 200


# ---------- happy path / success message ----------

def test_success_inserts_sets_message_shows_once_and_clears_form(authed, engine):
    resp = submit(authed, stationcode=" 401M02845 , 402M00155 ,", purpose=["status_and_trend", "targeted"])
    assert resp.status_code == 302 and "error" not in resp.headers["Location"]
    assert [r["stationcode"] for r in engine.tracker] == ["401M02845", "402M00155"]
    assert all(r["targeted"] and r["status_and_trend"] and not r["restoration"] for r in engine.tracker)
    with authed.session_transaction() as s:
        assert "Saved 2 station(s)" in s["SAMPLE_TRACKER_SUCCESS"]
        assert "401M02845, 402M00155" in s["SAMPLE_TRACKER_SUCCESS"]
        assert "2027" in s["SAMPLE_TRACKER_SUCCESS"]
        assert "SAMPLE_TRACKER_FORM" not in s
    first = authed.get("/sample-tracking-tool")
    assert b"Saved 2 station(s) for Southern California Coastal Water Research Project, 2027" in first.data
    assert b'value="401M02845' not in first.data  # form cleared
    second = authed.get("/sample-tracking-tool")
    assert b"Saved 2 station(s)" not in second.data


def test_defaults_workplan_and_effort(authed, engine):
    submit(authed, workplan="", effortequivalent="")
    row = engine.tracker[0]
    assert row["workplan"] == "SMC_2027_2031_v1" and row["effort"] == 1.0 and row["details"] is None


# ---------- required fields ----------

def test_all_required_missing(authed, engine):
    errs = errors_of(submit(authed, participant="", year="", stationcode="", purpose=None))
    assert "StationCode(s) is required." in errs
    assert "Participant is required." in errs
    assert "Year is required." in errs
    assert "At least one Purpose is required." in errs
    assert_no_insert(engine)


@pytest.mark.parametrize("field,over,msg", [
    ("participant", {"participant": ""}, "Participant is required."),
    ("year", {"year": ""}, "Year is required."),
    ("stationcode", {"stationcode": ""}, "StationCode(s) is required."),
    ("stationcode-commas-only", {"stationcode": " , ,"}, "StationCode(s) is required."),
    ("purpose", {"purpose": None}, "At least one Purpose is required."),
    ("purpose-bogus", {"purpose": ["bogus"]}, "At least one Purpose is required."),
])
def test_each_required_field(authed, engine, field, over, msg):
    assert msg in errors_of(submit(authed, **over))
    assert_no_insert(engine)


def test_year_must_be_integer(authed, engine):
    assert "Year must be an integer." in errors_of(submit(authed, year="20x7"))
    assert_no_insert(engine)


# ---------- stationcode parsing ----------

def test_stationcodes_split_on_commas_and_trimmed(authed, engine):
    submit(authed, stationcode="  401M02845 ,402M00155,, 403M00001  ")
    assert [r["stationcode"] for r in engine.tracker] == ["401M02845", "402M00155", "403M00001"]


def test_duplicate_stationcodes_in_one_submission_rejected(authed, engine):
    errs = errors_of(submit(authed, stationcode="401M02845, 402M00155, 401M02845"))
    assert any(e.startswith("Duplicate StationCode(s) in the list: 401M02845") for e in errs)
    assert_no_insert(engine)


def test_unknown_stationcode_names_bad_codes_and_hint(authed, engine):
    errs = errors_of(submit(authed, stationcode="401M02845, NOPE1, NOPE2"))
    msg = next(e for e in errs if e.startswith("Unknown StationCode(s)"))
    assert "NOPE1, NOPE2" in msg and "401M02845" not in msg
    assert "separate them by commas" in msg
    assert_no_insert(engine)


def test_space_separated_input_is_one_code(authed, engine):
    # "A" and "B" both exist, but "A B" is a single (unknown) code.
    errs = errors_of(submit(authed, stationcode="A B"))
    msg = next(e for e in errs if e.startswith("Unknown StationCode(s)"))
    assert "A B" in msg and "separate them by commas" in msg
    assert_no_insert(engine)


# ---------- participant ----------

def test_unknown_participant(authed, engine):
    errs = errors_of(submit(authed, participant="NOBODY"))
    assert any("Unknown Participant 'NOBODY'" in e and "lu_dataowner" in e for e in errs)
    assert_no_insert(engine)


# ---------- station already logged for the year ----------

def test_station_already_logged_same_year_names_station_and_logger(authed, engine):
    engine.tracker.append({"stationcode": "401M02845", "participant": "LACFCD", "year": 2027})
    errs = errors_of(submit(authed))
    msg = next(e for e in errs if e.startswith("Already logged for 2027"))
    assert "401M02845 (by LACFCD)" in msg
    assert len(engine.tracker) == 1


def test_same_station_different_year_is_fine(authed, engine):
    engine.tracker.append({"stationcode": "401M02845", "participant": "LACFCD", "year": 2028})
    resp = submit(authed)
    assert "error" not in resp.headers["Location"]
    assert len(engine.tracker) == 2


def test_partial_failure_inserts_nothing(authed, engine):
    engine.tracker.append({"stationcode": "402M00155", "participant": "LACFCD", "year": 2027})
    errs = errors_of(submit(authed, stationcode="401M02845, 402M00155, 403M00001"))
    assert any("402M00155 (by LACFCD)" in e for e in errs)
    assert len(engine.tracker) == 1  # only the pre-existing row
    assert engine.insert_attempts == 0


def test_partial_failure_unknown_station_inserts_nothing(authed, engine):
    submit(authed, stationcode="401M02845, NOPE")
    assert_no_insert(engine)


def test_db_error_mid_insert_rolls_back_all(authed, engine):
    engine.fail_with = RuntimeError("boom")
    errs = errors_of(submit(authed, stationcode="401M02845, 402M00155"))
    assert errs == ["Could not save - see server log for details."]
    assert_no_insert(engine)


def test_integrity_error_race_on_station_year_key(authed, engine):
    engine.fail_with = make_integrity_error('duplicate key value violates unique constraint "sample_tracker_stationcode_year_key"')
    errs = errors_of(submit(authed))
    assert "just logged for this year by someone else" in errs[0]
    assert_no_insert(engine)


def test_integrity_error_details_check_message(authed, engine):
    engine.fail_with = make_integrity_error('violates check constraint "sample_tracker_effort_details_check"')
    assert "Details is required" in errors_of(submit(authed))[0]


# ---------- details / effort rules ----------

def test_details_required_when_effort_not_1(authed, engine):
    errs = errors_of(submit(authed, effortequivalent="0.5", details=""))
    assert any(e.startswith("Details is required when EffortEquivalent is not 1") for e in errs)
    assert_no_insert(engine)


def test_effort_not_1_with_details_ok(authed, engine):
    submit(authed, effortequivalent="0.5", details="half the sites")
    assert engine.tracker[0]["effort"] == 0.5 and engine.tracker[0]["details"] == "half the sites"


def test_effort_1_point_0_needs_no_details(authed, engine):
    submit(authed, effortequivalent="1.0")
    assert len(engine.tracker) == 1


@pytest.mark.parametrize("val", ["0", "-2", "-0.5"])
def test_effort_non_positive(authed, engine, val):
    errs = errors_of(submit(authed, effortequivalent=val, details="x"))
    assert "EffortEquivalent must be greater than 0." in errs
    assert_no_insert(engine)


@pytest.mark.parametrize("val", ["abc", "1,5", "one"])
def test_effort_non_numeric(authed, engine, val):
    errs = errors_of(submit(authed, effortequivalent=val, details="x"))
    assert "EffortEquivalent must be numeric." in errs
    assert_no_insert(engine)


def test_non_numeric_effort_does_not_also_demand_details(authed):
    errs = errors_of(submit(authed, effortequivalent="abc", details=""))
    assert not any(e.startswith("Details is required when EffortEquivalent") for e in errs)


def test_details_required_when_purpose_other(authed, engine):
    errs = errors_of(submit(authed, purpose=["purpose_other"], details=""))
    assert any(e.startswith("Details is required when Purpose is Other") for e in errs)
    assert_no_insert(engine)


def test_purpose_other_with_details_ok(authed, engine):
    submit(authed, purpose=["purpose_other"], details="special study")
    assert engine.tracker[0]["purpose_other"] is True


# ---------- form stash / refill ----------

def test_failed_submit_stashes_form_and_refills_once(authed, engine):
    resp = submit(
        authed, stationcode="401M02845, NOPE", participant="LACFCD", year="2029",
        purpose=["restoration", "targeted"], effortequivalent="2", details="why not",
        workplan="WP_X", login_email="me@x.org",
    )
    with authed.session_transaction() as s:
        assert s["SAMPLE_TRACKER_FORM"]["stationcode"] == "401M02845, NOPE"
    page = authed.get(resp.headers["Location"])
    html = page.data.decode()
    assert 'value="401M02845, NOPE"' in html
    assert 'value="me@x.org"' in html and 'value="WP_X"' in html and 'value="why not"' in html
    assert 'value="2"' in html
    assert 'value="restoration" checked' in html and 'value="targeted" checked' in html
    assert 'value="status_and_trend" checked' not in html
    assert 'value="LACFCD" selected' in html and 'value="2029" selected' in html
    with authed.session_transaction() as s:
        assert "SAMPLE_TRACKER_FORM" not in s  # popped
    again = authed.get("/sample-tracking-tool").data.decode()
    assert "NOPE" not in again and 'value="me@x.org"' not in again


def test_error_banner_lists_each_error(authed):
    resp = submit(authed, participant="", year="")
    html = authed.get(resp.headers["Location"]).data.decode()
    assert "Participant is required." in html and "Year is required." in html


# ---------- Start Over ----------

def test_plain_get_is_clean_form(authed):
    html = authed.get("/sample-tracking-tool").data.decode()
    assert 'class="error"' not in html.split("</style>")[-1]
    assert 'name="stationcode"' in html and 'value="SMC_2027_2031_v1"' in html
    assert "checked" not in html.split('name="purpose"', 1)[1].split("</form>")[0].replace('type="checkbox"', "")
