"""Text cells in numeric columns (Excel text numbers) must not crash the core checks."""
import math

import pandas as pd
from flask import Flask

from proj.core.functions import check_precision, check_scale, convert_dtype
from proj.core.metadata import checkDataTypes, checkPrecision


def test_precision_and_scale_skip_text():
    for x in ["10", "0.72", "--", "1.8", ""]:
        assert check_precision(x, 6) is True
        assert check_scale(x, 3) is True


def test_precision_and_scale_numbers_unchanged():
    assert check_precision(10, 6) is True
    assert check_precision(1234567, 6) is False
    assert check_precision(1.5, 6) is True
    assert check_precision(-88, 6) is True
    assert check_precision(float("nan"), 6) is True
    assert check_scale(1.5, 3) is True
    assert check_scale(1.23456, 3) is False
    assert check_scale(-88, 3) is True
    assert check_scale(float("nan"), 3) is True


def test_core_checks_on_text_numeric_column():
    app = Flask(__name__)
    app.system_fields = ["objectid"]
    meta = pd.DataFrame({
        "column_name": ["result"],
        "udt_name": ["numeric"],
        "data_type": ["numeric"],
        "dtype": [float],
        "numeric_precision": [6],
        "numeric_scale": [3],
    })
    df = pd.DataFrame({"result": ["10", "0.72", "--", "1.8", -88, math.nan]}, dtype=object)
    with app.app_context():
        prec = checkPrecision(df, "tbl", None, meta)
        types_ = checkDataTypes(df, "tbl", None, meta)
    assert all(e == {} for e in prec)
    bad = [e for e in types_ if e]
    assert len(bad) == 1
    assert bad[0]["rows"] == [2]  # only '--' is reported
    assert bad[0]["error_type"] == "Invalid Datatype"
