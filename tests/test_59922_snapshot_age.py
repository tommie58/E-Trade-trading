"""Snapshot age is never fresh for a value that is not a past second-epoch."""
import math
import time

import main_bot


def test_bad_epochs_are_unknown_never_fresh():
    now = time.time()
    assert main_bot._snapshot_age({"epoch": now - 5}) == 5.0 or (
        4.0 <= main_bot._snapshot_age({"epoch": now - 5}) <= 6.0
    )
    fresh = main_bot._snapshot_age({"epoch": now - 2})
    assert fresh is not None and fresh < 10

    for epoch in (
        float("nan"),
        float("inf"),
        float("-inf"),
        now + 30,
        now * 1000,
        0,
        -5,
        "nope",
        None,
    ):
        age = main_bot._snapshot_age({"epoch": epoch})
        assert age is None, epoch
        assert age != 0

    assert main_bot._snapshot_age(None) is None
    assert main_bot._snapshot_age({}) is None
    assert math.isnan(float("nan"))
