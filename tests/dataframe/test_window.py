from __future__ import annotations

import datetime

import pytest

from daft import Window, col


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_window_local(make_df, repartition_nparts, with_morsel_size):
    daft_df = make_df(
        {
            "id": [1, 1, 2, 2, 2, 2, 2, 2, 2],
            "values": [0, 0, 0, 1.5, 3, 3, 3.5, 6, 6],
        },
        repartition=repartition_nparts,
    )

    window = Window(
        ["id"],
        ["values"],
    )

    daft_df = daft_df.with_columns(
        {
            "cum_sum": col("values").sum().over(window),
            "rank": col("values").rank().over(window),
            "lead": col("values").lead(2, 1.5).over(window),
        }
    ).sort(["id", "values"])

    expected = {
        "id": [1, 1, 2, 2, 2, 2, 2, 2, 2],
        "values": [0, 0, 0, 1.5, 3, 3, 3.5, 6, 6],
        "cum_sum": [0, 0, 0, 1.5, 4.5, 7.5, 11, 17, 23],
        "rank": [1, 1, 1, 2, 3, 3, 5, 6, 6],
        "lead": [1.5, 1.5, 3, 3, 3.5, 6, 6, 1.5, 1.5],
    }

    daft_df.collect()
    daft_cols = daft_df.to_pydict()
    print("computed dataframe", daft_cols)

    assert daft_cols == expected


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_window_row_frame(make_df, repartition_nparts, with_morsel_size):
    daft_df = make_df(
        {
            "id": [1, 1, 2, 2, 2, 2, 2, 2, 2],
            "values": [0, 0, 0, 1.5, 3, 3, 3.5, 6, 6],
        },
        repartition=repartition_nparts,
    )

    window = Window(
        ["id"],
        ["values"],
        rows_between=(1, 2),
    )

    daft_df = daft_df.with_column("frame_sum", col("values").sum().over(window)).sort(["id", "values"])

    expected = {
        "id": [1, 1, 2, 2, 2, 2, 2, 2, 2],
        "values": [0, 0, 0, 1.5, 3, 3, 3.5, 6, 6],
        "frame_sum": [0, 0, 4.5, 7.5, 11, 15.5, 18.5, 15.5, 12],
    }

    daft_df.collect()
    daft_cols = daft_df.to_pydict()
    print("computed dataframe", daft_cols)

    assert daft_cols == expected


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_window_range_frame(make_df, repartition_nparts, with_morsel_size):
    daft_df = make_df(
        {
            "id": [1, 1, 2, 2, 2, 2, 2, 2, 2],
            "values": [0, 0, 0, 1.5, 3, 3, 3.5, 6, 6],
        },
        repartition=repartition_nparts,
    )

    window = Window(
        ["id"],
        ["values"],
        range_between=(1, 2),
    )

    daft_df = daft_df.with_column("frame_sum", col("values").sum().over(window)).sort(["id", "values"])

    expected = {
        "id": [1, 1, 2, 2, 2, 2, 2, 2, 2],
        "values": [0, 0, 0, 1.5, 3, 3, 3.5, 6, 6],
        "frame_sum": [0, 0, 1.5, 11, 9.5, 9.5, 9.5, 12, 12],
    }

    daft_df.collect()
    daft_cols = daft_df.to_pydict()
    print("computed dataframe", daft_cols)

    assert daft_cols == expected


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_window_range_frame_temporal(make_df, repartition_nparts, with_morsel_size):
    daft_df = make_df(
        {
            "id": [1, 1, 2, 2, 2, 2, 2, 2, 2],
            "timestamps": [datetime.date(2024, 1, 1) + datetime.timedelta(days=i) for i in range(9)],
            "values": [0, 0, 0, 1.5, 3, 3, 3.5, 6, 6],
        },
        repartition=repartition_nparts,
    )

    window = Window(
        ["id"],
        ["timestamps"],
        range_between=(datetime.timedelta(days=1), datetime.timedelta(days=2)),
    )

    daft_df = daft_df.with_column("frame_sum", col("values").sum().over(window)).sort(["id", "timestamps"])

    expected = {
        "id": [1, 1, 2, 2, 2, 2, 2, 2, 2],
        "timestamps": [datetime.date(2024, 1, 1) + datetime.timedelta(days=i) for i in range(9)],
        "values": [0, 0, 0, 1.5, 3, 3, 3.5, 6, 6],
        "frame_sum": [0, 0, 4.5, 7.5, 11, 15.5, 18.5, 15.5, 12],
    }

    daft_df.collect()
    daft_cols = daft_df.to_pydict()
    print("computed dataframe", daft_cols)

    assert daft_cols == expected
