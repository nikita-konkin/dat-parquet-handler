from pathlib import Path

import pandas as pd

from dat_parquet_handler.converter import convert_tree, dat_to_dataframe, dataframe_to_dat


def _write_sample_dat(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(
            [
                "# Created on 2026-03-03 20:52:38",
                "# Columns: tsn, hour, el, az, tec.l1l2, tec.c1p2, validity",
                "1 0.00833333333 16.81043 237.18424 16.900 -33.502 0",
                "2 0.01666666667 17.01224 237.28189 16.819 -33.312 0",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def test_dat_to_parquet_tree_preserves_structure(tmp_path: Path) -> None:
    src = tmp_path / "src"
    dst = tmp_path / "dst"

    dat_file = src / "2026" / "001" / "aksu" / "aksu_G04_001_26.dat"
    _write_sample_dat(dat_file)

    converted = convert_tree(src, dst, direction="dat-to-parquet")

    assert len(converted) == 1
    out_file = dst / "2026" / "001" / "aksu" / "aksu_G04_001_26.parquet"
    assert out_file.exists()

    df = pd.read_parquet(out_file)
    assert list(df.columns) == ["tsn", "hour", "el", "az", "tec.l1l2", "tec.c1p2", "validity"]
    assert len(df) == 2


def test_parquet_to_dat_tree_preserves_structure(tmp_path: Path) -> None:
    src = tmp_path / "src"
    dst = tmp_path / "dst"

    parquet_file = src / "2026" / "001" / "aksu" / "aksu_G04_001_26.parquet"
    parquet_file.parent.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(
        {
            "tsn": [1, 2],
            "hour": [0.00833333333, 0.01666666667],
            "el": [16.81043, 17.01224],
            "az": [237.18424, 237.28189],
            "tec.l1l2": [16.9, 16.819],
            "tec.c1p2": [-33.502, -33.312],
            "validity": [0, 0],
        }
    )
    df.to_parquet(parquet_file, index=False)

    converted = convert_tree(src, dst, direction="parquet-to-dat")

    assert len(converted) == 1
    out_file = dst / "2026" / "001" / "aksu" / "aksu_G04_001_26.dat"
    assert out_file.exists()

    out_df = dat_to_dataframe(out_file)
    assert list(out_df.columns) == list(df.columns)
    assert len(out_df) == 2
    assert out_df["tsn"].tolist() == [1, 2]


def test_overwrite_false_skips_existing(tmp_path: Path) -> None:
    src = tmp_path / "src"
    dst = tmp_path / "dst"

    dat_file = src / "sample.dat"
    _write_sample_dat(dat_file)

    out_file = dst / "sample.parquet"
    out_file.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"x": [1]}).to_parquet(out_file, index=False)

    converted = convert_tree(src, dst, direction="dat-to-parquet", overwrite=False)
    assert converted == []


def test_dataframe_to_dat_uses_fixed_width_tec_layout(tmp_path: Path) -> None:
    out_file = tmp_path / "fixed_width.dat"
    df = pd.DataFrame(
        {
            "tsn": [13],
            "hour": [0.10833333333],
            "el": [10.67054],
            "az": [301.68187],
            "tec.l1l2": [29.225],
            "tec.c1p2": [-26.84],
            "validity": [3],
        }
    )

    dataframe_to_dat(df, out_file)
    lines = out_file.read_text(encoding="utf-8").splitlines()

    assert lines[2] == "# (I11,1X,F14.11,1X,F10.5,1X,F11.5,1X,F21.3,1X,F10.3,1X,I7)"
    assert lines[3] == "         13  0.10833333333   10.67054   301.68187                29.225    -26.840       3"


def test_dat_headers_restored_after_round_trip(tmp_path: Path) -> None:
    src = tmp_path / "src"
    mid = tmp_path / "mid"
    dst = tmp_path / "dst"

    dat_file = src / "2026" / "001" / "alex" / "alex_G01_001_26.dat"
    dat_file.parent.mkdir(parents=True, exist_ok=True)
    dat_file.write_text(
        "\n".join(
            [
                "# Created on 2026-03-03 20:52:39 ",
                "# Sources: /data/rinex/01/alex001/alex0010.26o, /data/rinex/01/alex001/alex0010.26n",
                "# Satellite: G01",
                "# Interval: 30.0",
                "# Sampling interval: 0.0 (not used).",
                "# Site: alex",
                "# Position (L, B, H): 38.76984687694461, 56.402265615270295, 201.89454679843038",
                "# Position (X, Y, Z): 2758256.344, 2215306.0778, 5289526.3492",
                "# datetime format: %Y-%m-%dT%H:%M:%S",
                "# Columns: tsn, hour, el, az, tec.l1l2, tec.c1p2, validity",
                "# (I11,1X,F14.11,1X,F10.5,1X,F11.5,1X,F21.3,1X,F10.3,1X,I7)",
                "         13  0.10833333333   10.67054   301.68187                29.225    -26.840       3",
                "         14  0.11666666667   10.87406   301.73937                29.267    -29.315       0",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    to_parquet = convert_tree(src, mid, direction="dat-to-parquet")
    assert len(to_parquet) == 1

    to_dat = convert_tree(mid, dst, direction="parquet-to-dat")
    assert len(to_dat) == 1

    out_file = dst / "2026" / "001" / "alex" / "alex_G01_001_26.dat"
    assert out_file.exists()

    original_headers = [
        line
        for line in dat_file.read_text(encoding="utf-8").splitlines()
        if line.startswith("#")
    ]
    restored_headers = [
        line
        for line in out_file.read_text(encoding="utf-8").splitlines()
        if line.startswith("#")
    ]

    assert restored_headers == original_headers
