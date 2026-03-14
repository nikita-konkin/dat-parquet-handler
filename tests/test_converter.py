from pathlib import Path

import pandas as pd

from dat_parquet_handler.converter import convert_tree, dat_to_dataframe


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
