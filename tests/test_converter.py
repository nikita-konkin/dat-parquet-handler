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


def test_dat_to_parquet_trims_last_four_chars_from_station_folder(tmp_path: Path) -> None:
    src = tmp_path / "src"
    dst = tmp_path / "dst"

    dat_file = src / "2026" / "070" / "aksu3070" / "aksu_G04_070_26.dat"
    _write_sample_dat(dat_file)

    converted = convert_tree(src, dst, direction="dat-to-parquet")

    assert len(converted) == 1
    assert (dst / "2026" / "070" / "aksu" / "aksu_G04_070_26.parquet").exists()
    assert not (dst / "2026" / "070" / "aksu3070" / "aksu_G04_070_26.parquet").exists()


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


def test_tayabstec_series_dat_to_parquet_and_back(tmp_path: Path) -> None:
    src = tmp_path / "src"
    mid = tmp_path / "mid"
    dst = tmp_path / "dst"

    dat_file = src / "2026" / "001" / "aksu0010" / "aksu_001_2026.dat"
    dat_file.parent.mkdir(parents=True, exist_ok=True)
    dat_file.write_text(
        "\n".join(
            [
                "# UT  I_v  G_lon  G_lat  G_q_lon  G_q_lat  G_t  G_q_t",
                "  0.000      5.032     -0.027     -0.626     -0.009     0.043     -0.193      0.080",
                "  0.050      4.812     -0.026     -0.592     -0.010     0.039     -1.087     -0.597",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    to_parquet = convert_tree(src, mid, direction="dat-to-parquet")
    assert len(to_parquet) == 1

    parquet_file = mid / "2026" / "001" / "aksu" / "aksu_001_2026.parquet"
    assert parquet_file.exists()

    df = pd.read_parquet(parquet_file)
    assert list(df.columns) == ["UT", "I_v", "G_lon", "G_lat", "G_q_lon", "G_q_lat", "G_t", "G_q_t"]
    assert len(df) == 2

    to_dat = convert_tree(mid, dst, direction="parquet-to-dat")
    assert len(to_dat) == 1

    out_file = dst / "2026" / "001" / "aksu" / "aksu_001_2026.dat"
    assert out_file.exists()

    restored_headers = [
        line
        for line in out_file.read_text(encoding="utf-8").splitlines()
        if line.startswith("#")
    ]
    assert restored_headers == ["# UT  I_v  G_lon  G_lat  G_q_lon  G_q_lat  G_t  G_q_t"]


def test_tayabstec_dcb_dat_to_parquet_and_back(tmp_path: Path) -> None:
    src = tmp_path / "src"
    mid = tmp_path / "mid"
    dst = tmp_path / "dst"

    dat_file = src / "2026" / "001" / "aksu0010" / "DCB_aksu_001_2026.dat"
    dat_file.parent.mkdir(parents=True, exist_ok=True)
    dat_file.write_text(
        "\n".join(
            [
                "# DCB sat:",
                "G 1     -10.665",
                "G10      13.410",
                "R 2       2.670",
                "# DCB rec:",
                "G     -34.972",
                "R     -20.379",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    to_parquet = convert_tree(src, mid, direction="dat-to-parquet")
    assert len(to_parquet) == 1

    parquet_file = mid / "2026" / "001" / "aksu" / "DCB_aksu_001_2026.parquet"
    assert parquet_file.exists()

    df = pd.read_parquet(parquet_file)
    assert list(df.columns) == ["section", "system", "prn", "value"]
    assert len(df) == 5
    assert df["section"].tolist() == ["sat", "sat", "sat", "rec", "rec"]

    to_dat = convert_tree(mid, dst, direction="parquet-to-dat")
    assert len(to_dat) == 1

    out_file = dst / "2026" / "001" / "aksu" / "DCB_aksu_001_2026.dat"
    assert out_file.exists()

    content = out_file.read_text(encoding="utf-8")
    assert "# DCB sat:" in content
    assert "# DCB rec:" in content
    assert "G 1    -10.665" in content
    assert "G10     13.410" in content
    assert "G    -34.972" in content


def test_convert_tree_skips_invalid_dat_and_continues(tmp_path: Path) -> None:
    src = tmp_path / "src"
    dst = tmp_path / "dst"

    invalid_dat = src / "001" / "armv001w30" / "armv_001_2026.dat"
    invalid_dat.parent.mkdir(parents=True, exist_ok=True)
    invalid_dat.write_text(
        "\n".join(
            [
                "# UT  I_v  G_lon  G_lat  G_q_lon  G_q_lat  G_t  G_q_t",
                "# only header, no rows",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    valid_dat = src / "001" / "aksu0010" / "aksu_001_2026.dat"
    valid_dat.parent.mkdir(parents=True, exist_ok=True)
    valid_dat.write_text(
        "\n".join(
            [
                "# UT  I_v  G_lon  G_lat  G_q_lon  G_q_lat  G_t  G_q_t",
                "  0.000      5.032     -0.027     -0.626     -0.009     0.043     -0.193      0.080",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    converted = convert_tree(src, dst, direction="dat-to-parquet")

    assert len(converted) == 1
    assert (dst / "001" / "aksu" / "aksu_001_2026.parquet").exists()
    assert not (dst / "001" / "armv001w30" / "armv_001_2026.parquet").exists()


def test_dat_to_parquet_uses_station_suffix_for_10_char_station_folder(tmp_path: Path) -> None:
    src = tmp_path / "src"
    dst = tmp_path / "dst"

    dat_file = src / "2026" / "001" / "armv001k00" / "armv_001_2026.dat"
    dat_file.parent.mkdir(parents=True, exist_ok=True)
    dat_file.write_text(
        "\n".join(
            [
                "# UT  I_v  G_lon  G_lat  G_q_lon  G_q_lat  G_t  G_q_t",
                "  0.000      5.032     -0.027     -0.626     -0.009     0.043     -0.193      0.080",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    converted = convert_tree(src, dst, direction="dat-to-parquet")

    assert len(converted) == 1
    assert (dst / "2026" / "001" / "armv00" / "armvk00_001_2026.parquet").exists()
    assert not (dst / "2026" / "001" / "armv00" / "armv_001_2026.parquet").exists()


def test_tec_suite_dat_to_parquet_uses_station_suffix_for_10_char_station_folder(tmp_path: Path) -> None:
    src = tmp_path / "src"
    dst = tmp_path / "dst"

    dat_file = src / "2026" / "001" / "aksu001i14" / "aksu_E02_001_26.dat"
    _write_sample_dat(dat_file)

    converted = convert_tree(src, dst, direction="dat-to-parquet")

    assert len(converted) == 1
    assert (dst / "2026" / "001" / "aksu00" / "aksui14_E02_001_26.parquet").exists()
    assert not (dst / "2026" / "001" / "aksu00" / "aksu_E02_001_26.parquet").exists()


def test_tec_suite_parquet_to_dat_keeps_filename_when_station_folder_is_trimmed(tmp_path: Path) -> None:
    src = tmp_path / "src"
    dst = tmp_path / "dst"

    parquet_file = src / "2026" / "001" / "aksu00" / "aksui14_E02_001_26.parquet"
    parquet_file.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(
        {
            "tsn": [1],
            "hour": [0.00833333333],
            "el": [16.81043],
            "az": [237.18424],
            "tec.l1l2": [16.9],
            "tec.c1p2": [-33.502],
            "validity": [0],
        }
    )
    df.to_parquet(parquet_file, index=False)

    converted = convert_tree(src, dst, direction="parquet-to-dat")

    assert len(converted) == 1
    assert (dst / "2026" / "001" / "aksu00" / "aksui14_E02_001_26.dat").exists()
