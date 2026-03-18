from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import re
from typing import Iterable

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .logger import get_logger


logger = get_logger(__name__)


_TEC_SUITE_COLUMNS = (
    "tsn",
    "hour",
    "el",
    "az",
    "tec.l1l2",
    "tec.c1p2",
    "validity",
)

# Mirrors '# (I11,1X,F14.11,1X,F10.5,1X,F11.5,1X,F21.3,1X,F10.3,1X,I7)'
_TEC_SUITE_FORMATS: dict[str, tuple[str, int, int]] = {
    "tsn": ("int", 11, 0),
    "hour": ("float", 14, 11),
    "el": ("float", 10, 5),
    "az": ("float", 11, 5),
    "tec.l1l2": ("float", 21, 3),
    "tec.c1p2": ("float", 10, 3),
    "validity": ("int", 7, 0),
}

_PARQUET_HEADER_METADATA_KEY = b"dat_parquet_handler.header_lines"
_PARQUET_DAT_FORMAT_METADATA_KEY = b"dat_parquet_handler.dat_format"

_DAT_FORMAT_TEC_SUITE = "tec-suite"
_DAT_FORMAT_TAYABSTEC_SERIES = "tayabstec-series"
_DAT_FORMAT_TAYABSTEC_DCB = "tayabstec-dcb"
_DAT_FORMAT_GENERIC = "generic"


@dataclass(frozen=True)
class ConvertResult:
    source: Path
    destination: Path


def parse_columns(header_lines: list[str]) -> list[str] | None:
    prefix = "# Columns:"
    for line in header_lines:
        if line.startswith(prefix):
            cols = line[len(prefix):].strip()
            return [c.strip() for c in cols.split(",") if c.strip()]
    return None


def parse_tayabstec_series_columns(header_lines: list[str]) -> list[str] | None:
    for line in header_lines:
        normalized = line.lstrip("#").strip()
        if not normalized:
            continue
        tokens = normalized.split()
        if len(tokens) >= 2 and tokens[0] == "UT" and ("I_v" in tokens or "G_lon" in tokens):
            return tokens
    return None


def detect_dat_format(header_lines: list[str]) -> str:
    if parse_columns(header_lines):
        return _DAT_FORMAT_TEC_SUITE

    if parse_tayabstec_series_columns(header_lines):
        return _DAT_FORMAT_TAYABSTEC_SERIES

    lowered_headers = [line.lower() for line in header_lines]
    if any("# dcb sat:" in line for line in lowered_headers) or any("# dcb rec:" in line for line in lowered_headers):
        return _DAT_FORMAT_TAYABSTEC_DCB

    return _DAT_FORMAT_GENERIC


def parse_data_row(line: str, columns: list[str]) -> list[str] | None:
    row = line.strip().split()

    if len(row) == len(columns):
        return row

    if "datetime" in columns and len(row) > len(columns):
        dt_idx = columns.index("datetime")
        extra = len(row) - len(columns)

        datetime_tokens = row[dt_idx:dt_idx + extra + 1]
        row = row[:dt_idx] + [" ".join(datetime_tokens)] + row[dt_idx + extra + 1:]
        if len(row) == len(columns):
            return row

    return None


def dat_to_dataframe(dat_file: Path) -> pd.DataFrame:
    header_lines = _extract_header_lines(dat_file)
    dat_format = detect_dat_format(header_lines)

    if dat_format == _DAT_FORMAT_TAYABSTEC_DCB:
        return _tayabstec_dcb_to_dataframe(dat_file)

    header_lines: list[str] = []
    data_lines: list[str] = []

    with dat_file.open("rt", encoding="utf-8") as fobj:
        for raw_line in fobj:
            if not raw_line.strip():
                continue
            if raw_line.startswith("#"):
                header_lines.append(raw_line.strip())
            else:
                data_lines.append(raw_line.rstrip("\n"))

    if dat_format == _DAT_FORMAT_TEC_SUITE:
        columns = parse_columns(header_lines)
    elif dat_format == _DAT_FORMAT_TAYABSTEC_SERIES:
        columns = parse_tayabstec_series_columns(header_lines)
    else:
        columns = parse_columns(header_lines)

    if not columns:
        raise ValueError(f"Columns header not found in '{dat_file}'")

    rows: list[list[str]] = []
    for line in data_lines:
        row = parse_data_row(line, columns)
        if row is not None:
            rows.append(row)

    if not rows:
        raise ValueError(f"No valid data rows found in '{dat_file}'")

    df = pd.DataFrame(rows, columns=columns)

    for col in columns:
        if col == "datetime":
            continue
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def _tayabstec_dcb_to_dataframe(dat_file: Path) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    section: str | None = None

    with dat_file.open("rt", encoding="utf-8") as fobj:
        for raw_line in fobj:
            line = raw_line.strip()
            if not line:
                continue

            if line.startswith("#"):
                lowered = line.lower()
                if "# dcb sat:" in lowered:
                    section = "sat"
                elif "# dcb rec:" in lowered:
                    section = "rec"
                continue

            if section == "sat":
                tokens = line.split()
                if len(tokens) >= 3:
                    system = tokens[0]
                    prn = int(tokens[1])
                    value = float(tokens[2])
                elif len(tokens) == 2:
                    match = re.fullmatch(r"([A-Za-z])(\d+)", tokens[0])
                    if match is None:
                        continue
                    system = match.group(1)
                    prn = int(match.group(2))
                    value = float(tokens[1])
                else:
                    continue
                rows.append(
                    {
                        "section": "sat",
                        "system": system,
                        "prn": prn,
                        "value": value,
                    }
                )
            elif section == "rec":
                tokens = line.split()
                if len(tokens) < 2:
                    continue
                rows.append(
                    {
                        "section": "rec",
                        "system": tokens[0],
                        "prn": None,
                        "value": float(tokens[1]),
                    }
                )

    if not rows:
        raise ValueError(f"No valid DCB rows found in '{dat_file}'")

    df = pd.DataFrame(rows, columns=["section", "system", "prn", "value"])
    df["prn"] = df["prn"].astype("Int64")
    return df


def _extract_header_lines(dat_file: Path) -> list[str]:
    header_lines: list[str] = []
    with dat_file.open("rt", encoding="utf-8") as fobj:
        for raw_line in fobj:
            if raw_line.startswith("#"):
                header_lines.append(raw_line.rstrip("\n"))
    return header_lines


def _write_parquet_with_headers(df: pd.DataFrame, dst: Path, header_lines: list[str], dat_format: str) -> None:
    table = pa.Table.from_pandas(df, preserve_index=False)
    metadata = dict(table.schema.metadata or {})
    metadata[_PARQUET_HEADER_METADATA_KEY] = json.dumps(header_lines, ensure_ascii=True).encode("utf-8")
    metadata[_PARQUET_DAT_FORMAT_METADATA_KEY] = dat_format.encode("utf-8")
    table = table.replace_schema_metadata(metadata)
    pq.write_table(table, dst)


def _read_headers_from_parquet(src: Path) -> list[str] | None:
    try:
        schema = pq.read_schema(src)
    except Exception:
        return None

    metadata = schema.metadata or {}
    encoded = metadata.get(_PARQUET_HEADER_METADATA_KEY)
    if not encoded:
        return None

    try:
        header_lines = json.loads(encoded.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None

    if not isinstance(header_lines, list) or not all(isinstance(line, str) for line in header_lines):
        return None

    return header_lines


def _read_dat_format_from_parquet(src: Path) -> str | None:
    try:
        schema = pq.read_schema(src)
    except Exception:
        return None

    metadata = schema.metadata or {}
    encoded = metadata.get(_PARQUET_DAT_FORMAT_METADATA_KEY)
    if not encoded:
        return None

    try:
        return encoded.decode("utf-8")
    except UnicodeDecodeError:
        return None


def _format_value(value, col: str) -> str:
    if pd.isna(value):
        return "0"

    if col in {"tsn", "validity"} or col.endswith(".lli"):
        try:
            return str(int(round(float(value))))
        except (TypeError, ValueError):
            return "0"

    if isinstance(value, int):
        return str(value)

    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return str(value)

    return f"{numeric:.12g}"


def _format_fixed_width_value(value, col: str) -> str:
    spec = _TEC_SUITE_FORMATS.get(col)
    if spec is None:
        return _format_value(value, col)

    kind, width, precision = spec

    if kind == "int":
        try:
            ival = int(round(float(value)))
        except (TypeError, ValueError):
            ival = 0
        return f"{ival:>{width}d}"

    try:
        fval = float(value)
    except (TypeError, ValueError):
        fval = 0.0
    return f"{fval:>{width}.{precision}f}"


def _is_tec_suite_layout(columns: list[str]) -> bool:
    return tuple(columns) == _TEC_SUITE_COLUMNS


def dataframe_to_dat(
    df: pd.DataFrame,
    dat_file: Path,
    header_lines: list[str] | None = None,
    dat_format: str | None = None,
) -> None:
    columns = list(df.columns)
    active_format = dat_format or _DAT_FORMAT_GENERIC

    if active_format == _DAT_FORMAT_TAYABSTEC_DCB:
        _write_tayabstec_dcb(df, dat_file)
        return

    dat_file.parent.mkdir(parents=True, exist_ok=True)
    with dat_file.open("wt", encoding="utf-8") as fobj:
        if header_lines:
            for line in header_lines:
                fobj.write(f"{line}\n")
            use_fixed_width = _is_tec_suite_layout(columns)
        elif active_format == _DAT_FORMAT_TAYABSTEC_SERIES:
            fobj.write("# " + "  ".join(columns) + "\n")
            use_fixed_width = False
        else:
            fobj.write("# Generated by dat-parquet-handler\n")
            fobj.write(f"# Columns: {', '.join(columns)}\n")
            use_fixed_width = _is_tec_suite_layout(columns)
            if use_fixed_width:
                fobj.write("# (I11,1X,F14.11,1X,F10.5,1X,F11.5,1X,F21.3,1X,F10.3,1X,I7)\n")

        for row in df.itertuples(index=False, name=None):
            if use_fixed_width:
                values = [_format_fixed_width_value(value, columns[idx]) for idx, value in enumerate(row)]
            else:
                values = [_format_value(value, columns[idx]) for idx, value in enumerate(row)]
            fobj.write(" ".join(values) + "\n")


def _write_tayabstec_dcb(df: pd.DataFrame, dat_file: Path) -> None:
    expected = {"section", "system", "prn", "value"}
    missing = expected.difference(df.columns)
    if missing:
        missing_list = ", ".join(sorted(missing))
        raise ValueError(f"Cannot write TayAbsTEC DCB DAT. Missing columns: {missing_list}")

    dat_file.parent.mkdir(parents=True, exist_ok=True)

    normalized = df.copy()
    normalized["section"] = normalized["section"].astype(str).str.lower()
    normalized["system"] = normalized["system"].astype(str)
    normalized["value"] = pd.to_numeric(normalized["value"], errors="coerce").fillna(0.0)

    with dat_file.open("wt", encoding="utf-8") as fobj:
        sat_rows = normalized[normalized["section"] == "sat"]
        rec_rows = normalized[normalized["section"] == "rec"]

        if not sat_rows.empty:
            fobj.write("# DCB sat:\n")
            for row in sat_rows.itertuples(index=False):
                prn = 0 if pd.isna(row.prn) else int(row.prn)
                fobj.write(f"{row.system}{prn:>2d} {float(row.value):10.3f}\n")

        if not rec_rows.empty:
            fobj.write("# DCB rec:\n")
            for row in rec_rows.itertuples(index=False):
                fobj.write(f"{row.system} {float(row.value):10.3f}\n")


def convert_dat_to_parquet(src: Path, dst: Path, overwrite: bool = False) -> ConvertResult | None:
    if dst.exists() and not overwrite:
        logger.info("skip: %s (destination exists)", dst)
        return None

    dst.parent.mkdir(parents=True, exist_ok=True)
    header_lines = _extract_header_lines(src)
    dat_format = detect_dat_format(header_lines)
    logger.info("reading DAT: %s (format=%s)", src, dat_format)

    df = dat_to_dataframe(src)
    logger.info("writing Parquet: %s (rows=%s, columns=%s)", dst, len(df), len(df.columns))

    _write_parquet_with_headers(df, dst, header_lines, dat_format)
    return ConvertResult(source=src, destination=dst)


def convert_parquet_to_dat(src: Path, dst: Path, overwrite: bool = False) -> ConvertResult | None:
    if dst.exists() and not overwrite:
        logger.info("skip: %s (destination exists)", dst)
        return None

    header_lines = _read_headers_from_parquet(src)
    dat_format = _read_dat_format_from_parquet(src)
    effective_format = dat_format or _DAT_FORMAT_GENERIC
    logger.info("reading Parquet: %s (format=%s)", src, effective_format)

    df = pd.read_parquet(src, engine="pyarrow")
    logger.info("writing DAT: %s (rows=%s, columns=%s)", dst, len(df), len(df.columns))

    dataframe_to_dat(df, dst, header_lines=header_lines, dat_format=dat_format)
    return ConvertResult(source=src, destination=dst)


def iter_files(root: Path, suffix: str) -> Iterable[Path]:
    suffix = suffix.lower()
    for path in root.rglob("*"):
        if path.is_file() and path.suffix.lower() == suffix:
            yield path


def convert_file(src: Path, dst: Path, direction: str, overwrite: bool = False) -> ConvertResult | None:
    if direction == "dat-to-parquet":
        return convert_dat_to_parquet(src, dst, overwrite=overwrite)
    if direction == "parquet-to-dat":
        return convert_parquet_to_dat(src, dst, overwrite=overwrite)
    raise ValueError(f"Unknown direction: {direction}")


def convert_tree(src_root: Path, dst_root: Path, direction: str, overwrite: bool = False) -> list[ConvertResult]:
    if direction == "dat-to-parquet":
        in_suffix = ".dat"
        out_suffix = ".parquet"
    elif direction == "parquet-to-dat":
        in_suffix = ".parquet"
        out_suffix = ".dat"
    else:
        raise ValueError(f"Unknown direction: {direction}")

    converted: list[ConvertResult] = []
    source_files = list(iter_files(src_root, in_suffix))
    total = len(source_files)

    if total == 0:
        logger.info("Completed 0 / 0")
        logger.info("Progress: 100%%")
        return converted

    for idx, src_file in enumerate(source_files, start=1):
        rel_path = src_file.relative_to(src_root)
        dst_file = (dst_root / rel_path).with_suffix(out_suffix)

        try:
            result = convert_file(src_file, dst_file, direction=direction, overwrite=overwrite)
        except Exception as err:
            logger.error("error: %s", err)
            logger.info("skipped: %s", src_file)
            result = None

        if result is not None:
            converted.append(result)
            logger.info("converted: %s -> %s", result.source, result.destination)

        progress = round((idx / total) * 100)
        logger.info("Completed %s / %s", idx, total)
        logger.info("Progress: %s%%", progress)

    return converted
