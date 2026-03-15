from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
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


def _extract_header_lines(dat_file: Path) -> list[str]:
    header_lines: list[str] = []
    with dat_file.open("rt", encoding="utf-8") as fobj:
        for raw_line in fobj:
            if raw_line.startswith("#"):
                header_lines.append(raw_line.rstrip("\n"))
    return header_lines


def _write_parquet_with_headers(df: pd.DataFrame, dst: Path, header_lines: list[str]) -> None:
    table = pa.Table.from_pandas(df, preserve_index=False)
    metadata = dict(table.schema.metadata or {})
    metadata[_PARQUET_HEADER_METADATA_KEY] = json.dumps(header_lines, ensure_ascii=True).encode("utf-8")
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


def dataframe_to_dat(df: pd.DataFrame, dat_file: Path, header_lines: list[str] | None = None) -> None:
    columns = list(df.columns)

    dat_file.parent.mkdir(parents=True, exist_ok=True)
    with dat_file.open("wt", encoding="utf-8") as fobj:
        if header_lines:
            for line in header_lines:
                fobj.write(f"{line}\n")
            use_fixed_width = _is_tec_suite_layout(columns)
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


def convert_dat_to_parquet(src: Path, dst: Path, overwrite: bool = False) -> ConvertResult | None:
    if dst.exists() and not overwrite:
        return None

    dst.parent.mkdir(parents=True, exist_ok=True)
    df = dat_to_dataframe(src)
    header_lines = _extract_header_lines(src)
    _write_parquet_with_headers(df, dst, header_lines)
    return ConvertResult(source=src, destination=dst)


def convert_parquet_to_dat(src: Path, dst: Path, overwrite: bool = False) -> ConvertResult | None:
    if dst.exists() and not overwrite:
        return None

    header_lines = _read_headers_from_parquet(src)
    df = pd.read_parquet(src, engine="pyarrow")
    dataframe_to_dat(df, dst, header_lines=header_lines)
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

        result = convert_file(src_file, dst_file, direction=direction, overwrite=overwrite)
        if result is not None:
            converted.append(result)
            logger.info("converted: %s -> %s", result.source, result.destination)

        progress = round((idx / total) * 100)
        logger.info("Completed %s / %s", idx, total)
        logger.info("Progress: %s%%", progress)

    return converted
