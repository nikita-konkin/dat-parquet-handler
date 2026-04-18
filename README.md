# dat-parquet-handler

Standalone project for converting DAT files in both directions:

- `.dat` -> `.parquet`
- `.parquet` -> `.dat`

The converter preserves relative directory structure from source root to destination root.

Supported DAT layouts:

- TEC-suite files with `# Columns: ...` headers (including fixed-width TEC satellite rows)
- TayAbsTEC time-series files with header like `# UT  I_v  G_lon ...`
- TayAbsTEC DCB files with sections `# DCB sat:` and `# DCB rec:`

## Install

```bash
pip install .
```

## Usage

```bash
dat-parquet-handler --direction dat-to-parquet --src /path/to/out --dst /path/to/out_parquet
dat-parquet-handler --direction parquet-to-dat --src /path/to/out_parquet --dst /path/to/out_dat
```

Year/day/station tree example:

```bash
dat-parquet-handler --direction dat-to-parquet --src /data/absoltec_out --dst /data/absoltec_parquet
```

If source contains:

`/data/absoltec_out/2026/001/aksu0010/alex_001_2026.dat`

and:

`/data/absoltec_out/2026/001/aksu0010/DCB_alex_001_2026.dat`

then output will contain:

- `/data/absoltec_parquet/2026/001/aksu0010/alex_001_2026.parquet`
- `/data/absoltec_parquet/2026/001/aksu0010/DCB_alex_001_2026.parquet`

Options:

- `--direction` : `dat-to-parquet` or `parquet-to-dat`
- `--src` : source root directory
- `--dst` : destination root directory (defaults to source)
- `--overwrite` : overwrite existing destination files
- `--day-from` : inclusive start day-of-year filter (`1..366`)
- `--day-to` : inclusive end day-of-year filter (`1..366`)

Example range conversion (only days 120..130):

```bash
dat-parquet-handler --direction dat-to-parquet --src /data/in --dst /data/out --day-from 120 --day-to 130
```

During conversion, the tool logs progress messages that can be parsed by automation:

- `Completed 3 / 10`
- `Progress: 30%`

For `.parquet` -> `.dat` with TEC columns (`tsn, hour, el, az, tec.l1l2, tec.c1p2, validity`),
the writer uses fixed-width spacing compatible with original DAT layout:

- `# (I11,1X,F14.11,1X,F10.5,1X,F11.5,1X,F21.3,1X,F10.3,1X,I7)`

When Parquet files are created by this tool from DAT inputs, the original DAT header block
(`Created on`, `Sources`, `Satellite`, `Site`, positions, `datetime format`, etc.) is stored
in Parquet metadata and restored on `.parquet` -> `.dat` conversion.

For TayAbsTEC DCB files, Parquet data is stored with columns:

- `section` (`sat` or `rec`)
- `system` (for example `G`, `R`)
- `prn` (nullable, used for `sat` rows)
- `value`

## Run tests

```bash
pip install -e .[test]
pytest
```

## Docker

Build image:

```bash
docker build -t dat-parquet-handler .
```

Run converter (example):

```bash
docker run --rm -v /host/data:/data dat-parquet-handler \
  --direction dat-to-parquet --src /data/in --dst /data/out
```

Or via compose:

```bash
docker compose run --rm dat-parquet-handler \
  --direction parquet-to-dat --src /data/in --dst /data/out
```
