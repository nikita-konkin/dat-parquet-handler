# dat-parquet-handler

Standalone project for converting tec-suite output files in both directions:

- `.dat` -> `.parquet`
- `.parquet` -> `.dat`

The converter preserves relative directory structure from source root to destination root.

## Install

```bash
pip install .
```

## Usage

```bash
dat-parquet-handler --direction dat-to-parquet --src /path/to/out --dst /path/to/out_parquet
dat-parquet-handler --direction parquet-to-dat --src /path/to/out_parquet --dst /path/to/out_dat
```

Options:

- `--direction` : `dat-to-parquet` or `parquet-to-dat`
- `--src` : source root directory
- `--dst` : destination root directory (defaults to source)
- `--overwrite` : overwrite existing destination files

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
