from __future__ import annotations

import argparse
from pathlib import Path

from .converter import convert_tree


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Convert tec-suite DAT and Parquet files preserving directory layout.",
    )

    parser.add_argument(
        "--direction",
        choices=["dat-to-parquet", "parquet-to-dat"],
        default="dat-to-parquet",
        help="conversion direction (default: dat-to-parquet)",
    )

    parser.add_argument(
        "-s",
        "--src",
        default="out",
        help="source root directory (default: out)",
    )

    parser.add_argument(
        "-d",
        "--dst",
        default=None,
        help="destination root directory (default: same as source)",
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="overwrite existing destination files",
    )

    parser.add_argument(
        "--day-from",
        type=int,
        default=None,
        help="inclusive start day-of-year filter (1..366)",
    )

    parser.add_argument(
        "--day-to",
        type=int,
        default=None,
        help="inclusive end day-of-year filter (1..366)",
    )

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    src_root = Path(args.src).expanduser().resolve()
    dst_root = Path(args.dst).expanduser().resolve() if args.dst else src_root

    day_from = args.day_from
    day_to = args.day_to

    if day_from is not None and not (1 <= day_from <= 366):
        print("error: --day-from must be in range 1..366")
        return 1

    if day_to is not None and not (1 <= day_to <= 366):
        print("error: --day-to must be in range 1..366")
        return 1

    if day_from is not None and day_to is not None and day_from > day_to:
        print("error: --day-from must be less than or equal to --day-to")
        return 1

    if not src_root.is_dir():
        print(f"error: source directory does not exist: {src_root}")
        return 1

    try:
        converted = convert_tree(
            src_root=src_root,
            dst_root=dst_root,
            direction=args.direction,
            overwrite=args.overwrite,
            day_from=day_from,
            day_to=day_to,
        )
    except Exception as err:  # pragma: no cover
        print(f"error: {err}")
        return 1

    # for rec in converted:
    #     print(f"converted: {rec.source} -> {rec.destination}")

    # print(f"done: {len(converted)} file(s) converted")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
