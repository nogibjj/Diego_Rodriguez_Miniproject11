"""handles cli commands"""

import sys
import argparse
from mylib.extract import extract
from mylib.transform_load import load
from mylib.query import (
    general_query,
)


def handle_arguments(args):
    """Add action based on initial calls"""
    parser = argparse.ArgumentParser(description="ETL-Query script")
    parser.add_argument(
        "action",
        choices=["extract", "transform_load", "general_query"],
        help="Action to perform (extract, transform_load, general_query).",
    )

    # Add query argument if the action is general_query
    if len(args) > 0 and args[0] == "general_query":
        parser.add_argument("query", help="The SQL query to execute.")

    # Parse arguments
    return parser.parse_args(args)


def main():
    """handles all the cli commands"""
    args = handle_arguments(sys.argv[1:])

    if args.action == "extract":
        print("Extracting data...")
        extract()
    elif args.action == "transform_load":
        print("Transforming data...")
        load()
    elif args.action == "general_query":
        general_query(args.query)
    else:
        print(f"Unknown action: {args.action}")


if __name__ == "__main__":
    main()
