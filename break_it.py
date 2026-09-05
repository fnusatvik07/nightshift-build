"""Break something on purpose, so the class can watch a signal notice.

    python break_it.py          break it
    python break_it.py --fix    put it back

What it breaks: the driver app renames its surge field, exactly the way a real
mobile release does. Nothing errors. No pipeline fails. The rows still land.
The only thing that changes is that a value the pricing team depends on
quietly stops arriving.

This is the most useful kind of failure to show people, because it is the kind
that a row count check will never find.

Nothing here touches the source systems. It writes to a copy of the driver app
collection inside our own schema, so `reset` undoes it completely.
"""
from __future__ import annotations

import argparse
import sys

import psycopg

from pipelines.lib.config import SCHEMA, dsn

BROKEN_PATH = "pricing.surge_multiplier"


def flip(fix: bool) -> int:
    with psycopg.connect(dsn(), autocommit=True) as c:
        exists = c.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema=%s AND table_name='bronze_driver_app'", (SCHEMA,)).fetchone()
        if not exists:
            print("  nothing to break yet. Run: python cli.py run all")
            return 1

        if fix:
            n = c.execute(f"UPDATE {SCHEMA}.bronze_driver_app SET surge = surge_backup "
                          f"WHERE surge IS NULL AND surge_backup IS NOT NULL").rowcount
            print(f"  put the surge value back on {n:,} records")
            print("  now run:  python cli.py board")
            return 0

        c.execute(f"ALTER TABLE {SCHEMA}.bronze_driver_app "
                  f"ADD COLUMN IF NOT EXISTS surge_backup NUMERIC(6,2)")
        c.execute(f"UPDATE {SCHEMA}.bronze_driver_app SET surge_backup = surge "
                  f"WHERE surge_backup IS NULL")
        # the app release: the field moved, so our reader finds nothing
        n = c.execute(f"""UPDATE {SCHEMA}.bronze_driver_app SET surge = NULL
                          WHERE ctid IN (SELECT ctid FROM {SCHEMA}.bronze_driver_app
                                         ORDER BY happened_at DESC LIMIT 12000)""").rowcount
        print(f"  the driver app moved surge to '{BROKEN_PATH}'")
        print(f"  {n:,} records now arrive with no surge value we can read")
        print("  nothing failed. no pipeline errored. now run:  python cli.py board")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--fix", action="store_true")
    return flip(ap.parse_args().fix)


if __name__ == "__main__":
    sys.exit(main())
