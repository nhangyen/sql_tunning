import cx_Oracle
import os
import argparse
from datetime import datetime
import re

# ----------------------------
# Helpers
# ----------------------------
def get_dsn(host, port, service):
    return cx_Oracle.makedsn(host, port, service_name=service)

def fetch_source(cur, name, obj_type, today):
    cur.execute("""
        SELECT text
        FROM all_source
        WHERE name = :name
          AND type = :type
        ORDER BY line
    """, name=name.upper(), type=obj_type.upper())

    body = "".join(row[0] for row in cur)

    if not body.strip():
        raise RuntimeError("Object source not found (check name/type/schema).")

    new_name = f"{name.upper()}_BK_{today}"

    header_pattern = rf"^\s*(FUNCTION|PROCEDURE|PACKAGE|PACKAGE\s+BODY)\s+{name}\b"
    body = re.sub(
        header_pattern,
        rf"\1 {new_name}",
        body,
        flags=re.IGNORECASE | re.MULTILINE
    )

    source = "CREATE OR REPLACE " + body + ";"
    return source, new_name

def prepare_for_compile(source):
    stmt = source.strip()
    if stmt.endswith(";"):
        stmt = stmt[:-1]  # remove SQL*Plus semicolon
    return stmt

# ----------------------------
# Main
# ----------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--service", required=True)
    parser.add_argument("--user", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--name", required=True)
    parser.add_argument("--type", required=True)

    args = parser.parse_args()

    dsn = get_dsn(args.host, args.port, args.service)
    conn = cx_Oracle.connect(args.user, args.password, dsn)
    cur = conn.cursor()

    try:
        today = datetime.now().strftime("%Y%m%d")

        # ✅ UNPACK HERE
        source, new_name = fetch_source(cur, args.name, args.type, today)

        os.makedirs("bk", exist_ok=True)
        filename = f"bk/{new_name}.sql"

        with open(filename, "w", encoding="utf-8") as f:
            f.write(source)

        print(f"✅ Source saved to {filename}")

        compile_stmt = prepare_for_compile(source)
        cur.execute(compile_stmt)
        conn.commit()

        print(f"✅ {args.type.upper()} {new_name} compiled successfully")

    except Exception as e:
        print("❌ Error:")
        print(e)

    finally:
        cur.close()
        conn.close()

# ----------------------------
if __name__ == "__main__":
    main()
