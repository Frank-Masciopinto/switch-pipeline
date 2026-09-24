"""DDL and data generation for the demo source table: TPC-H ORDERS joined with CUSTOMER."""

from switch_pipeline.adapter.snowflake import SourceTable

# Business columns besides the key; the three contract columns come from settings.
ORDER_COLUMNS: tuple[tuple[str, str], ...] = (
    ("O_CUSTKEY", "NUMBER(38,0) NOT NULL"),
    ("O_ORDERSTATUS", "VARCHAR(1)"),
    ("O_TOTALPRICE", "NUMBER(12,2)"),
    ("O_ORDERDATE", "DATE"),
    ("O_ORDERPRIORITY", "VARCHAR(15)"),
    ("O_CLERK", "VARCHAR(15)"),
    ("O_SHIPPRIORITY", "NUMBER(38,0)"),
    ("O_COMMENT", "VARCHAR(79)"),
    ("C_NAME", "VARCHAR(25)"),
    ("C_MKTSEGMENT", "VARCHAR(10)"),
    ("C_NATIONKEY", "NUMBER(38,0)"),
    ("C_ACCTBAL", "NUMBER(12,2)"),
)


def column_names(table: SourceTable) -> list[str]:
    return [
        table.key_column,
        *(name for name, _ in ORDER_COLUMNS),
        table.version_column,
        table.updated_at_column,
    ]


def create_table_sql(table: SourceTable) -> str:
    columns = [
        f"{table.key_column} NUMBER(38,0) NOT NULL",
        *(f"{name} {definition}" for name, definition in ORDER_COLUMNS),
        f"{table.version_column} NUMBER(38,0) NOT NULL",
        # Microsecond precision on purpose: it is what Python datetimes hold. With
        # Snowflake's default nanoseconds the persisted watermark would be truncated
        # and the last row of every batch read (and emitted) again.
        f"{table.updated_at_column} TIMESTAMP_NTZ(6) NOT NULL",
        f"CONSTRAINT {table.table}_PK PRIMARY KEY ({table.key_column})",
    ]
    body = ",\n    ".join(columns)
    return f"CREATE OR REPLACE TABLE {table.qualified_name} (\n    {body}\n)"


def sample_share_insert_sql(table: SourceTable, *, sample_schema: str, row_count: int) -> str:
    return (
        f"INSERT INTO {table.qualified_name} ({', '.join(column_names(table))})\n"
        "SELECT o.O_ORDERKEY, o.O_CUSTKEY, o.O_ORDERSTATUS, o.O_TOTALPRICE, o.O_ORDERDATE,\n"
        "       o.O_ORDERPRIORITY, o.O_CLERK, o.O_SHIPPRIORITY, o.O_COMMENT,\n"
        "       c.C_NAME, c.C_MKTSEGMENT, c.C_NATIONKEY, c.C_ACCTBAL, 1, SYSDATE()\n"
        f"FROM {sample_schema}.ORDERS o\n"
        f"JOIN {sample_schema}.CUSTOMER c ON c.C_CUSTKEY = o.O_CUSTKEY\n"
        "ORDER BY o.O_ORDERKEY\n"
        f"LIMIT {int(row_count)}"
    )


def synthetic_insert_sql(table: SourceTable, *, key_offset: int, row_count: int) -> str:
    """Deterministic TPC-H-shaped rows with keys key_offset+1 .. key_offset+row_count,
    generated inside the warehouse (standard Snowflake SQL, no share required)."""
    return (
        f"INSERT INTO {table.qualified_name} ({', '.join(column_names(table))})\n"  # noqa: S608 - validated identifiers
        f"SELECT {int(key_offset)} + n,\n"
        "       1 + MOD(n * 7919, 150000),\n"
        "       DECODE(MOD(n, 3), 0, 'O', 1, 'F', 'P'),\n"
        "       (900 + MOD(n * 104729, 50000000) / 100)::NUMBER(12,2),\n"
        "       DATEADD(day, MOD(n * 31, 2405), '1992-01-01'::DATE),\n"
        "       DECODE(MOD(n, 5), 0, '1-URGENT', 1, '2-HIGH', 2, '3-MEDIUM', 3, '4-NOT SPECIFIED',"
        " '5-LOW'),\n"
        "       'Clerk#' || LPAD(TO_VARCHAR(1 + MOD(n * 13, 1000)), 9, '0'),\n"
        "       0,\n"
        f"       'synthetic order ' || TO_VARCHAR({int(key_offset)} + n),\n"
        "       'Customer#' || LPAD(TO_VARCHAR(1 + MOD(n * 7919, 150000)), 9, '0'),\n"
        "       DECODE(MOD(n, 5), 0, 'AUTOMOBILE', 1, 'BUILDING', 2, 'FURNITURE', 3, 'HOUSEHOLD',"
        " 'MACHINERY'),\n"
        "       MOD(n, 25),\n"
        "       (MOD(n * 3571, 1099999) / 100 - 999.99)::NUMBER(12,2),\n"
        "       1,\n"
        "       SYSDATE()\n"
        "FROM (SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) AS n\n"
        f"      FROM TABLE(GENERATOR(ROWCOUNT => {int(row_count)})))"
    )
