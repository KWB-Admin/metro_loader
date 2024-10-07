import psycopg2 as pg
from psycopg2 import sql
from datetime import datetime
import polars as pl

user, host, password = ["", "", ""]


def load(
    dbname: str, schema: str, table_name: str, data_path: str, prim_key: str = "None"
):
    connection = get_pg_connecter(dbname)
    columns_and_dtypes = get_columns_and_dtypes(connection, table_name)
    data = read_data(data_path)
    new_columns = {
        data.columns[i]: columns_and_dtypes[i][0] for i in range(len(data.columns))
    }
    data = data.rename(new_columns)
    for items in columns_and_dtypes:
        col = items[0]
        dtype = items[1]
        if dtype == "integer":
            data = data.with_columns(pl.col(col).cast(pl.Int16))
        elif dtype == "text":
            data = data.with_columns(pl.col(col).cast(pl.String))
        elif dtype in ["double precision", "real"]:
            data = data.with_columns(pl.col(col).cast(pl.Float64))
        elif dtype == "date":
            data = data.with_columns(
                pl.col(col).str.strptime(pl.Date, format="%m/%d/%Y")
            )

    write_data_to_tables(data.to_numpy(), dbname, schema, table_name, prim_key)


def read_data(data_path: str) -> pl.DataFrame:
    if "csv" in data_path:
        return pl.read_csv(data_path)
    elif "xlsx" in data_path:
        return pl.read_excel(data_path)
    elif "parquet" in data_path:
        return pl.read_parquet(data_path)
    else:
        raise "New file type detected, please refactor"


def write_data_to_tables(
    data, db_name: str, schema: str, table: str, prim_key: str = "None"
):
    con = get_pg_connecter(db_name=db_name)
    check_tables_exists(con, schema=schema, table=table)
    columns_and_dtypes = get_columns_and_dtypes(con, table)
    try:
        cur = con.cursor()
        for row in data:
            query = build_insert_query(
                row,
                schema=schema,
                table=table,
                columns_and_dtypes=columns_and_dtypes,
                prim_key=prim_key,
            )
            cur.execute(query)
        cur.close()
        con.close()
        print(f"Data was successfully written to {table} table in {db_name} database")
    except:
        con.close()
        print("Data did not get written")


def build_insert_query(
    data: list, schema: str, table: str, columns_and_dtypes: dict, prim_key: str
):
    col_names = sql.SQL(", ").join(sql.Identifier(col[0]) for col in columns_and_dtypes)
    values = sql.SQL(" , ").join(sql.Literal(val) for val in data)
    if prim_key == "None":
        query = sql.SQL(
            """
            INSERT INTO {schema}.{table} ({col_names}) VALUES ({values})
            """
        ).format(
            schema=sql.Identifier(schema),
            table=sql.Identifier(table),
            col_names=col_names,
            values=values,
        )
    elif "date_added" in columns_and_dtypes:
        query = sql.SQL(
            """
            INSERT INTO {schema}.{table} ({col_names}) VALUES ({values})
            ON CONFLICT {prim_key} DO UPDATE
            SET 'date_added' = {date}
            """
        ).format(
            schema=sql.Identifier(schema),
            table=sql.Identifier(table),
            col_names=col_names,
            values=values,
            prim_key=sql.SQL(prim_key),
            date=sql.Literal(datetime.today()),
        )
    else:
        query = sql.SQL(
            """
            INSERT INTO {schema}.{table} ({col_names}) VALUES ({values})
            ON CONFLICT ({prim_key}) DO NOTHING
            """
        ).format(
            schema=sql.Identifier(schema),
            table=sql.Identifier(table),
            col_names=col_names,
            values=values,
            prim_key=sql.SQL(prim_key),
            date=sql.Literal(datetime.today()),
        )

    return query


def get_pg_connecter(db_name: str) -> pg.extensions.connection:
    try:
        con = pg.connect(
            f"dbname={db_name} user='{user}' host='{host}' password='{password}'"
        )
        con.autocommit = True
        print("Database Exists.")
        return con

    except pg.OperationalError as Error:
        con = pg.connect(f"user='{user}' host='{host}' password='{password}'")
        con.autocommit = True
        cur = con.cursor()
        cur.execute(
            sql.SQL("CREATE DATABASE {db_name};").format(sql.Identifier(db_name))
        )
        con.close()
        print("Database Created.")
        return pg.connect(
            f"dbname='{db_name}' user='{user}' host='{host}' password='{password}'"
        )


def get_columns_and_dtypes(con, table: str) -> dict[str, str]:
    cur = con.cursor()
    command = sql.SQL(
        """
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = {table} 
        order by ordinal_position
        """
    ).format(table=sql.Literal(table))
    try:
        cur.execute(command)
        return cur.fetchall()
    except pg.OperationalError as Error:
        raise Error


def check_tables_exists(con, schema, table: str):
    cur = con.cursor()
    command = sql.SQL(
        """
        Select * from {schema}.{table} limit 1  
        """
    ).format(
        schema=sql.Identifier(schema),
        table=sql.Identifier(table),
    )
    try:
        cur.execute(command)
        if isinstance(cur.fetchall(), list):
            print("Table Exists.")
    except pg.OperationalError as Error:
        raise Error
