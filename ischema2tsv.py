#!/usr/bin/env python3

import argparse
import getpass
import mysql.connector
from contextlib import contextmanager


@contextmanager
def create_connection(config):
    try:
        cnx = mysql.connector.connect(**config)
        yield cnx
    finally:
        cnx.close()


class TableDao:
    @staticmethod
    @contextmanager
    def find_by_table_schema(cnx, table_schema: str):
        query = """
        SELECT TABLE_NAME FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = %s
          AND TABLE_TYPE = 'BASE TABLE'
        """
        cursor = cnx.cursor()

        try:
            cursor.execute(query, [table_schema])
            yield cursor
        finally:
            cursor.close


class ColumnDao:
    @staticmethod
    @contextmanager
    def find_by_table_schema_and_table_name(cnx, table_schema: str, table_name: str):
        query = """
        SELECT
          C.COLUMN_NAME
        , C.ORDINAL_NUMBER
        , C.COLUMN_DEFAULT
        , C.IS_NULLABLE
        , C.COLUMN_TYPE
        , T.CONSTRAINT_TYPE
        , K.REFERENCED_TABLE_NAME
        , K.REFERENCED_COLUMN_NAME
          FROM information_schema.COLUMNS C
          LEFT JOIN information_schema.TABLE_CONSTRAINTS T
            ON C.TABLE_SCHEMA = T.TABLE_SCHEMA
           AND C.TABLE_NAME = T.TABLE_NAME
          LEFT JOIN information_schema.KEY_COLUMN_USAGE K
            ON C.TABLE_SCHEMA = K.TABLE_SCHEMA
           AND C.TABLE_NAME = K.TABLE_NAME
           AND C.COLUMN_NAME = K.COLUMN_NAME
         WHERE TABLE_SCHEMA=%s
           AND TABLE_NAME=%s
         ORDER BY C.ORDINAL_POSITION
        """
        cursor = cnx.cursor()

        try:
            cursor.execute(query, [table_schema, table_name])
            yield cursor
        finally:
            cursor.close()


class TableConstraintDao:
    @staticmethod
    @contextmanager
    def find_by_table_schema_and_table_name(cnx, table_schema: str, table_name: str):
        query = """
        SELECT T.CONSTRAINT_TYPE, C.COLUMN_NAME, C.REFERENCED_TABLE_NAME, C.REFERENCED_COLUMN_NAME
          FROM information_schema.TABLE_CONSTRAINTS T
         INNER JOIN information_schema.KEY_COLUMN_USAGE C
            ON T.CONSTRAINT_CATALOG = C.CONSTRAINT_CATALOG
           AND T.CONSTRAINT_SCHEMA = C.CONSTRAINT_SCHEMA
           AND T.CONSTRAINT_NAME = C.CONSTRAINT_NAME
           AND T.TABLE_SCHEMA = C.TABLE_SCHEMA
           AND T.TABLE_NAME = C.TABLE_NAME
         WHERE TABLE_SCHEMA=%s
           AND TABLE_NAME=%s
         ORDER BY ORDINAL_POSITION
        """
        cursor = cnx.cursor()

        try:
            cursor.execute(query, [table_schema, table_name])
            yield cursor
        finally:
            cursor.close()


class ColumnDef:
    def __init__(self, name: str):
        self.name = name


class TableDef:
    def __init__(self, name: str):
        self.name = name


def create_argparser():
    parser = argparse.ArgumentParser(
        description='Dump MySQL information schema.')
    parser.add_argument('-u', '--user', help='user login name (default: current user)',
                        default=getpass.getuser())
    parser.add_argument('-p', '--password',
                        help='user password (default:empty)', default='')
    parser.add_argument(
        '-P', '--port', help='port number (default:3306)', default='3306')
    parser.add_argument(
        '-H', '--host', help='hostname (default:localhost)', default='localhost')
    parser.add_argument('database', help='database name')
    return parser


def main():
    args = create_argparser().parse_args()
    config = {
        'user': args.user,
        'password': args.password,
        'host': args.host,
        'port': args.port,
        'database': args.database
    }

    with create_connection(config) as cnx:
        with TableDao.find_by_table_schema(cnx, args.database) as cursor:
            tables = cursor.fetchall()

            for table_name in tables:
                print(table_name)

                with ColumnDao.find_by_table_schema_and_table_name(cnx, args.database, 'titles') as cursor:
                    for (column_name, column_default, is_nullable, column_type) in cursor:
                        print(column_name, column_default,
                              is_nullable, column_type)


if __name__ == '__main__':
    main()
