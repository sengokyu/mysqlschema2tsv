#!/usr/bin/env python3

from os import stat
import sys
import argparse
import getpass
import re
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
        , C.COLUMN_DEFAULT
        , C.IS_NULLABLE
        , C.COLUMN_TYPE
          FROM information_schema.COLUMNS C
         WHERE C.TABLE_SCHEMA=%s
           AND C.TABLE_NAME=%s
         ORDER BY C.ORDINAL_POSITION
        """
        cursor = cnx.cursor()

        try:
            cursor.execute(query, [table_schema, table_name])
            yield cursor
        finally:
            cursor.close()


class KeyColumnUsageDao:
    @staticmethod
    @contextmanager
    def find_by_table_schema_and_table_name_and_column_name(cnx, table_schema: str, table_name: str, column_name: str):
        query = """
        SELECT T.CONSTRAINT_TYPE, K.REFERENCED_TABLE_NAME, K.REFERENCED_COLUMN_NAME
          FROM information_schema.KEY_COLUMN_USAGE K
          LEFT JOIN information_schema.TABLE_CONSTRAINTS T
            ON K.TABLE_SCHEMA = T.TABLE_SCHEMA
           AND K.TABLE_NAME = T.TABLE_NAME
         WHERE K.TABLE_SCHEMA=%s
           AND K.TABLE_NAME=%s
           AND K.COLUMN_NAME=%s
        """
        cursor = cnx.cursor()

        try:
            cursor.execute(query, [table_schema, table_name, column_name])
            yield cursor
        finally:
            cursor.close()


class ColumnDef:
    column_type_pattern = re.compile('^(.+)\((.*)\)$')

    def __init__(self, table_name, column_name, column_type, is_nullable, column_default):
        self.table_name = table_name
        self.column_name = column_name
        self.set_column_type(column_type)
        self.is_nullable = is_nullable
        self.columun_default = column_default if column_default is not None else ''
        self.is_primary_key = ''
        self.foreign_key = ''

    def set_column_type(self, column_type):
        m = ColumnDef.column_type_pattern.match(column_type)
        if m:
            self.column_type = m.group(1)
            self.column_size = m.group(2)
        else:
            self.column_type = column_type
            self.column_size = ''

    def set_constraint(self, constraint_type, table_name, column_name):
        if constraint_type == 'PRIMARY KEY':
            self.is_primary_key = 'YES'
        elif constraint_type == 'FOREIGN KEY':
            self.foreign_key = '{}.{}'.format(table_name, column_name)

    def to_tuple(self):
        return (self.table_name, self.column_name,
                self.column_type,
                self.column_size,
                self.is_nullable, self.columun_default, self.is_primary_key,
                self.foreign_key)


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
    table_schema = args.database

    print('\t'.join(('Table', 'Column', 'Type', 'Size',
                     'Nullable', 'Default', 'PKEY', 'FKEY')))

    with create_connection(config) as cnx:
        with TableDao.find_by_table_schema(cnx, table_schema) as cursor:
            tables = cursor.fetchall()

        for tables_row in tables:
            table_name = tables_row[0]

            with ColumnDao.find_by_table_schema_and_table_name(cnx, table_schema, table_name) as cursor:
                columns = cursor.fetchall()

            for (column_name, column_default, is_nullable, column_type) in columns:
                output = ColumnDef(table_name, column_name,
                                   column_type, is_nullable, column_default)

                with KeyColumnUsageDao.find_by_table_schema_and_table_name_and_column_name(cnx, table_schema, table_name, column_name) as cursor:
                    column_usages = cursor.fetchall()

                for (constraint_type, referenced_table_name, referenced_column_name) in column_usages:
                    output.set_constraint(
                        constraint_type, referenced_table_name, referenced_column_name)

                print('\t'.join(output.to_tuple()))


if __name__ == '__main__':
    main()
