#!/bin/python3

import re
import sys
from typing import List, Set
import mysql.connector
import mysql.connector.cursor
from pandas.io.clipboard import clipboard_get
import argparse
import pystuff

T = '    '  # four space tab
TT = T + T  # two tabs

# sql type -> java type mapping (dict)
sql_java_type_mapping = {
    'varchar': 'String',
    'datetime': 'LocalDateTime',
    'timestamp': 'LocalDateTime',
    'int': 'Integer',
    'smallint': 'Integer',
    'tinyint': 'Integer',
    'short': 'Integer',
    'bigint': 'Long',
    'decimal': 'BigDecimal',
    'char': 'String',
    'text': 'String',
    'json': 'String'
}

def first_char_lower(s: str) -> str:
    """Make first char lowercase"""
    return s[0:1].lower() + s[1:]


def first_char_upper(s: str) -> str:
    """Make first char uppercase"""
    return s[0:1].upper() + s[1:]


def to_camel_case(s: str) -> str:
    """
    Convert a string to camel case
    """
    is_prev_uc = False  # is prev in uppercase
    s = s.lower()
    ccs = ''
    for i in range(len(s)):
        ci = s[i]
        if ci == '_':
            is_prev_uc = True
        else:
            if is_prev_uc:
                ccs += ci.upper()
                is_prev_uc = False
            else:
                ccs += ci
    return ccs

def parse_args():
    ap = argparse.ArgumentParser(prog="java_entity_gen.py by Yongj.Zhuang")
    req = ap.add_argument_group('required arguments')
    req.add_argument("-user", '-u', help="Username of database connection", type=str, required=True)
    req.add_argument("-database", '-db', help="Database name", type=str, required=True)

    ap.add_argument("-author", help="Author name of the class", type=str, default="", required=False)
    ap.add_argument("-password", help="Password of database connection (by default it's empty string)", type=str, default="", required=False)
    ap.add_argument("-host", help="Host of database connection (by default it's localhost)", type=str, default="localhost", required=False)
    ap.add_argument("-table", help="Table name, there can be multiple table names delimited by comma", type=str, required=False)
    ap.add_argument("-excl", help="Excluded fields, there can be multiple table names delimited by comma", type=str, required=False)
    ap.add_argument("-output", help="Where the generated java class file is written to", type=str, required=False)
    ap.add_argument("-extends", help="Where the generated java class file is written to", type=str, required=False)

    ap.add_argument("-mybatis", help="Enable mybatis-plus feature, e.g., @TableField, @TableName, etc", action="store_true", required=False)
    ap.add_argument("-lambok", help="Enable Lombok feature, e.g., @Data on class", action="store_true", required=False)
    return ap.parse_args()


def extract_sql_type(sql_type):
    m = re.match("^\s*(\w+)\(\d+\)\s*$", sql_type)
    if not m: return sql_type
    return m[1]


def parseSqlTable(cursor, table, args):
    table_name: str = table
    table_comment: str  = ''

    fields: list[SQLField] = []
    cursor.execute(f"DESC {table}")
    rl: list = cursor.fetchall()
    # print(rl)

    for r in rl:
        f = SQLField(
            field_name=r[0],
            sql_type=extract_sql_type(r[1]),
            comment=""
                     )
        fields.append(f)

    ci = fetch_column_info(cursor, table)
    for f in fields:
        f.comment = ci[f.sql_field_name]

    table_comment = fetch_table_comment(cursor, table)
    return SQLTable(table_name, table_comment, fields)


def fetch_table_comment(cursor, table):
    cursor.execute(f"SELECT table_comment FROM information_schema.tables WHERE TABLE_NAME = '{table}'")
    r = cursor.fetchall()
    return r[0][0]


def fetch_column_info(cursor, table):
    cursor.execute(f"SELECT column_name, column_comment FROM information_schema.columns WHERE TABLE_NAME = '{table}'")
    r = cursor.fetchall()
    m = {}
    for row in r:
        m[row[0]] = row[1]
    return m


def generate_java_class(table: "SQLTable", ap, spec_class_name: None, package: None) -> str:
    """
    Generate Java class, and return it as a string

    :param table SQLTable object
    :param ctx Context object
    :param spec_class_name specified class name (if None, it will attempt to generate one based the class name used in DDL)
    :param package package for the class (optional)
    :return generated java class as a str
    """

    # features
    mbp_ft = ap.mybatis
    lambok_ft = ap.lambok

    # if the class name is specified, we used the given one instead of the one parsed from CREATE TABLE statement
    class_name = spec_class_name if spec_class_name is not None else table.supply_java_class_name()
    s = ''

    '''
        For package
    '''
    if package is not None:
        s += f"package {package};\n"

    '''
        For Imports
    '''
    if table.is_type_used('LocalDateTime'):
        s += "import java.time.*;\n"
    if table.is_type_used('BigDecimal'):
        s += "import java.math.*;\n"
    s += '\n'

    # for mybatis-plus only
    if mbp_ft:
        s += "import com.baomidou.mybatisplus.annotation.*;\n"
    s += '\n'

    # for lambok
    if lambok_ft:
        s += "import lombok.*;\n"

    # for inheritance
    if ap.extends:
        s += f"import {ap.extends.strip()};\n"

    s += '\n'
    s += '/**\n'
    s += f" * {table.table_comment}\n"

    # author
    if ap.author:
        s += ' *\n'
        s += f' * @author {ap.author}\n'

    s += ' */\n'

    if lambok_ft:
        s += '@Data\n'

    if mbp_ft:
        s += f"@TableName(value = \"{table.table_name}\", autoResultMap = true)\n"

    s += f"public class {class_name}"
    if ap.extends:
        canonical = ap.extends
        r = canonical.rfind('.')
        s += f" extends {canonical[r + 1:]}"

    s += " {\n\n"

    '''
        Fields
    '''
    for f in table.fields:
        s += f"{T}/** {f.comment} */\n"
        if mbp_ft:
            if pystuff.str_matches(f.sql_field_name, 'id'):
                s += f"{T}@TableId(type = IdType.AUTO)\n"
            else:
                s += f"{T}@TableField(\"{f.sql_field_name}\")\n"
        s += f"{T}private {f.java_type} {f.java_field_name};\n\n"

    '''
        Getter, setters only appended when lambok is not used
    '''
    if not lambok_ft:
        for f in table.fields:
            us = first_char_upper(f.java_field_name)
            s += f"{T}public {f.java_type} get{us}() {{\n"
            s += f"{TT}return this.{f.java_field_name}\n"
            s += f"{T}}}\n\n"

            s += f"{T}public void set{us}({f.java_type} {f.java_field_name}) {{\n"
            s += f"{TT}this.{f.java_field_name} = {f.java_field_name};\n"
            s += f"{T}}}\n\n"

    s += '}\n'
    return s


def to_java_type(sql_type: str) -> str:
    sql_type = sql_type.lower()

    if not sql_type in sql_java_type_mapping:
        raise ValueError(f"Unable to find corresponding java type for {sql_type}")

    return sql_java_type_mapping[sql_type]


class SQLField:
    def __init__(self, field_name: str, sql_type: str, comment: str):
        self.sql_field_name = field_name.replace('`', '')
        self.sql_type = sql_type
        self.comment = comment
        self.java_type = to_java_type(sql_type)
        self.java_field_name = to_camel_case(self.sql_field_name)

    def __str__(self):
        return f"Field: {self.sql_field_name} ({self.java_field_name}), type: {self.sql_type} ({self.java_type}), " \
               f"comment: \'{self.comment}\' "


class SQLTable:
    def __init__(self, table_name: str, table_comment: str, fields: List["SQLField"]):
        self.table_name = table_name
        self.table_comment = '' if table_comment is None else table_comment
        self.fields = fields
        self.java_type_set = set()
        for f in fields:
            self.java_type_set.add(f.java_type)

    def __str__(self):
        s = ''
        s += f"Table: {self.table_name}\n"
        s += f"Comment: {self.table_comment}\n"
        for f in self.fields:
            s += f" - {f}\n"
        return s

    def is_type_used(self, java_type: str) -> bool:
        """
        Check whether the java type is used in this table

        arg[0] - java type name
        """
        return java_type in self.java_type_set

    def supply_java_class_name(self) -> str:
        """
        Supply java class name based on the one used in DDL
        """
        return first_char_upper(to_camel_case(self.table_name))


def guess_package(path: str) -> str or None:
    hi = path.rfind("/")  # only works for unix like OS
    if hi == -1:
        return None

    pat = 'src/main/java'
    lo = path.find(pat)
    if lo == -1:
        return None

    return path[lo + len(pat) + 1: hi].replace('/', '.')


def get_clipboard_text():
    return clipboard_get()


if __name__ == '__main__':
    ap = parse_args()
    tables = ap.table.split(',')

    config = {
        'user': ap.user,
        'password': ap.password,
        'host': ap.host,
        'database': ap.database,
        'raise_on_warnings': True,
        'connect_timeout': 5000
    }
    cnx: mysql.connector.MySQLConnection = mysql.connector.connect(**config)
    cursor: mysql.connector.cursor.MySQLCursor = cnx.cursor()

    for i in range(len(tables)):
        # parse ddl
        table: SQLTable = parseSqlTable(cursor, tables[i], ap)
        print()
        print(table)

        # check whether file name is specified, it does affect the java class name that we are about to use
        java_class_name = None
        fn = ""

        if ap.output:
            fn: str = ap.output
            hi = fn.rfind('.java')

            if hi > -1:
                lo = fn.rfind('/')
                if lo == -1:
                    lo = 0
                else:
                    lo = lo + 1
                java_class_name = fn[lo: hi]
            else:
                print(f"fn: {fn}")
                fn = fn if fn.endswith("/") else fn + "/"

        if java_class_name == None:
            java_class_name = table.supply_java_class_name()
            fn = fn + java_class_name + ".java"

        # generate java class
        generated = generate_java_class(table, ap, java_class_name, guess_package(fn))

        # write to file
        with open(fn, "w") as f:
            f.write(generated)
            f.close()
        print(f"Java class generated and written to \'{fn}\'")

    cursor.close()
    cnx.close()
