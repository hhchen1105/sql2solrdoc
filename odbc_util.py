import pyodbc
import sys


def init_db(database_name='', charset='utf8'):
    db_info = get_db_info()
    db = None
    if sys.platform.startswith('linux'):
        db = pyodbc.connect('DSN=%s;UID=%s;PWD=%s;DATABASE=%s;' % (
            db_info['dsn'], db_info['user'], db_info['passwd'], database_name))
    else:  # assuming windows...
        db = pyodbc.connect('DRIVER=%s; SERVER=%s; DATABASE=%s; UID=%s; PWD=%s' % (
            db_info['DRIVER'], db_info['host'], database_name, db_info['user'], db_info['passwd']))
    cursor = db.cursor()

    return db, cursor


def close_db(db, cursor):
    cursor.close()
    db.close()


def _is_this_a_setting_line(line):
    line = line.strip()
    if line == '':
        return False
    if line[0] == '#':
        return False
    return True


def get_db_info():
    db_info = {}
    f = open('settings/odbc_settings', 'r')
    for line in f:
        if not _is_this_a_setting_line(line):
            continue
        line = line.split('#')[0]  # remove comments at the end of the line
        fields = line.strip().split(':')
        if len(fields) < 2:
            raise Exception("Wrong format in the solr setting file")
        field = fields[0].strip()
        val = ':'.join(fields[1:]).strip()
        if (val[0] == '"' and val[-1] == '"') or (val[0] == "'" and val[-1] == "'"):
            val = val[1:-1]
        db_info[field] = val
    f.close()
    return db_info

