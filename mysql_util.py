import MySQLdb
import MySQLdb.cursors

import datetime
import os
import sys


def init_db(charset='utf8', create_if_not_exist=False):
    db_info = get_db_info()
    db = MySQLdb.connect(host=db_info['host'], user=db_info['user'], \
            passwd=db_info['passwd'], unix_socket=db_info['socket'])
    cursor = db.cursor()
    if not does_db_exist(db_info['db']):
        if create_if_not_exist:
            cursor.execute("CREATE DATABASE " + db_info['db'] + " DEFAULT CHARACTER SET " + charset)
        else:
            close_db(db, cursor)
            raise Exception('Database ' + db_info['db'] + ' does not exist')
    db.select_db(db_info['db'])

    db.set_character_set(charset)
    cursor.execute('SET NAMES ' + charset + ';')
    cursor.execute('SET CHARACTER SET ' + charset + ';')
    cursor.execute('SET character_set_connection=' + charset + ';')
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
    db_info = { }
    f = open('settings/mysql_settings', 'r')
    for line in f:
        if not _is_this_a_setting_line(line):
            continue
        line = line.split('#')[0] # remove comments at the end of the line
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


def does_db_exist(db_name):
    db_info = get_db_info()
    db = MySQLdb.connect(host=db_info['host'], user=db_info['user'], \
            passwd=db_info['passwd'], unix_socket=db_info['socket'])
    cursor = db.cursor()

    cursor.execute("""SELECT schema_name FROM information_schema.schemata """ + \
         """WHERE schema_name = %s""", [db_name])
    row = cursor.fetchone()

    cursor.close()
    db.close()
    return row != None


def does_table_exist(table_name):
    db_info = get_db_info()
    db, cursor = init_db()
    cursor.execute("""SELECT * FROM information_schema.tables """ + \
            """WHERE table_schema = %s AND table_name = %s""", [db_info['db'], table_name])
    row = cursor.fetchone()
    close_db(db, cursor)
    return row != None


def drop_table(table_name):
    db, cursor = init_db()
    if does_table_exist(table_name):
        cursor.execute("""DROP TABLE """ + table_name)
    close_db(db, cursor)


def get_tables():
    db, cursor = init_db()
    cursor.execute("SHOW TABLES")
    rows = cursor.fetchall()
    close_db(db, cursor)
    return [r[0] for r in rows]


def backup_all_tables(backup_dir, is_archive=True):
    backup_dir = os.path.realpath(backup_dir)
    print backup_dir
    if not os.path.isdir(backup_dir):
        os.mkdir(backup_dir)
    db_info = get_db_info()
    tables = get_tables()
    num_tables = len(tables)
    print('Dumping sql files:')
    for i, table in enumerate(tables):
        sys.stdout.write("%s (%d/%d)\n" % (table, i+1, num_tables))
        filename = os.path.join(backup_dir, table + ".sql")
        dump_cmd = "mysqldump -u" + db_info['user'] +" -p"+ db_info['passwd'] + \
                " " + db_info['db'] + " '" + table + "' > '"+filename+"'"
        os.system(dump_cmd)

    if is_archive:
        print('Making tar-gz file...')
        targz_cmd = 'tar -zcvf ' + \
                os.path.join(backup_dir, '..', \
                    db_info['db'] + datetime.date.today().strftime("%m%d%Y") + '.tar.gz ') + \
                os.path.join(backup_dir, '*')
        os.system(targz_cmd)


def does_col_exist(table_name, column_name):
    db_info = get_db_info()
    db, cursor = init_db()
    cursor.execute("""SELECT * FROM information_schema.columns """ + \
            """WHERE table_schema = %s AND table_name = %s AND column_name LIKE %s""", \
            [db_info['db'], table_name, column_name])
    row = cursor.fetchone()
    close_db(db, cursor)
    return row != None


def add_index(tbl_name, col_name, idx_type):
    if idx_type.lower() not in ['index', 'unique index', 'fulltext']:
        raise Exception('index type can only be "index" or "unique index"')

    if not does_table_exist(tbl_name):
        raise Exception("Table " + tbl_name + " does not exist in the database")
    if not does_col_exist(tbl_name, col_name):
        raise Exception("Column " + col_name + " does not exist in table " + tbl_name)

    db, cursor = init_db()
    cursor.execute('ALTER TABLE ' + tbl_name + ' ADD ' + idx_type + '(' + col_name + ')')
    close_db(db, cursor)

