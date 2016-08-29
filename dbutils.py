# dbutils.py
# Better interface to MySQL from Python.
# Context manager abstracts out cursors.
#
# Author: Alex Kindel
# Date: 28 August 2016

import MySQLdb
import MySQLdb.cursors
from functools import wraps

class Database(object):

    def __init__(self, username, password, db, host='127.0.0.1', port=3306):
        self.username = username
        self.password = password
        self.db = db
        self.connection = MySQLdb.connect(host=host, port=port, user=username, passwd=password, db=db, cursorclass=MySQLdb.cursors.DictCursor)
        self.connection.autocommit(True)
        self.cursors = []

    def __enter__(self):
        curs = self.connection.cursor()
        self.cursors.append(curs)
        return curs

    def __exit__(self, *args):
        for cursor in self.cursors:
            cursor.close()
        self.connection.close()


def query(cursor, query, fetchall=False):
    cursor.execute(query)
    results = cursor.fetchall()
    if fetchall:
        return results
    else:
        return (row for row in results)

def insert(cursor, table, cols, vals):
    query = "INSERT INTO %s (`%s`) VALUES ('%s')" % (table, "`,`".join(cols), "','".join(vals))
    status = cursor.execute(query)
    return status

def with_db(dbcfg):
    def db_call(f):
        @wraps(f)
        def db_wrap(*args, **kwargs):
            with Database(**dbcfg) as db:
                return f(db, *args, **kwargs)
        return db_wrap
    return db_call



if __name__ == '__main__':

    dbcfg = {'username': 'unittest', 'password': 'unittest', 'db': 'unittest'}

    # Test queries
    print "Testing query()"
    from dbutils import query
    with Database(**dbcfg) as db:
        for result in query(db, 'SELECT * FROM unittest.unittest'):
            print result
        assert result

    # Test inserts
    print "Testing insert()"
    from dbutils import insert
    with Database(**dbcfg) as db:
        status = insert(db, table='unittest.unittest', cols=['b'], vals=['unittest'])
        assert status

    # Test decorator
    print "Testing @with_db"
    from dbutils import with_db

    @with_db(dbcfg)
    def get_results(db, query_text):
        for row in query(db, query_text):
            print row
    get_results('SELECT * FROM unittest.unittest')

    @with_db(dbcfg)
    def insert_many(db, table, cols, vals):
        for row in vals:
            status = insert(db, table, cols, row)
            print "Inserted another row."
    rows = [['u'], ['n'], ['i'], ['t'], ['t'], ['e'], ['s'], ['t']]
    insert_many(table='unittest.unittest', cols=['b'], vals=rows)
