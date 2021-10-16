#
# Assignment2 Interface
#

import psycopg2
import sys
import threading

# Do not close the connection inside this file i.e. do not perform openConnection.close()
def parallelJoin (pointsTable, rectsTable, outputTable, outputPath, openConnection):
    cur = openConnection.cursor()
    cur.execute('DROP TABLE IF EXISTS final_join')
    cur.execute("CREATE TABLE final_join (rectangleGeom geometry, _count bigint)")
    createFragments(cur)
    thread1 = thread('p1','r1',cur)
    thread2 = thread('p2','r2',cur)
    thread3 = thread('p3','r3',cur)
    thread4 = thread('p4','r4',cur)
    thread1.start()
    thread2.start()
    thread3.start()
    thread4.start()
    thread1.join()
    thread2.join()
    thread3.join()
    thread4.join()
    cur.execute("SELECT DISTINCT rectangleGeom,_count  from final_join order by _count asc")
    rows = cur.fetchall()
    f = open(outputPath, "a")
    for row in rows:
        f.write(str(row[1])+'\n')
    f.close()
    openConnection.commit()

class thread(threading.Thread):
    def __init__(self, pointsTable, rectsTable, cur):
        threading.Thread.__init__(self)
        self.pointsTable = pointsTable
        self.rectsTable = rectsTable
        self.cur = cur

    def run(self):
        self.cur.execute(
            'INSERT INTO final_join (rectangleGeom ,_count) SELECT ' + self.rectsTable
            + '.geom, count( ' + self.pointsTable + '.geom) AS count FROM ' + self.rectsTable
            + '.geom, count( ' + self.pointsTable + '.geom) AS count FROM ' + self.rectsTable
            + ' JOIN ' + self.pointsTable + ' ON st_contains(' + self.rectsTable + '.geom,' + self.pointsTable + '.geom) GROUP BY '
            + self.rectsTable + '.geom order by count asc')

def createFragments(cur):
    cur.execute('select * from rectangles order by longitude2 desc')
    highest_longitude = cur.fetchone()[3]
    cur.execute('select * from rectangles order by longitude1 asc')
    lowest_longitude = cur.fetchone()[1]
    cur.execute('select max(st_xmax(geom) - st_xmin(geom)) from rectangles')
    largest_width = cur.fetchone()[0]
    fragmentation_point = (highest_longitude - lowest_longitude) / 4
    cur.execute('DROP TABLE IF EXISTS p1')
    cur.execute('DROP TABLE IF EXISTS p2')
    cur.execute('DROP TABLE IF EXISTS p3')
    cur.execute('DROP TABLE IF EXISTS p4')
    cur.execute('DROP TABLE IF EXISTS r1')
    cur.execute('DROP TABLE IF EXISTS r2')
    cur.execute('DROP TABLE IF EXISTS r3')
    cur.execute('DROP TABLE IF EXISTS r4')
    cur.execute(
        'CREATE TABLE r1 AS select * from rectangles where longitude2 <= ' + str(
            lowest_longitude + fragmentation_point + largest_width))
    cur.execute(
        'CREATE TABLE r2 AS select * from rectangles where longitude1 >= ' + str(
            lowest_longitude + fragmentation_point - largest_width)
        + ' and ' + ' longitude2 <= ' + str(lowest_longitude + (2 * fragmentation_point) + largest_width))
    cur.execute(
        'CREATE TABLE r3 AS select * from rectangles where longitude1 >= ' + str(
            lowest_longitude + (2 * fragmentation_point) - largest_width)
        + ' and ' + ' longitude2 <= ' + str(lowest_longitude + (3 * fragmentation_point) + largest_width))
    cur.execute(
        'CREATE TABLE r4 AS select * from rectangles where longitude1 >= ' + str(
            lowest_longitude + (3 * fragmentation_point) - largest_width))
    cur.execute(
        'CREATE TABLE p1 AS select * from points where longitude <= ' + str(
            lowest_longitude + fragmentation_point + largest_width))
    cur.execute(
        'CREATE TABLE p2 AS select * from points where longitude >= ' + str(
            lowest_longitude + fragmentation_point - largest_width)
        + ' and ' + ' longitude <= ' + str(lowest_longitude + (2 * fragmentation_point) + largest_width))
    cur.execute(
        'CREATE TABLE p3 AS select * from points where longitude >= ' + str(
            lowest_longitude + (2 * fragmentation_point) - largest_width)
        + ' and ' + ' longitude <= ' + str(lowest_longitude + (3 * fragmentation_point) + largest_width))
    cur.execute(
        'CREATE TABLE p4 AS select * from points where longitude >= ' + str(
            lowest_longitude + (3 * fragmentation_point) - largest_width))

################### DO NOT CHANGE ANYTHING BELOW THIS #############################

# Donot change this function
def getOpenConnection(user='postgres', password='12345', dbname='dds_assignment2'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='dds_assignment2'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(tablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if tablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (tablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()


