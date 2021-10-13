#
# Assignment2 Interface
#

import psycopg2
import sys
from threading import Thread

# Do not close the connection inside this file i.e. do not perform openConnection.close()
def setupFragments (openConnection):
    cur = openConnection.cursor()
    cur.execute('select max(st_ymax(geom) - st_ymin(geom)) from rectangles')
    largest_height = cur.fetchone()[0]
    cur.execute('select * from rectangles order by latitude2 desc limit 1')
    highest_latitude = cur.fetchone()[3]
    cur.execute('select * from rectangles order by latitude1 asc limit 1')
    lowest_latitude = cur.fetchone()[1]
    fragmentation_point = (highest_latitude - lowest_latitude) / 4
    dropTable('pointsf1',cur)
    dropTable('pointsf2',cur)
    dropTable('pointsf3',cur)
    dropTable('pointsf4',cur)
    dropTable('rectsf1',cur)
    dropTable('rectsf2',cur)
    dropTable('rectsf3',cur)
    dropTable('rectsf4',cur)
    createFragmentsRectangles(cur, largest_height/2, fragmentation_point, lowest_latitude)
    createFragmentsPoints(cur, largest_height/2, fragmentation_point, lowest_latitude)

    print('fragments complete')
    cur.close()
    openConnection.commit()
    pass

def createFragmentsRectangles(cur, largest_height, fragmentation_point, lowest_latitude):
    cur.execute(
        'CREATE TABLE rectsf1 AS select * from rectangles where latitude2 <= ' + str(lowest_latitude + fragmentation_point + largest_height))
    cur.execute(
        'CREATE TABLE rectsf2 AS select * from rectangles where latitude1 >= ' + str(lowest_latitude + fragmentation_point - largest_height)
        + ' and ' + ' latitude2 <= ' + str(lowest_latitude + (2*fragmentation_point) + largest_height))
    cur.execute(
        'CREATE TABLE rectsf3 AS select * from rectangles where latitude1 >= ' + str(lowest_latitude + (2*fragmentation_point) - largest_height)
        + ' and ' + ' latitude2 <= ' + str(lowest_latitude + (3*fragmentation_point) + largest_height))
    cur.execute(
        'CREATE TABLE rectsf4 AS select * from rectangles where latitude1 >= ' + str(lowest_latitude + (3*fragmentation_point) - largest_height))

def createFragmentsPoints(cur, largest_height, fragmentation_point, lowest_latitude):
    cur.execute(
        'CREATE TABLE pointsf1 AS select * from points where latitude <= ' + str(lowest_latitude + fragmentation_point + largest_height))
    cur.execute(
        'CREATE TABLE pointsf2 AS select * from points where latitude >= ' + str(lowest_latitude + fragmentation_point - largest_height)
        + ' and ' + ' latitude <= ' + str(lowest_latitude + (2*fragmentation_point) + largest_height))
    cur.execute(
        'CREATE TABLE pointsf3 AS select * from points where latitude >= ' + str(lowest_latitude + (2*fragmentation_point) - largest_height)
        + ' and ' + ' latitude <= ' + str(lowest_latitude + (3*fragmentation_point) + largest_height))
    cur.execute(
        'CREATE TABLE pointsf4 AS select * from points where latitude >= ' + str(lowest_latitude + (3*fragmentation_point) - largest_height))



def dropTable (table_name, cur):
    cur.execute('DROP TABLE IF EXISTS ' + table_name)


def parallelJoin (pointsTable, rectsTable, outputTable, outputPath, openConnection):
    cur = openConnection.cursor()
    dropTable('output',cur)
    cur.execute("CREATE TABLE output (_count bigint, rectangle geometry)")
    thread1 = Thread(target = joinFragments, args=('pointsf1','rectsf1',cur))
    thread2 = Thread(target = joinFragments, args=('pointsf2','rectsf2',cur))
    thread3 = Thread(target = joinFragments, args=('pointsf3','rectsf3',cur))
    thread4 = Thread(target = joinFragments, args=('pointsf4','rectsf4',cur))
    thread1.start()
    thread2.start()
    thread3.start()
    thread4.start()
    thread1.join()
    thread2.join()
    thread3.join()
    thread4.join()
    cur.execute("SELECT DISTINCT _count, rectangle from output order by _count asc")
    rows = cur.fetchall()
    f = open(outputPath, "a")
    for row in rows:
        f.write(str(row[0])+'\n')
    f.close()
    print('join done')
    openConnection.commit()


################### DO NOT CHANGE ANYTHING BELOW THIS #############################

def joinFragments(pointsTable, rectsTable, cur):
    cur.execute('INSERT INTO output (_count,rectangle) SELECT  count( ' + pointsTable + '.geom) AS count , ' + rectsTable
        +'.geom  as rectangle FROM ' + rectsTable
        + ' JOIN ' + pointsTable + ' ON st_contains('+ rectsTable +'.geom,' + pointsTable + '.geom) GROUP BY '
        +rectsTable +'.geom order by count asc')


# Donot change this function
def getOpenConnection(user='postgres', password='admin', dbname='dds_assignment2'):
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


