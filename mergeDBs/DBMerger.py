import mysql.connector
from mysql.connector import errorcode 
import logging, json

logging.basicConfig(
    filename="mergeDBs.log",
    level=logging.DEBUG,
    format="%(asctime)s:%(levelname)s:%(message)s"
)

def getCursor (db) :
    try :
        cursor = db.cursor()
    except e:
        raise ValueError('Get Cursor Error.')
        return
    else :
        return cursor


def allTables (db) :
    cursor = getCursor(db)

    cursor.execute('show tables;')
    
    return [table[0] for table in cursor.fetchall()]

def fetchData (db, tableName) :
    cursor = getCursor(db)

    cursor.execute('select * from {};'.format(tableName))
    return cursor.fetchall()

def closeAllDB (dbDict) :
    for _, db in DBDict.items() :
        db.close()

def fetchAllData (db) :
    tables = allTables(db)
    return [[table, fetchData(db, table)] for table in tables]

def mergeDBs (db1, db2) :
    mergedDBData = fetchAllData(db2)

    for [table, value] in mergedDBData :

        if (not len(value)) :
            continue

        for data in value :
            insert(db1, table, data)

        # break

def insert (db, tableName, data) :
    cursor = getCursor(db)
    columns = showColumns(db, tableName)
    # print(','.join(columns))
    insertSQL = ' '.join((
        "INSERT INTO {}".format(tableName),
        '(' + ', '.join(columns) + ')', 
        'VALUES (' + ', '.join(['%s' for index in columns]) + ')'
    ))

    logging.debug('[SQL]:{}'.format(insertSQL))
    logging.debug('[Data]:{}'.format(data))

    try:
        cursor.execute(insertSQL, data)
        db.commit()
    except mysql.connector.Error as err:
        if (err.errno == errorcode.ER_DUP_ENTRY) :
            logging.debug('[Conflict]:[Table: {}]:[Data: {}]'.format(tableName, data))
        else :
            logging.debug(err)


def showColumns (db, tableName) :
    cursor = getCursor(db)
    cursor.execute('show columns from {}'.format(tableName))
    # logging.debug(cursor.fetchall())
    # return [column[0] for column in cursor.fetchall() if column[5] != 'auto_increment']
    return [column[0] for column in cursor.fetchall()]


if __name__ == '__main__' :
    
    mainDBOrder = 0
    mergedDBOrder = 1
    DBDict = {}

    with open('config.json') as config :    
        DBConfig = json.load(config)
        print(DBConfig)

    # exit()
    # print([k for _,k in range(0, 10).items()])

    try :
        DBDict['mainDB'] = mysql.connector.connect(**DBConfig[mainDBOrder])
        DBDict['mergedDB'] = mysql.connector.connect(**DBConfig[mergedDBOrder])
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logging.debug("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            logging.debug("Database does not exist")
        else:
            logging.debug(err)
    else:
        # print(allTables(DBDict['mainDB']))
        # print(fetchTable(DBDict['mainDB'], 'world_boss'))
        mergeDBs(DBDict['mainDB'], DBDict['mergedDB'])
        # showColumns(DBDict['mergedDB'], 'team_battle_cache')
        # insert(DBDict['mergedDB'], 'team_battle_cache', '')
        closeAllDB(DBDict)