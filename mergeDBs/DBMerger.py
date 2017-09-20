import mysql.connector
from mysql.connector import errorcode 
import logging, json

logging.basicConfig(
    filename="mergeDBs.log",
    level=logging.DEBUG,
    format="%(asctime)s:%(levelname)s:%(message)s"
)

playerConflictList = []
allianceConflictList = []
mainDBOrder = 0
mergedDBOrder = 1
DBDict = {}
mainDBPlayerList = []
mainDBAllianceList = []

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
    mainDBPlayerList = [player[0] for player in fetchData(db1, 'player')]
    mainDBAllianceList = [alliance[0] for alliance in fetchData(db1, 'alliance')]
    # print(mainDBPlayerList)
    # exit()

    for [table, value] in mergedDBData :

        if (not len(value)) :
            continue

        for data in value :
            insert(db1, table, data)

        # break
    logging.debug('[Conflict]:[Player]:{}'.format(playerConflictList))
    logging.debug('[Conflict]:[Alliance]:{}'.format(allianceConflictList))

def insert (db, tableName, data) :
    cursor = getCursor(db)
    columns = showColumns(db, tableName)
    # print(','.join(columns))

    if (
        idConflict('player', columns, data) or 
        idConflict('alliance', columns, data)
    ) :
        # logging.debug('[Player or Alliance Conflict]')
        return
    

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
            if (tableName == 'player' and (not data[0] in playerConflictList)) :
                playerConflictList.append(data[0])
            if (tableName == 'alliance' and (not data[0] in allianceConflictList)) :
                allianceConflictList.append(data[0])
        else :
            logging.debug(err)


def showColumns (db, tableName) :
    cursor = getCursor(db)
    cursor.execute('show columns from {}'.format(tableName))
    # logging.debug(cursor.fetchall())
    # return [column[0] for column in cursor.fetchall() if column[5] != 'auto_increment']
    return [column[0] for column in cursor.fetchall()]

def hasUser (playerId) :
    return playerId in mainDBPlayerList
    # cursor = getCursor(DBDict['mainDB'])
    # cursor.execute('select name from player where playerId = {}'.format(playerId))
    # return len(cursor.fetchall())

def hasAlliance (allianceId) :
    return allianceId in mainDBAllianceList
    # cursor = getCursor(DBDict['mainDB'])
    # cursor.execute('select name from alliance where allianceId = {}'.format(allianceId))
    # return len(cursor.fetchall())

def idConflict (type, columns, data) :
    
    idIndex = 0
    uid = 0

    try :
        idIndex = columns.index(type + 'Id')
    except :
        pass

    if (idIndex) :

        uid = data[idIndex]
        conflict = -1

        try :
            if (type == 'player') :
                conflict = playerConflictList.index(uid)
            if (type == 'alliance') :
                conflict = allianceConflictList.index(uid)
        except ValueError :
            pass

        # print(uid)
        # print(conflict)
        # print(playerConflictList)

        if (conflict != -1 or hasUser(uid) or hasAlliance(uid)) :

            if (conflict == -1) :
                if (type == 'player') :
                    logging.debug('[Player Conflict]:{}'.format(uid))
                    playerConflictList.append(uid)
                if (type == 'alliance') :
                    logging.debug('[Alliance Conflict]:{}'.format(uid))
                    allianceConflictList.append(uid)
            
            return True

if __name__ == '__main__' :

    with open('config.json') as config :    
        DBConfig = json.load(config)
        # print(DBConfig)

    # exit()
    # print([k for _,k in range(0, 10).items()])

    try :
        DBDict['mainDB'] = mysql.connector.connect(**DBConfig[mainDBOrder])
        DBDict['mergedDB'] = mysql.connector.connect(**DBConfig[mergedDBOrder])
        mergeDBs(DBDict['mainDB'], DBDict['mergedDB'])
        closeAllDB(DBDict)
        print('Successful!')
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logging.debug("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            logging.debug("Database does not exist")
        else:
            logging.debug(err)