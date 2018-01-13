import pysher
import sys
import time
import json
import time

#!/usr/bin/python
import pymysql.cursors
import pymysql


# Connect to the database
connection = pymysql.connect(host="5.175.18.151",    # your host, usually localhost
                            user="crypto",         # your username
                            passwd="!!4231crypto1324!!",  # your password
                            db="trad",
                            charset='utf8mb4',
                            cursorclass=pymysql.cursors.DictCursor)


# Add a logging handler so we can see the raw communication data
#import logging
#root = logging.getLogger()
#root.setLevel(logging.INFO)
#ch = logging.StreamHandler(sys.stdout)
#root.addHandler(ch)


def print_usage(filename):
    print("Usage: python %s <appkey>" % filename)

def channel_callback(data):
    array = json.loads(data)
    ts = time.gmtime()
    tx=time.strftime("%s", ts)
    # Unix timestamp
    #print 'ci sono '+tx
    with connection.cursor() as cursor:
        # Create a new record
        array2 = []
        for f in range(1):
            array2.append((array['bids'][f][0], array['bids'][f][1], '0', tx))
            array2.append((array['asks'][f][0], array['asks'][f][1], '1', tx))
        cursor.executemany("INSERT INTO `offer` (`prezzo`, `quanto`, `tipo`, `ordine`) VALUES (%s, %s, %s, %s)", array2)
        connection.commit()
        cursor.execute("INSERT INTO offer_temp SELECT * FROM offer ORDER BY id DESC LIMIT 200;")
        cursor.execute("DELETE FROM offer;")
        cursor.execute("INSERT INTO offer SELECT * FROM offer_temp;")
        # cursor.execute("DROP TABLE offer_temp;")
        cursor.execute("DELETE FROM offer_temp;")
        connection.commit()

    with connection.cursor() as cursor:
        # Read a single record
        sql = "SELECT `prezzo`,`data` FROM `buy` "
        cursor.execute(sql)
        buy = cursor.fetchone()
        sell=True
        sceso=0
        tempdiff = 0
        if buy is None:
            sell=False
            sql = "SELECT `quanto`, `prezzo`, `ordine` FROM `offer` WHERE `tipo`=%s ORDER BY id DESC LIMIT 180"
            cursor.execute(sql, ('1',))
            result = cursor.fetchall()
            controlloA(tx,result,sell)
        else:
            sql = "SELECT `quanto`, `prezzo`, `ordine` FROM `offer` WHERE `tipo`=%s ORDER BY id DESC LIMIT 1"
            cursor.execute(sql, ('0',))
            result = cursor.fetchall()
            controlloV(tx, result, sell,buy)

def controlloV(tx,result,sell,buy):
    with connection.cursor() as cursor:
        # Read a single record
        tempdiff = 0
        a = buy['prezzo']
        tempo = buy['data']
        tempdiff = int(tx) - tempo
        for i in range(len(result)):
            b=result[i]['prezzo']
            ts=result[i]['ordine']
            readable = time.ctime(ts)
            diff=int(tx)-ts
            c=(b-a)/b*100
            #print('da ' + str(a) + ' sceso ' + str(b) + '  quanto in %  ' + str(c) + ' in data ' + readable)
            if a>b and c<-0.1 and diff < 120 and sell==False:
                    if diff<1:
                        print('da ' + str(a) + ' sceso ' + str(b) + '  quanto in %  ' + str(c) + ' in data ' + readable)


            elif a<b and c>1 and sell and diff<15:
                print('da ' + str(a) + ' salito ' + str(b) + '  quanto in %  ' + str(c) + ' in data ' + readable)
                print 'vendo'
                cursor.execute("INSERT INTO storia (prezzo,profitto,venduto,data) VALUES (%s,%s,%s,%s)",
                               (a, c, b, tx))
                connection.commit()
                cursor.execute("DELETE FROM buy ")
                connection.commit()
                break
            elif a<b and c>1.2 and diff<30 and sell:
                print('da ' + str(a) + ' salito ' + str(b) + '  quanto in %  ' + str(c) + ' in data ' + readable)
                print 'vendo'
                cursor.execute("INSERT INTO storia (prezzo,profitto,venduto,data) VALUES (%s,%s,%s,%s)",
                               (a, c, b, tx))
                connection.commit()
                cursor.execute("DELETE FROM buy ")
                connection.commit()
                break
            elif a<b and c>1.3 and diff<60 and sell:
                print('da ' + str(a) + ' salito ' + str(b) + '  quanto in %  ' + str(c) + ' in data ' + readable)
                print 'vendo'
                cursor.execute("INSERT INTO storia (prezzo,profitto,venduto,data) VALUES (%s,%s,%s,%s)",
                               (a, c, b, tx))
                connection.commit()
                cursor.execute("DELETE FROM buy ")
                connection.commit()
                break
            elif a<b and c>1.4 and diff<180 and sell:
                print('da ' + str(a) + ' salito ' + str(b) + '  quanto in %  ' + str(c) + ' in data ' + readable)
                print 'vendo'
                cursor.execute("INSERT INTO storia (prezzo,profitto,venduto,data) VALUES (%s,%s,%s,%s)",
                               (a, c, b, tx))
                connection.commit()
                cursor.execute("DELETE FROM buy ")
                connection.commit()
                break
            elif ((c<-4 and diff < 2) or (tempdiff>1800)) and sell :
                if diff < 1:
                    print('da ' + str(a) + ' sceso ' + str(b) + '  quanto in %  ' + str(c) + ' in data ' + readable)
                print 'Minimizzo perdita'
                cursor.execute("INSERT INTO storia (prezzo,profitto,venduto,data) VALUES (%s,%s,%s,%s)",
                               (a, c, b, tx))
                connection.commit()
                cursor.execute("DELETE FROM buy ")
                connection.commit()
                break
            else:
                print('cerco di vender da ' + str(a) + ' e ora ' + str(b) + '  quanto in %  ' + str(c) + ' in data ' + readable)

def controlloA(tx,result,sell):
    with connection.cursor() as cursor:
        # Read a single record
        tempdiff = 0
        a = result[0]['prezzo']

        for i in range(len(result)):
            b=result[i]['prezzo']
            ts=result[i]['ordine']
            readable = time.ctime(ts)
            diff=int(tx)-ts
            c=(a-b)/b*100
            #print('da ' + str(a) + ' sceso ' + str(b) + '  quanto in %  ' + str(c) + ' in data ' + readable)
            if a<b and c<-1 and diff < 180 and sell==False:
                    if diff<1:
                        print('da ' + str(a) + ' sceso ' + str(b) + '  quanto in %  ' + str(c) + ' in data ' + readable)
                        print 'compro'
                        cursor.execute("INSERT INTO buy (quanto,prezzo,data) VALUES (%s,%s,%s)", ('100', a,tx))
                        connection.commit()
                        break
                    elif diff<30 and c<-1.1:
                        print('da ' + str(a) + ' sceso ' + str(b) + '  quanto in %  ' + str(c) + ' in data ' + readable)
                        print 'compro'
                        cursor.execute("INSERT INTO buy (quanto,prezzo,data) VALUES (%s,%s,%s)", ('100', a,tx))
                        connection.commit()
                        break
                    elif diff<60 and c<-1.3:
                        print('da ' + str(a) + ' sceso ' + str(b) + '  quanto in %  ' + str(c) + ' in data ' + readable)
                        print 'compro'
                        cursor.execute("INSERT INTO buy (quanto,prezzo,data) VALUES (%s,%s,%s)", ('100', a,tx))
                        connection.commit()
                        break
                    elif diff<90 and c<-1.5 and a>result[1]['prezzo']:
                        print('da ' + str(a) + ' sceso ' + str(b) + '  quanto in %  ' + str(c) + ' in data ' + readable)
                        print 'compro'
                        cursor.execute("INSERT INTO buy (quanto,prezzo,data) VALUES (%s,%s,%s)", ('100', a,tx))
                        connection.commit()
                        break
                    elif diff<180 and c<-3 and a>result[1]['prezzo']:
                        print('da ' + str(a) + ' sceso ' + str(b) + '  quanto in %  ' + str(c) + ' in data ' + readable)
                        print 'compro'
                        cursor.execute("INSERT INTO buy (quanto,prezzo,data) VALUES (%s,%s,%s)", ('100', a,tx))
                        connection.commit()
                        break


            elif a<b and c>0.3 and diff < 2 and sell:
                if diff < 1:
                    print('da ' + str(a) + ' salito ' + str(b) + '  quanto in %  ' + str(c) + ' in data ' + readable)
            else:
                if diff == 2:
                    print('da ' + str(a) + ' e ora ' + str(b) + '  quanto in %  ' + str(c) + ' in data ' + readable)


def connect_ghandler(data):
    channel = pusher.subscribe("order_book_xrpusd")
    channel.bind('data', channel_callback)



pusher = pysher.Pusher('de504dc5763aeef9ff52')
pusher.connection.bind('pusher:connection_established', connect_ghandler)
pusher.connect()

while True:
    time.sleep(600)