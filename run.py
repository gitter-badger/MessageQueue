#
# IMPORTS
#
#import tracemalloc
#tracemalloc.start()

import sys
from time import strftime
import urllib
import requests
import queue
import logging
import logging.handlers
import threading
import libextra
import pymysql
import time

from logging.handlers import TimedRotatingFileHandler

#
# LOG SETTINGS
#
logg = logging.getLogger('MyLogger')
logg.setLevel(logging.DEBUG)
handler = logging.handlers.RotatingFileHandler('run.log',
                                               maxBytes=(1024*1024*2), #2 mb
                                               backupCount=5,
                                               )
formatter = logging.Formatter("[%(asctime)s - %(filename)s:%(lineno)4s - %(levelname)10s - %(funcName)20s()]  %(message)s")
handler.setFormatter(formatter)
logg.addHandler(handler)

#
# GLOBAL VARIABLES
#
cSQL_LIMIT   = 20000
cTHREADS_CNT = 200

#
# FIRST STEPS
#
logg.info('************** STARTING DRIVER **************** (Threads: %s)(SQL Limit: %s)',cTHREADS_CNT,cSQL_LIMIT)

QueueMaster = queue.Queue()
QueueUpdate = queue.Queue()
lock = threading.Lock()

cur = libextra.fnMySQLConnect()
#
# STEP 1 - THREAD LOOP
#
def fnThreadLoop(i, queue, lock):

    s = requests.Session()

    while True:
        #exit Thread when detect signal to quit.
        while libextra.fnExitNow():
            try:
                r = queue.get_nowait()
                break
            except:
                #libextra.fnQueueEmpty(1,lock)
                time.sleep(0.1)
                continue

        if libextra.fnExitNow() == False:
            break

        id = r[0]
        (status, gw_resp, log_msg) = fnJOB_CallAPI(s,r)
        gw_resp = pymysql.escape_string(gw_resp)

        #if (log_msg != ''):
        #    print(log_msg)

        QueueUpdate.put((id,status,gw_resp))
        queue.task_done()


#
# STEP 2 - START THREADS
#
for i in range(cTHREADS_CNT):
    worker = threading.Thread(target=fnThreadLoop, args=(i, QueueMaster,lock,))
    worker.start()

QueueMaster.join()
QueueUpdate.join()

#
# STEP 3 - Do HTTP GETs
#
def fnJOB_CallAPI(s,r):
    id      = r[0]
    bnum    = r[1]
    anum    = r[2]
    carrier = r[3]
    text    = r[4]

    url = 'http://www.endpoint.com/api.php?msisdn='+str(anum)+'&la='+str(bnum)+'&mensagem='+urllib.parse.quote_plus(text)+'&codigo=GW'+str(id)

    r = libextra.fnGET(s,url)

    if (len(r.text.strip()) == 20) and (libextra.fnPOS(r.text.strip(),' ') == False):
        return((2,r.text,''))

    if (    (r.status_code != 200) or
            (r.text.strip() == '505') or
            (libextra.fnPOS(r.text.upper(),'mysql_pconnect():'.upper())) or
            (libextra.fnPOS(r.text.upper(),'<b>Warning</b>'.upper()))
        ):
        log_msg = 'Loop4Ever -> url='+url+' - body='+r.text
        #logg.warning(log_msg) #gera erro pacas!
        return((-10,r.text,log_msg))

    log_msg = 'Error on Sending -> url='+url+' - body='+r.text
    logg.error(log_msg)
    return((10,r.text,log_msg))


#
# STEP 4 - UPDATE MYSQL ROWS
#
def fnUpdateRows(cur):
    global lock

    vSTATUS  = ''
    vGW_RESP = ''
    vIDs     = ''

    for i in range(50): #atualiza 50 linhas no banco por vez
        try:
            r = QueueUpdate.get_nowait()


            id      = r[0]
            status  = r[1]
            gw_resp = r[2]

            if (status != -10):
                vSTATUS  = vSTATUS  + "when id = "+str(id)+" then "+str(status)+" "
                vGW_RESP = vGW_RESP + "when id = "+str(id)+" then '"+gw_resp+"' "
                vIDs     = vIDs     + str(id)+","

            QueueUpdate.task_done()

            libextra.fnQueueControl(lock, -1)
        except Exception as e:
            break

    if vIDs == '':
        return(False)

    sql =   " UPDATE MessageQueue" \
            " SET dup = now(), status = case " \
            " "+vSTATUS+" " \
            "     end, " \
            "     gw_resp = case " \
            " "+vGW_RESP+" " \
            "     end " \
            " WHERE status in (0,15) and id in ("+vIDs+"0)"

    while True:
        try:
            #print(sql)
            cur.execute(sql)

            return(True)

        except Exception as e:
            logg.error('Loop4Ever Exception (MySQL conn problem) -> ', exc_info=True)

            cur = libextra.fnMySQLConnect()
            time.sleep(5)



#
# STEP 5 - GET DATA FROM MYSQL
#
while True:
    while (libextra.fnQueueControl(lock,0) > 0) & (libextra.fnExitNow(True)):
        while fnUpdateRows(cur):
            pass
        time.sleep(0.1)

    while fnUpdateRows(cur):
            pass

    if libextra.fnExitNow() == False:
        break


    sql = " SELECT id, bnum, anum, carrier, text" \
          " FROM MessageQueue " \
          " WHERE status = 0" \
          " and ((now() >= din_sched) or (din_sched is null))" \
          " and id_canal = 0 and bnum <> '40810' and carrier = 5 " \
          " LIMIT "+str(cSQL_LIMIT)

    print('.') #So pra mostrar que nao ta morto...

    #snapshot = tracemalloc.take_snapshot()
    #top_stats = snapshot.statistics('lineno')

    #print("[ Top 20 ]")
    #for stat in top_stats[:20]:
    #    print(stat)

    try:
        cur.execute(sql)

        for r in cur:
            QueueMaster.put(r)
            libextra.fnQueueControl(lock, 1)
            #libextra.fnQueueEmpty(0,lock)

        if (cur.rowcount < cSQL_LIMIT):
            time.sleep(5)

    except Exception as e:
        logg.error('Loop4Ever Exception (probably MySQL conn problem) -> ', exc_info=True)

        cur = libextra.fnMySQLConnect()
        time.sleep(5)
#
# Exiting...
#
logg.info('Exiting... Waiting Threads to get finished...')

while threading.active_count() > 1:
    logg.info('Threads still running: '+str(threading.active_count()-1))
    time.sleep(0.5)

while fnUpdateRows(cur):
        pass

cur.close()
logg.info('Done, see you!')
