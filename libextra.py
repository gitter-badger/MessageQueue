#
# IMPORTS
#
import os
import pymysql

#
# EXIT APP CONTROL
#

cFILE_DELETE_TO_STOP = "delete_me_to_stop_driver.txt"

killit=open(cFILE_DELETE_TO_STOP,"w")
killit.close()

vClosingApp = False
def fnExitNow(checkfile = False):
    global vClosingApp

    if (vClosingApp == True):
        return(False)

    if checkfile:
        if os.path.exists(cFILE_DELETE_TO_STOP):
            return(True)

    if (checkfile == False) & (vClosingApp == False):
        return(True)

    #if checkfile: #print only once
        #print('Exiting... Waiting Threads to get finished.')

    vClosingApp = True
    return(False)

#
# Queue is empty?
#
"""
vQueueEmpty = 0 #1 = SIM, TA VAZIO | 0 = NAO


def fnQueueEmpty(empty=10, lock = None):
    global vQueueEmpty
    if (empty == 10):  # just checking
        return (vQueueEmpty == 1)
    lock.acquire()
    vQueueEmpty = empty
    lock.release()
    return (vQueueEmpty == 1)
"""

#
# ThreadControl
#

vQueueControl = 0


def fnQueueControl(lock,incc):
    global vQueueControl

    if (incc == 0):
        return(vQueueControl)

    lock.acquire()
    vQueueControl = vQueueControl + incc
    lock.release()



#
# MySQL (re)connect
#
def fnMySQLConnect():
    try:
        conn.close
    except:
        pass

    try:
        conn = pymysql.connect(host='localhost', user='root', passwd='password', port=3306, autocommit=True)
        cur = conn.cursor()
        return(cur)
    except Exception as e:
        pass
        #run.logging.error('MySQL Exception: ', exc_info=True)
        #print('MYSQL EXCEPTION:',e)

    return(False)


#
# String POS
#
def fnPOS(text,value):
    try:
        text.index(value)
        return(True)
    except:
        return(False)

#
# HTTP GET
#

class return_timeout(object):
    text = ''
    status_code = -100

    def __init__(self, text):
        self.text = text

def fnGET(s,url):
    try:
        return(s.get(url, headers={'User-Agent': ':)'}, timeout=60))
    except Exception as e:
        tmp = '@EXCEPTION@ '+str(e).upper()
        return(return_timeout(tmp))
