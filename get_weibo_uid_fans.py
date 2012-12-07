#-*-coding:utf-8-*-
import pymongo
import sqlite3
import time
import re
import mongo_autoreconnect
import MySQLdb
import env_data
import multiprocessing

if __name__ == '__main__':
    taget_uid=[1189615035]

    con=pymongo.Connection('mongodb://xcj.server4/',read_preference=pymongo.ReadPreference.SECONDARY_ONLY)
    con.fsync(lock=True)

    users=[]
    try:
        cur=con.weibolist.user.find({'friend_list':{'$exists':1}},{'friend_list':1,'id':1,'_id':0})
        for line in cur:
            friend_list=line.get('friend_list')
            insect=[one for one in friend_list if one in taget_uid]
            if len(insect)>0:
                users.append(line['id'])
    except Exception,e:
        print(e)
    finally:
        con.unlock()
        con.close()
    print len(users)

    sqlcon=MySQLdb.connect(host=env_data.mysql_host,user=env_data.mysql_user,passwd=env_data.mysql_psw,db='cx_mid_database')
    sqlcur=sqlcon.cursor()
    for id in users:
        sqlcur.execute('insert ignore into user_follow_temp(weibo_uid) values(%s)',(id,))
    sqlcon.commit()
    sqlcur.close()
    sqlcon.close()