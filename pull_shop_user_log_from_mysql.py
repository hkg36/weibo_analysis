#-*-coding:utf-8-*-
import pymongo
import sqlite3
import time
import re
import mongo_autoreconnect
import MySQLdb
def MongoDBConnect():
    #return pymongo.Connection('mongodb://xcj.server4/')
    return pymongo.Connection('mongodb://xcj.server2/')
def MySQLConnect():
    #return MySQLdb.connect(host="localhost",user="root",passwd="123456",db='data_mining_xcj')
    return MySQLdb.connect(host="192.168.1.111",user="root",passwd="mysql@xcj",db='data_mining_xcj')
def Pull_shop_user_from_mysql():
    sqlcon=MySQLConnect()
    sqlc=sqlcon.cursor(MySQLdb.cursors.DictCursor)
    sqlc.execute('select shop_id,weibo_uid,counts from shop_user order by shop_id,counts desc')
    con=MongoDBConnect()

    now_shop_id=0
    weibo_user_ids_list=None
    while True:
        line=sqlc.fetchone()
        if line==None:
            break
        if now_shop_id!=line['shop_id']:
            if now_shop_id!=0:
                con.dianpin.shop.update({'dianpin_id':now_shop_id},{'$set':{'weibo_users':weibo_user_ids_list}})
            now_shop_id=line['shop_id']
            weibo_user_ids_list=[]
        weibo_user_ids_list.append([line['weibo_uid'],line['counts']])
    sqlc.close()
    sqlcon.close()
    con.close()
    return
def Pull_shop_latest_user_from_mysql():
    sqlcon=MySQLConnect()
    sqlc=sqlcon.cursor(MySQLdb.cursors.DictCursor)
    sqlc.execute('select shop_id,weibo_uid,max(weibo_id) as max_weibo_id from shop_user_log group by shop_id,weibo_uid order by shop_id,max_weibo_id desc')
    con=MongoDBConnect()
    now_shop_id=0
    weibo_user_ids_list=None
    while True:
        line=sqlc.fetchone()
        if line==None:
            break
        if now_shop_id!=line['shop_id']:
            if now_shop_id!=0:
                con.dianpin.shop.update({'dianpin_id':now_shop_id},{'$set':{'latest_weibo_user':weibo_user_ids_list}})
            now_shop_id=line['shop_id']
            weibo_user_ids_list=[]
        weibo_user_ids_list.append(line['weibo_uid'])
    sqlc.close()
    sqlcon.close()
    con.close()
def Pull_user_log_from_mysql():
    #select weibo_uid,shop_id,weibo_id from shop_user_log order by weibo_uid,shop_id

    sqlcon=MySQLConnect()
    sqlc=sqlcon.cursor(MySQLdb.cursors.DictCursor)
    sqlc.execute('select weibo_uid,shop_id,weibo_id from shop_user_log order by weibo_uid')
    con=MongoDBConnect()

    now_weibo_uid=0
    now_shop_id=0
    now_user_log=None
    while True:
        line=sqlc.fetchone()
        if line==None:
            break
        if now_weibo_uid!=line['weibo_uid']:
            if now_weibo_uid!=0:
                shop_list=[]
                for shop_id in now_user_log:
                    shop_list.append({'shop':shop_id,'weibos':list(now_user_log[shop_id])})
                con.dianpin.user_log.update({'weibo_uid':now_weibo_uid},{'$set':{'shop_log':shop_list,'shop_log_update_time':time.time()}},upsert=True)
            now_weibo_uid=line['weibo_uid']
            now_user_log={}
        shop_log=now_user_log.get(line['shop_id'])
        if None==shop_log:
            shop_log=set()
            now_user_log[line['shop_id']]=shop_log
        shop_log.add(line['weibo_id'])
    sqlc.close()
    sqlcon.close()
    con.close()
def Pull_user_ave_cost_from_mysql():
    sqlcon=MySQLConnect()
    sqlc=sqlcon.cursor(MySQLdb.cursors.DictCursor)
    sqlc.execute('select weibo_uid,ave_cost from user_profile')
    con=MongoDBConnect()

    while True:
        line=sqlc.fetchone()
        if line==None:
            break
        con.dianpin.user_log.update({'weibo_uid':line['weibo_uid']},{'$set':{'ave_cost':line['ave_cost']}})
    sqlc.close()
    sqlcon.close()
    con.close()
if __name__ == '__main__':
    Pull_shop_user_from_mysql()
    Pull_shop_latest_user_from_mysql()
    Pull_user_log_from_mysql()
    Pull_user_ave_cost_from_mysql()