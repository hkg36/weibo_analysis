#-*-coding:utf-8-*-
import pymongo
import sqlite3
import time
import re
import mongo_autoreconnect
import MySQLdb
import env_data
import multiprocessing
import gzip
import json
from optparse import OptionParser

#分析附近的微薄确定用户是否到达，制作店铺用户记录以及用户活动记录
def analysis_point(center,fill_shop_ids,mongodb_url):
    print center

    con=pymongo.Connection(mongodb_url)

    print 'read %d shop'%len(fill_shop_ids)
    #找出周边所有地理位置微薄
    all_weibo={}
    HALF_PICE_COUNT=4
    RADIUS=0.012
    radius=RADIUS/HALF_PICE_COUNT
    for x_i in range(-HALF_PICE_COUNT,HALF_PICE_COUNT):
        for y_i in range(-HALF_PICE_COUNT,HALF_PICE_COUNT):
            area=[[center['lat']+radius*x_i,center['lng']+radius*y_i],[center['lat']+radius*(x_i+1),center['lng']+radius*(y_i+1)]]
            with con.weibolist.weibo.find({'pos':{'$within':{'$box':area}}}) as cur:
                for line in cur:
                    line['clean_word']=re.sub(u"(?i)\w{0,4}://[\w\d./]*","",line['word'])
                    all_weibo[line['weibo_id']]=line
            print 'read pice %d,%d to %d weibo'%(x_i,y_i,len(all_weibo))

    print 'read %d weibo'%len(all_weibo)
    #生成用户到店记录和用户行动历史

    shop_weibo_log={}
    for weibo_id in all_weibo:
        one_weibo=all_weibo[weibo_id]
        match_shop_id=0
        for oneshop in fill_shop_ids:
            if oneshop['shopname'] in one_weibo['clean_word']:
                match_shop_id=oneshop['dianpin_id']
                break
        if match_shop_id==0:
            for oneshop in fill_shop_ids:
                for s_name in oneshop['name_short']:
                    if s_name in one_weibo['clean_word']:
                        match_shop_id=oneshop['dianpin_id']
                        break
                if match_shop_id:
                    break
        if match_shop_id:
            shop_weibo=shop_weibo_log.get(match_shop_id)
            if shop_weibo==None:
                shop_weibo=[]
                shop_weibo_log[match_shop_id]=shop_weibo
            shop_weibo.append(one_weibo)

    sqldb=MySQLdb.connect(host=env_data.mysql_host,user=env_data.mysql_user,passwd=env_data.mysql_psw,db='data_mining_xcj')
    sqlc=sqldb.cursor()
    for shop_id in shop_weibo_log:
        shop_weibo=shop_weibo_log[shop_id]
        for one_weibo in shop_weibo:
            sqlc.execute('insert ignore shop_user_log(shop_id,weibo_uid,weibo_id,time) values(%s,%s,%s,%s)',
                (shop_id,
                one_weibo['uid'],
                one_weibo['weibo_id'],
                one_weibo['time']))

    sqldb.commit()
    sqlc.close()
    sqldb.close()

    return 0

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-s", "--server", dest="server",
                      help="set mongo db ip (like 127.0.0.1:27010)", default='218.241.207.45:27011')
    parser.add_option("-a", "--area",dest="area", default=None,
                      help="set area id,must be number")

    (options, args) = parser.parse_args()
    mongodb_server='mongodb://%s/'%(options.server)
    searcharea=options.area
    if searcharea is not None:
        searcharea=int(searcharea)

    f=gzip.open('data/allpointshop.gz','r')
    shoppoints=json.load(f)
    f.close()

    pool = multiprocessing.Pool()
    for pt in shoppoints:
        if searcharea is not None:
            if pt['center']['id']!=searcharea:
                print pt['center']['id'],'pass'
                continue
        pool.apply_async(analysis_point, (pt['center'],pt['shops'],mongodb_server))
    pool.close()
    pool.join()
    print "Sub-process(es) done."