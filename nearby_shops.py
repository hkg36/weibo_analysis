#-*-coding:utf-8-*-
import pymongo
import sqlite3
import time
import re
import mongo_autoreconnect
import MySQLdb

def MySQLConnect():
    return MySQLdb.connect(host="192.168.1.111",user="root",passwd="znb@xcj",db='data_mining_xcj')
#分析附近的微薄确定用户是否到达，制作店铺用户记录以及用户活动记录
def analysis_point(center):
    print center
    fill_shop_ids=[]
    con=pymongo.Connection('mongodb://xcj.server4/',read_preference=pymongo.ReadPreference.SECONDARY)
    cur=con.dianpin.shop.find({"loc":{"$within":{"$center":[[center['lat'],center['lng']],0.01]}}},
        {'dianpin_id':1,'dianpin_tag':1,'loc':1,'shopname':1,'atmosphere':1,'recommend':1,'alias':1})
    for line in cur:
        line['shopname']=unicode(line['shopname'])
        name_short=set((re.sub('(?i)\([^\)]*\)','',line['shopname']),))
        if line.has_key('alias'):
            name_short.add(line['alias'])
        line['name_short']=name_short
        alltag=set()
        if line.has_key('dianpin_tag'):
            alltag.update(line['dianpin_tag'])
        if line.has_key('atmosphere'):
            alltag.update(line['atmosphere'])
        if line.has_key('recommend'):
            alltag.update(line['recommend'])
        line['alltag']=alltag
        fill_shop_ids.append(line)

    print 'read %d shop'%len(fill_shop_ids)
    #找出周边所有地理位置微薄
    all_weibo={}
    HALF_PICE_COUNT=4
    RADIUS=0.012
    for x_i in range(-HALF_PICE_COUNT,HALF_PICE_COUNT):
        for y_i in range(-HALF_PICE_COUNT,HALF_PICE_COUNT):
            radius=RADIUS/HALF_PICE_COUNT
            area=[[center['lat']+radius*x_i,center['lng']+radius*y_i],[center['lat']+radius*(x_i+1),center['lng']+radius*(y_i+1)]]
            cur=con.weibolist.weibo.find({'pos':{'$within':{'$box':area}}})
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

    sqldb=MySQLConnect()
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
    return

    weibo_user_go_shop={}
    for shop_id in shop_weibo_log:
        shop_weibo=shop_weibo_log[shop_id]
        for one_weibo in shop_weibo:
            history=weibo_user_go_shop.get(one_weibo['uid'])
            if history==None:
                history={}
                weibo_user_go_shop[one_weibo['uid']]=history
            record=history.get(shop_id)
            if record==None:
                record=set()
                history[shop_id]=record
            record.add(one_weibo['weibo_id'])

    for shop_id in shop_weibo_log:
        shop_weibo=shop_weibo_log.get(shop_id)
        shop_weibo.sort(lambda a,b:-cmp(a['weibo_id'],b['weibo_id']))
        #最近到店
        latest_weibo_user_id=[]
        for rm_weibo in shop_weibo:
            if len(latest_weibo_user_id)>=20:
                break
            if rm_weibo['uid'] not in latest_weibo_user_id:
                latest_weibo_user_id.append(rm_weibo['uid'])

        weibo_user_ids={}
        for w in shop_weibo:
            s_time=weibo_user_ids.get(w['uid'],0)
            weibo_user_ids[w['uid']]=s_time+1
        weibo_user_ids_list=weibo_user_ids.items()
        weibo_user_ids_list.sort(lambda a,b:-cmp(a[1],b[1]))
        con.dianpin.shop.update({'dianpin_id':shop_id},{'$set':{'weibo_users':weibo_user_ids_list,'latest_weibo_user':latest_weibo_user_id}})

    #记录用户行动历史，和旧数据合并
    for uid in weibo_user_go_shop:
        new_log=weibo_user_go_shop[uid]
        data=con.dianpin.user_log.find_one({'weibo_uid':uid},{'shop_log':1,'weibo_uid':1})
        if data==None or 'shop_log' not in data:
            data={}
            old_log=new_log
        else:
            old_log={}
            shop_list=data.get('shop_log')
            if shop_list:
                for shop in shop_list:
                    old_log[shop['shop']]=set(shop['weibos'])
            for shop_id in new_log:
                old_record=old_log.get(shop_id)
                if old_record==None:
                    old_log[shop_id]=new_log[shop_id]
                else:
                    old_record.update(new_log[shop_id])

        shop_list=[]
        for shop_id in old_log:
            shop_list.append({'shop':shop_id,'weibos':list(old_log[shop_id])})
        data['weibo_uid']=uid
        data['shop_log']=shop_list
        data['shop_log_update_time']=time.time()
        con.dianpin.user_log.update({'weibo_uid':uid},data,upsert=True)

if __name__ == '__main__':
#港丽餐厅(大悦城店) 2384860
#海底捞（西单店）2114887
#麻辣诱惑(三里屯Village西南) 2814994
# 6113943 4683333 6209778
    """shop_ids=[2384860,2114887,2814994,6113943,4683333,6209778]
    for sid in shop_ids:
        analysis_shop(sid)"""
    """f=open('shop_id2.txt')
    lines=f.readlines()
    f.close()
    for id in lines:
        analysis_shop(int(id))"""
    con=sqlite3.connect('../fetchDianPin/GeoPointList.db')
    try:
        con.execute('alter table geoweibopoint add column analysis_checked int default 0')
    except Exception,e:
        print e
    cc=con.cursor()
    cc.execute('select id,lat,lng from geoweibopoint where analysis_checked=0')
    all_point=[]
    for id,lat,lng in cc:
        all_point.append({'id':id,'lat':lat,'lng':lng})

    for pt in all_point:
        analysis_point(pt)
        con.execute('update geoweibopoint set analysis_checked=1 where id=?',(pt['id'],))
        con.commit()

    sqlcon=MySQLConnect()
    sqlc=sqlcon.cursor()
    sqlc.execute('Truncate shop_user')
    sqlc.execute('insert into shop_user(shop_id,weibo_uid,counts) select shop_id,weibo_uid,count(*) from shop_user_log group by shop_id,weibo_uid')
    sqlcon.commit()
    sqlc.close()
    sqlcon.close()