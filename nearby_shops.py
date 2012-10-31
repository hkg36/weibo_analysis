#-*-coding:utf-8-*-
import pymongo
import sqlite3
import time
import re
#分析店铺周边的竞争对手
def analysis_shop(dianpin_shopid):
    con=pymongo.Connection('mongodb://xcj.server4')
    shop=con.dianpin.shop.find_one({'dianpin_id':dianpin_shopid},{'dianpin_id':1,'loc':1,'dianpin_tag':1,'shopname':1})
    if shop==None:
        print 'shop not found'
        return
    shop['name_short']=re.sub('(?i)\([^\)]*\)','',shop['shopname'])
    #找出周边所有竞争对手店铺
    loc=shop.get('loc')
    cur=con.dianpin.shop.find({"loc":{"$within":{"$center":[[loc['lat'],loc['lng']],0.02]}}},{'dianpin_id':1,'dianpin_tag':1,'loc':1,'shopname':1})
    competitors=[]
    for other in cur:
        if other['dianpin_id']==shop['dianpin_id']:
            continue
        tags=other['dianpin_tag']
        other['match']=len(set(tags).intersection(set(shop['dianpin_tag'])))
        competitors.append(other)
    competitors.sort(lambda a,b:-cmp(a['match'],b['match']))
    competitors_id=[s['dianpin_id'] for s in competitors[0:40]]
    con.dianpin.shop.update({'dianpin_id':shop['dianpin_id']},{"$set":{"competitor":{'list':competitors_id,'time':time.time()}}})
    print 'processed',shop['dianpin_id']
#分析附近的微薄确定用户是否到达，制作店铺用户记录以及用户活动记录
def analysis_point(center):
    print center
    fill_shop_ids={}
    con=pymongo.Connection('mongodb://xcj.server4')
    cur=con.dianpin.shop.find({"loc":{"$within":{"$center":[[center['lat'],center['lng']],0.02]}}},{'dianpin_id':1,'dianpin_tag':1,'loc':1,'shopname':1})
    for line in cur:
        line['name_short']=re.sub('(?i)\([^\)]*\)','',line['shopname'])
        fill_shop_ids[line['dianpin_id']]=line
    #找出周边所有地理位置微薄
    radius=0.03
    area=[[center['lat']-radius,center['lng']-radius],[center['lat']+radius,center['lng']+radius]]
    cur=con.weibolist.weibo.find({'pos':{'$within':{'$box':area}}})
    all_weibo={}
    for line in cur:
        all_weibo[line['weibo_id']]=line
    print 'read %d weibo'%len(all_weibo)
    #生成用户到店记录和用户行动历史
    weibo_user_go_shop={}
    for shop_id in fill_shop_ids:
        oneshop=fill_shop_ids[shop_id]
        short_name=oneshop['name_short']
        shop_weibo=[]
        to_remove_weibos=[]
        for weibo_id in all_weibo:
            one_weibo=all_weibo[weibo_id]
            if short_name in one_weibo['word']:
                shop_weibo.append(one_weibo)
                to_remove_weibos.append(one_weibo)

                history=weibo_user_go_shop.get(one_weibo['uid'],{})
                record=history.get(shop_id,set())
                record.add(one_weibo['weibo_id'])
                history[shop_id]=record
                weibo_user_go_shop[one_weibo['uid']]=history

        to_remove_weibos.sort(lambda a,b:-cmp(a['weibo_id'],b['weibo_id']))
        latest_weibo_user_id=[]
        for rm_weibo in to_remove_weibos:
            if len(latest_weibo_user_id)>=20:
                break
            if rm_weibo['uid'] not in latest_weibo_user_id:
                latest_weibo_user_id.append(rm_weibo['uid'])

        for rm_weibo in to_remove_weibos:
            del all_weibo[rm_weibo['weibo_id']]

        weibo_user_ids={}
        for w in shop_weibo:
            s_time=weibo_user_ids.get(w['uid'],0)
            weibo_user_ids[w['uid']]=s_time+1
        weibo_user_ids_list=[]
        for uid in weibo_user_ids:
            weibo_user_ids_list.append((uid,weibo_user_ids[uid]))
        weibo_user_ids_list.sort(lambda a,b:-cmp(a[1],b[1]))
        con.dianpin.shop.update({'dianpin_id':shop_id},{'$set':{'weibo_users':weibo_user_ids_list,'latest_weibo_user':latest_weibo_user_id}})

    #记录用户行动历史，和旧数据合并
    for uid in weibo_user_go_shop:
        new_log=weibo_user_go_shop[uid]
        data=con.dianpin.user_log.find_one({'weibo_uid':uid})
        if data==None:
            data={'weibo_uid':uid}
            old_log=new_log
        else:
            old_log={}
            shop_list=data.get('shop_log')
            if shop_list!=None:
                for shop in shop_list:
                    old_log[shop['shop']]=set(shop['weibos'])
            for shop_id in new_log:
                old_record=old_log.get(shop_id,set())
                old_log[shop_id]=old_record.union(new_log[shop_id])

        shop_list=[]
        for shop_id in old_log:
            shop_list.append({'shop':shop_id,'weibos':list(old_log[shop_id])})
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
    cc=con.cursor()
    cc.execute('select lat,lng from geoweibopoint')
    all_point=[]
    for lat,lng in cc:
        all_point.append({'lat':lat,'lng':lng})
    cc.close()

    for pt in all_point:
        analysis_point(pt)