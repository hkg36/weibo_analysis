#-*-coding:utf-8-*-
import pymongo
import pymongo.errors
import time
import re
#分析店铺周边的竞争对手，分析附近的微薄确定用户是否到达，制作店铺用户记录以及用户活动记录
if __name__ == '__main__':
    con=pymongo.Connection('mongodb://xcj.server4,xcj.server2/?slaveOk=true')
    cur=con.dianpin.shop.find({'dianpin_id':6209778},{'dianpin_id':1,'loc':1,'dianpin_tag':1,'shopname':1})# 6113943 4683333 6209778
    shops=[]
    fill_shop_ids={}
    for one in cur:
        one['name_short']=re.sub('\([^\)]*\)','',one['shopname'],flags=re.I)
        shops.append(one)
        fill_shop_ids[one['dianpin_id']]=one
    #找出周边所有竞争对手店铺
    for one in shops:
        loc=one.get('loc')
        cur=con.dianpin.shop.find({"loc":{"$within":{"$center":[[loc['lat'],loc['lng']],0.01]}}},{'dianpin_id':1,'dianpin_tag':1,'loc':1,'shopname':1})
        competitors=[]
        for other in cur:
            if other['dianpin_id']==one['dianpin_id']:
                continue
            tags=other['dianpin_tag']
            other['match']=len(set(tags).intersection(set(one['dianpin_tag'])))
            competitors.append(other)
        competitors.sort(lambda a,b:-cmp(a['match'],b['match']))
        competitors = competitors[0:40]
        for shop in competitors:
            shop['name_short']=re.sub('\([^\)]*\)','',shop['shopname'],flags=re.I)
            fill_shop_ids[shop['dianpin_id']]=shop
        competitors_id=[shop['_id'] for shop in competitors]
        con.dianpin.shop.update({'dianpin_id':one['dianpin_id']},{"$set":{"competitor":{'list':competitors_id,'time':time.time()}}})
        print 'processed',one['dianpin_id']

    #找出周边所有地理位置微薄
    radius=0.015
    center=shops[0]['loc']
    area=[[center['lat']-radius,center['lng']-radius],[center['lat']+radius,center['lng']+radius]]
    cur=con.weibolist.weibo.find({'pos':{'$within':{'$box':area}}})
    all_weibo={}
    for line in cur:
        all_weibo[line['weibo_id']]=line

    #生成用户到店记录和用户行动历史
    weibo_user_go_shop={}
    for shop_id in fill_shop_ids:
        oneshop=fill_shop_ids[shop_id]
        short_name=oneshop['name_short']
        shop_weibo=[]
        to_remove_weibo_id=[]
        for weibo_id in all_weibo:
            one_weibo=all_weibo[weibo_id]
            if short_name in one_weibo['word']:
                shop_weibo.append(one_weibo)
                to_remove_weibo_id.append(weibo_id)

                history=weibo_user_go_shop.get(one_weibo['uid'],{})
                record=history.get(shop_id,set())
                record.add(one_weibo['weibo_id'])
                history[shop_id]=record
                weibo_user_go_shop[one_weibo['uid']]=history

        for id in to_remove_weibo_id:
            del all_weibo[id]

        weibo_user_ids={}
        for w in shop_weibo:
            s_time=weibo_user_ids.get(w['uid'],0)
            weibo_user_ids['uid']=s_time+1
        weibo_user_ids_list=[]
        for uid in weibo_user_ids:
            weibo_user_ids_list.append((uid,weibo_user_ids[uid]))
        weibo_user_ids_list.sort(lambda a,b:-cmp(a[1],b[1]))
        con.dianpin.shop.update({'dianpin_id':shop},{'$set':{'weibo_users':weibo_user_ids_list}})

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
                for one in shop_list:
                    old_log[one['shop']]=set(one['weibos'])
            for shop_id in new_log:
                old_record=old_log.get(shop_id,set())
                old_log[shop_id]=old_record.union(new_log[shop_id])

        shop_list=[]
        for shop_id in old_log:
            shop_list.append({'shop':shop_id,'weibos':list(old_log[shop_id])})
        data['shop_log']=shop_list
        data['shop_log_update_time']=time.time()
        con.dianpin.user_log.update({'weibo_uid':uid},data,upsert=True)

