#-*-coding:utf-8-*-
import pymongo
import pymongo.errors
import time
import re
if __name__ == '__main__':
    con=pymongo.Connection('mongodb://xcj.server4,xcj.server2/?slaveOk=true')
    cur=con.dianpin.shop.find({'dianpin_id':4683333},{'dianpin_id':1,'loc':1,'dianpin_tag':1,'shopname':1})
    shops=[]
    fill_shop_ids={}
    for one in cur:
        one['name_short']=re.sub('\([^\)]*\)','',one['shopname'],flags=re.I)
        shops.append(one)
        fill_shop_ids[one['dianpin_id']]=one
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
        competitors = competitors[0:20]
        for shop in competitors:
            shop['name_short']=re.sub('\([^\)]*\)','',shop['shopname'],flags=re.I)
            fill_shop_ids[shop['dianpin_id']]=shop
        competitors_id=[shop['_id'] for shop in competitors]
        con.dianpin.shop.update({'dianpin_id':one['dianpin_id']},{"$set":{"competitor":{'list':competitors_id,'time':time.time()}}})
        print 'processed',one['dianpin_id']

    radius=0.015
    center=shops[0]['loc']
    area=[[center['lat']-radius,center['lng']-radius],[center['lat']+radius,center['lng']+radius]]
    cur=con.weibolist.weibo.find({'pos':{'$within':{'$box':area}}})
    all_weibo={}
    for line in cur:
        all_weibo[line['weibo_id']]=line
    for shop in fill_shop_ids:
        oneshop=fill_shop_ids[shop]
        short_name=oneshop['name_short']
        shop_weibo=[]
        to_rm_weibo_id=[]
        for weibo_id in all_weibo:
            one_weibo=all_weibo[weibo_id]
            if short_name in one_weibo['word']:
                shop_weibo.append(one_weibo)
                to_rm_weibo_id.append(weibo_id)
        for id in to_rm_weibo_id:
            del all_weibo[id]

        weibo_user_ids={}
        for w in shop_weibo:
            if w['uid'] in weibo_user_ids:
                weibo_user_ids[w['uid']]+=1
            else:
                weibo_user_ids[w['uid']]=1
        weibo_user_ids_list=[]
        for uid in weibo_user_ids:
            weibo_user_ids_list.append((uid,weibo_user_ids[uid]))
        weibo_user_ids_list.sort(lambda a,b:-cmp(a[1],b[1]))
        con.dianpin.shop.update({'dianpin_id':shop},{'$set':{'weibo_users':weibo_user_ids_list}})
    print len(all_weibo)

