#-*-coding:utf-8-*-
import pymongo
import pymongo.errors
import time
import re
#分析店铺周边的竞争对手
def find_shop_competitor(dianpin_shopid):
    con=pymongo.Connection('mongodb://xcj.server4,xcj.server2/',read_preference=pymongo.ReadPreference.SECONDARY)
    shop=con.dianpin.shop.find_one({'dianpin_id':dianpin_shopid},{'dianpin_id':1,'loc':1,'dianpin_tag':1,'shopname':1,
                                                                  'atmosphere':1,'recommend':1})
    if shop==None:
        print 'shop not found'
        return
    shop['name_short']=re.sub('(?i)\([^\)]*\)','',shop['shopname'])
    if 'atmosphere' in shop:
        shop['atmosphere']=set(shop['atmosphere'])
    if 'recommend' in shop:
        shop['recommend']=set(shop['recommend'])
    if 'dianpin_tag' in shop:
        shop['dianpin_tag']=set(shop['dianpin_tag'])
        #找出周边所有竞争对手店铺
    loc=shop.get('loc')
    cur=con.dianpin.shop.find({"loc":{"$within":{"$center":[[loc['lat'],loc['lng']],0.02]}}},{'dianpin_id':1,'dianpin_tag':1,
                                                                                              'loc':1,'shopname':1,'atmosphere':1,'recommend':1})
    competitors=[]
    for other in cur:
        if other['dianpin_id']==shop['dianpin_id']:
            continue
        match_score=0
        if 'dianpin_tag' in shop and 'dianpin_tag' in other:
            match_score+=len(set(other['dianpin_tag']).intersection(shop['dianpin_tag']))*2
        if 'atmosphere' in shop and 'atmosphere' in other:
            match_score+=len(set(other['atmosphere']).intersection(shop['atmosphere']))*1
        if 'recommend' in shop and 'recommend' in other:
            match_score+=len(set(other['recommend']).intersection(shop['recommend']))*2
        other['match']=match_score
        competitors.append(other)
    competitors.sort(lambda a,b:-cmp(a['match'],b['match']))
    competitors_id=[s['dianpin_id'] for s in competitors[0:40]]
    con.dianpin.shop.update({'dianpin_id':shop['dianpin_id']},{"$set":{"competitor":{'list':competitors_id,'time':time.time()}}})
    print 'processed',shop['dianpin_id']

def count_user_go_competitor(shop_id):
    con=pymongo.Connection('mongodb://xcj.server4,xcj.server2/',read_preference=pymongo.ReadPreference.SECONDARY)
    master_shop=con.dianpin.shop.find_one({'dianpin_id':shop_id},{'dianpin_id':1,'competitor':1,'weibo_users':1,'watch_shop':1})
    competitor=master_shop['competitor']['list']

    watch_shop=master_shop.get('watch_shop',[])
    for ws in watch_shop:
        if ws in competitor:
            competitor.remove(ws)
    master_weibo_users=master_shop['weibo_users']

    find_shop_info=watch_shop[:]
    find_shop_info.extend(competitor)

    competitor_info={}
    cur=con.dianpin.shop.find({'dianpin_id':{'$in':find_shop_info}},{'dianpin_id':1,'weibo_users':1})
    for line in cur:
        competitor_info[line['dianpin_id']]=line

    weibo_user_added=set([u[0] for u in master_weibo_users])
    weibo_potential_users=[]
    for oc in competitor:
        c_info=competitor_info.get(oc)
        if c_info==None:
            continue
        weibo_users=c_info.get('weibo_users')
        if weibo_users==None:
            continue
        for wu in weibo_users:
            if wu[0] not in weibo_user_added:
                weibo_user_added.add(wu[0])
                weibo_potential_users.append(wu[0])

    user_last_act_weibo={}
    find_shop_info=set(find_shop_info)
    for weibo_uid in weibo_potential_users:
        one_user=con.dianpin.user_log.find_one({'weibo_uid':weibo_uid},{'shop_log':1})
        max_weibo_id=0
        if one_user != None:
            shop_log=one_user.get('shop_log',[])
            for sl in shop_log:
                #if sl['shop'] in find_shop_info:
                    m=max(sl['weibos'])
                    user_last_act_weibo[weibo_uid]=max(m,user_last_act_weibo.get(weibo_uid,0))
    user_last_act_weibo=[(k,v) for k,v in user_last_act_weibo.iteritems()]
    user_last_act_weibo.sort(lambda a,b:-cmp(a[1],b[1]))
    latest_weibo_potential_users=[o[0] for o in user_last_act_weibo[0:20]]

    con.dianpin.shop.update({'dianpin_id':master_shop['dianpin_id']},{'$set':{'potential_weibo_users':weibo_potential_users,
                                                                              'latest_potential_weibo_users':latest_weibo_potential_users}})
if __name__ == '__main__':
#港丽餐厅(大悦城店) 2384860
#海底捞（西单店）2114887
#麻辣诱惑(三里屯Village西南) 2814994
#6113943 4683333 6209778
    shop_ids=[2384860,2114887,2814994,6113943,4683333,6209778]
    #shop_ids=[6209778]
    for sid in shop_ids:
        find_shop_competitor(sid)
    for sid in shop_ids:
        count_user_go_competitor(sid)