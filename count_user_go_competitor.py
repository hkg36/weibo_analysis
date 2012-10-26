#-*-coding:utf-8-*-
import pymongo
import pymongo.errors
import time
import re

def count_user_go_competitor(shop_id):
    con=pymongo.Connection('mongodb://xcj.server4,xcj.server2/?slaveOk=true')
    master_shop=con.dianpin.shop.find_one({'dianpin_id':shop_id},{'dianpin_id':1,'competitor':1,'weibo_users':1})
    competitor=master_shop['competitor']['list']
    master_weibo_users=master_shop['weibo_users']

    competitor_info={}
    cur=con.dianpin.shop.find({'dianpin_id':{'$in':competitor}},{'dianpin_id':1,'weibo_users':1})
    for line in cur:
        competitor_info[line['dianpin_id']]=line

    competitor=set(competitor)

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
    for weibo_uid in weibo_potential_users:
        one_user=con.dianpin.user_log.find_one({'weibo_uid':weibo_uid},{'shop_log':1})
        max_weibo_id=0
        if one_user != None:
            shop_log=one_user.get('shop_log',[])
            for sl in shop_log:
                if sl['shop'] in competitor:
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
    for sid in shop_ids:
        count_user_go_competitor(sid)