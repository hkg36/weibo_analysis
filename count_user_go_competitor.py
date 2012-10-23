#-*-coding:utf-8-*-
import pymongo
import pymongo.errors
import time
import re

if __name__ == '__main__':
    con=pymongo.Connection('mongodb://xcj.server4,xcj.server2/?slaveOk=true')
    master_shop=con.dianpin.shop.find_one({'dianpin_id':4683333},{'dianpin_id':1,'competitor':1,'weibo_users':1})
    competitor=master_shop['competitor']['list']
    master_weibo_users=master_shop['weibo_users']

    competitor_info={}
    cur=con.dianpin.shop.find({'_id':{'$in':competitor}},{'dianpin_id':1,'weibo_users':1})
    for line in cur:
        competitor_info[line['_id']]=line

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
    con.dianpin.shop.update({'dianpin_id':master_shop['dianpin_id']},{'$set':{'potential_weibo_users':weibo_potential_users}})
