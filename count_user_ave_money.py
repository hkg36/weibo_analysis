#-*-coding:utf-8-*-
import pymongo
import pymongo.errors
import time
import re
import env_data
if __name__ == '__main__':
    con=pymongo.Connection(env_data.mongo_connect_str,read_preference=pymongo.ReadPreference.SECONDARY)
    cur=con.dianpin.user_log.find({})
    user_list=[]
    for one in cur:
        if one.get('ave_cost_update_time',0)<one['shop_log_update_time']:
            user_list.append(one)
    for one in user_list:
        shop_log=one['shop_log']
        shop_ids=[line['shop'] for line in shop_log]
        cur=con.dianpin.shop.find({'dianpin_id':{'$in':shop_ids}},{'dianpin_id':1,'aver_cost':1})
        shop_cost_list={}
        for line in cur:
            if 'aver_cost' in line:
                shop_cost_list[line['dianpin_id']]=line['aver_cost']
        all_count=0
        all_money=0
        for one_log in shop_log:
            cost=shop_cost_list.get(one_log['shop'],-1)
            if cost!=-1:
                all_count+=len(one_log['weibos'])
                all_money+=len(one_log['weibos'])*cost
        if all_count:
            ave_money=float(all_money)/all_count
        else:
            ave_money=0
        con.dianpin.user_log.update({"weibo_uid":one['weibo_uid']},{'$set':{'ave_cost':ave_money,'ave_cost_update_time':time.time()}})
        print one['weibo_uid'],'done'
