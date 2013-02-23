#-*-coding:utf-8-*-
import sqlite3
import pymongo
import re
import json
import gzip

def GetAllPoint():
    con=sqlite3.connect('../fetchDianPin/GeoPointList.db')
    try:
        con.execute('alter table geoweibopoint add column analysis_checked int default 0')
    except Exception,e:
        print e
    cc=con.cursor()
    cc.execute('select id,lat,lng from geoweibopoint')
    all_point=[]
    for id,lat,lng in cc:
        all_point.append({'id':id,'lat':lat,'lng':lng})
    cc.close()
    con.close()
    return all_point
def analysis_point(center):
    print center
    fill_shop_ids=[]
    con=pymongo.Connection('mongodb://xcj.server4:27010/',read_preference=pymongo.ReadPreference.SECONDARY_ONLY)
    cur=con.dianpin.shop.find({"loc":{"$within":{"$center":[[center['lat'],center['lng']],0.01]}}},
                              {'_id':False,'dianpin_id':1,'dianpin_tag':1,'loc':1,'shopname':1,'atmosphere':1,'recommend':1,'alias':1})
    for line in cur:
        line['shopname']=unicode(line['shopname'])
        name_short=set((re.sub('(?i)\([^\)]*\)','',line['shopname']),))
        if line.has_key('alias'):
            name_short.add(line['alias'])
        line['name_short']=list(name_short)
        alltag=set()
        if line.has_key('dianpin_tag'):
            alltag.update(line['dianpin_tag'])
        if line.has_key('atmosphere'):
            alltag.update(line['atmosphere'])
        if line.has_key('recommend'):
            alltag.update(line['recommend'])
        line['alltag']=list(alltag)
        fill_shop_ids.append(line)
    return fill_shop_ids
if __name__ == '__main__':
    all_point=GetAllPoint()
    shop_info_list=[]
    for center in all_point:
        shop_info=analysis_point(center)
        shop_info_list.append({'center':center,'shops':shop_info})
    out_file=gzip.open('data/allpointshop.gz','w')
    json.dump(shop_info_list,out_file)
    out_file.close()