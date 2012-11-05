#-*-coding:utf-8-*-
import pymongo
import codecs
import decode_poi
import string
import re

def GetPlaceNameList():
    file=codecs.open('data_in/place_name.txt','r','utf-8')
    addresses=set()
    for line in file:
        addresses.add(line.strip(u' \r\n'))
    file.close()
    return addresses
if __name__ == '__main__':
    #addrs=GetPlaceNameList()

    con=pymongo.Connection('mongodb://xcj.server4,xcj.server2/',read_preference=pymongo.ReadPreference.SECONDARY)
    #con=pymongo.Connection('mongodb://xcj.server4/',read_preference=pymongo.ReadPreference.SECONDARY)
    cur=con.dianping.shops.find({},{'atmosphere':1,'POI':1,'address':1,'shop_name':1,'tags':1,'recommend':1,'avg_price':1,
                                    'shop_id':1,'alias':1})
    dianpin_shopdata={}
    for s in cur:
        shop={}
        try:
            if len(s['POI'])==0:
                continue
            shop['loc']=decode_poi.poi2coordinate(s['POI'])
        except Exception,e:
            continue
        try:
            if len(s['atmosphere'])>0:
                shop['atmosphere']=list(set(re.split(r'[/,]+',s['atmosphere'])))
            shop['address']=s['address']
            shop['dianpin_id']=int(s['shop_id'])
            shop['dianpin_poi']=s['POI']
            shop['shopname']=s['shop_name']
            ave_p=s.get('avg_price',0)
            if isinstance(ave_p,int):
                shop['aver_cost']=int(ave_p)
            elif isinstance(ave_p,unicode) and len(ave_p):
                shop['aver_cost']=int(ave_p)
            if isinstance(s.get('recommend'),unicode) and len(s.get('recommend'))>0:
                shop['recommend']=list(set(re.split(r'[/,]+',s['recommend'])))
            if len(s.get('tags'))>0:
                shop['dianpin_tag']=list(set(re.split(r'[/,]+',s['tags'])))
            alias=s.get('alias','')
            if isinstance(alias,unicode) and len(alias)>0:
                p_pos=alias.find(',')
                if p_pos==-1:
                    shop['alias']=alias
                else:
                    shop['alias']=alias[0:p_pos]
            dianpin_shopdata[shop['dianpin_id']]=shop
        except Exception,e:
            print 'error %d'%s['shop_id']
            print e
            continue

    for shop in dianpin_shopdata.values():
        print shop['dianpin_id']
        con.dianpin.shop.update({'dianpin_id':shop['dianpin_id']},{'$set':shop},upsert=True)