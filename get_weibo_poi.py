#-*-coding:utf-8-*-
import weibo_tools
import gzip
import json
import pymongo
import env_data
import mongo_autoreconnect
if __name__ == '__main__':
    weibo_tools.UseRandomLocalAddress()
    client=weibo_tools.DefaultWeiboClient()

    f=gzip.open('data/allpointshop.gz','r')
    shoppoints=json.load(f)
    f.close()

    centers=[];
    for point in shoppoints:
        pt=point.get('center')
        if pt is not None:
            centers.append(pt)

    shoppoints=None

    con=pymongo.Connection(env_data.mongo_connect_str,read_preference=pymongo.ReadPreference.PRIMARY)
    for point in centers:
        total_number=-1
        for page in xrange(1,100):
            if total_number>=0:
                if total_number<(page-1)*50:
                    break
            for t in xrange(5):
                try:
                    result=client.place__nearby__pois(lat=point['lat'],long=point['lng'],
                                                  offset=1,range=10000,count=50,sort=1,page=page)
                    break
                except Exception,e:
                    print e
            try:
                pois=result.get("pois")
                total_number=result.get("total_number")
                print page
                for poi in pois:
                    con.weibo_pois.pois.insert(poi)
            except Exception,e:
                print e