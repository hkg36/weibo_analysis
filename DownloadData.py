#-*-coding:utf-8-*-
import pymongo
import sqlite3
import re
import codecs
import os
import string
try:
    import ujson as json
except:
    import json

if __name__ == '__main__':
    pointlist={}

    sqldb=sqlite3.connect('../fetchDianPin/GeoPointList.db')
    sqlcur=sqldb.cursor()
    sqlcur.execute('select tag,lat,lng from geoweibopoint where lat>39.734727 and lng>116.149659 and lat<40.072546 and lng<116.611777')
    for tag,lat,lng in sqlcur:
        if tag in pointlist:
            ptl=pointlist[tag]
        else:
            ptl=[]
            pointlist[tag]=ptl
        ptl.append((lat,lng))

    sqlcur.close()

    for point in pointlist:
        ptl=pointlist[point]
        point=point.replace(u'/',u'_')
        root_path=u'data/%s'%point
        print root_path
        if not os.path.exists(root_path):
            os.mkdir(root_path)

        sqldb=sqlite3.connect('../fetchDianPin/dianpinData.db')

        con=pymongo.Connection('218.241.207.46',27017)
        weibo_list=con.weibolist
        weibo_l_w=weibo_list.weibo
        weibo_l_u=weibo_list.user

        shopinfos={}
        shop_id_infos={}

        for center in ptl:
            #center=(39.931985,116.440918)
            radius=0.01
            area=[[center[0]-radius,center[1]-radius],[center[0]+radius,center[1]+radius]]

            sqlcur=sqldb.cursor()
            sqlcur.execute('select shopid,lat,lng,address,shopname from shop_list where lat>? and lat<? and lng>? and lng<?',(area[0][0],area[1][0],
                area[0][1],area[1][1]))

            for shopid,lat,lng,address,shopname in sqlcur:
                shopname_short=re.sub('\([^\)]*\)','',shopname,flags=re.I)
                info={'id':shopid,'pos':[lat,lng],'src_name':shopname,'weibo_list':[],'user_list':set()}
                shopinfos[shopname_short]=info
                shop_id_infos[shopid]=info
            sqlcur.close()

            cur=weibo_l_w.find({'pos':{'$within':{'$box':area}}})
            weibos=[]
            for line in cur:
                weibos.append(line)

        user_go_shop={}
        for line in weibos:
            word=line['word']
            for s_name in shopinfos:
                if s_name in word:
                    info=shopinfos[s_name]
                    info['weibo_list'].append(line)
                    info['user_list'].add(line['uid'])

                    if line['uid'] in user_go_shop:
                        go_shop=user_go_shop[line['uid']]
                    else:
                        go_shop={}
                        user_go_shop[line['uid']]=go_shop
                    if info['id'] in go_shop:
                        go_shop[info['id']]+=1
                    else:
                        go_shop[info['id']]=1
                    break

        for s_name in shopinfos:
            info=shopinfos[s_name]
            w_count=0
            if 'weibo_list' in info:
                w_count=len(info['weibo_list'])

        shop_all_info=shopinfos.values()
        shop_all_info.sort(lambda a,b:-cmp(len(a['user_list']),len(b['user_list'])))

        u_cur=weibo_l_u.find({'id':{'$in':user_go_shop.keys()}})
        all_user_info={}
        for u in u_cur:
            all_user_info[u['id']]=u

        user_go_shop_temp=[]
        for uid in user_go_shop:
            shop_go=user_go_shop[uid]
            if uid not in all_user_info:
                continue
            shop_go_list=[]
            user_go_one={'user':all_user_info[uid],'go_list':shop_go_list}
            for s_id in shop_go:
                times=shop_go[s_id]
                s_info=shop_id_infos[s_id]
                shop_go_list.append({'shop':s_info,'time':times})
            shop_go_list.sort(lambda a,b:-cmp(a['time'],b['time']))
            user_go_shop_temp.append(user_go_one)
        user_go_shop=user_go_shop_temp
        user_go_shop.sort(lambda a,b:-cmp(len(a['go_list']),len(b['go_list'])))

        file=codecs.open(root_path+u'/店铺.csv','w','GB2312','ignore')
        file.write(u'编号,店名,纬度,经度,微薄数,人数\n')
        for info in shop_all_info:
            file.write(u'%d,%s,%g,%g,%d,%d\n'%(info['id'],info['src_name'],info['pos'][0],info['pos'][1],
                                             len(info['weibo_list']),len(info['user_list'])))
        file.close()

        file=codecs.open(root_path+u'/客户.csv','w','GB2312','ignore')
        file.write(u'微薄用户,用户名,关注,粉丝,到过店铺数,到过的店铺\n')
        for line in user_go_shop:
            user=line['user']
            go_list=line['go_list']
            file.write(u'http://weibo.com/%s,%s,%d,%d,%d,'%(user['profile_url'],user['screen_name'],user['friends_count'],user['followers_count'],len(go_list)))
            for one_shop in go_list:
                file.write(u'%s %d次,'%(one_shop['shop']['src_name'],one_shop['time']))
            file.write('\n')
        file.close()






