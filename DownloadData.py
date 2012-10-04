#-*-coding:utf-8-*-
import pymongo
import sqlite3
import re
try:
    import ujson as json
except:
    import json

if __name__ == '__main__':
    sqldb=sqlite3.connect('../fetchDianPin/dianpinData.db')

    con=pymongo.Connection('218.241.207.46',27017)
    weibo_list=con.weibolist
    weibo_l_w=weibo_list.weibo
    weibo_l_u=weibo_list.user

    center=(39.931985,116.440918)
    radius=0.005
    area=[[center[0]-radius,center[1]-radius],[center[0]+radius,center[1]+radius]]

    sqlcur=sqldb.cursor()
    sqlcur.execute('select shopid,lat,lng,address,shopname from shop_list where lat>? and lat<? and lng>? and lng<?',(area[0][0],area[1][0],
        area[0][1],area[1][1]))
    shopinfos={}
    shop_id_infos={}
    for shopid,lat,lng,address,shopname in sqlcur:
        shopname_short=re.sub('\([^\)]*\)','',shopname,flags=re.I)
        info={'id':shopid,'pos':[lat,lng],'src_name':shopname}
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
                if 'weibo_list' in info:
                    wl=info['weibo_list']
                else:
                    wl=[]
                    info['weibo_list']=wl
                wl.append(line)

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
        print s_name,w_count

