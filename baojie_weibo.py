# -*- coding: utf-8 -*-
import pymongo
import env_data
import codecs
import weibo_tools

if __name__ == '__main__':
    conn=pymongo.Connection(env_data.mongo_connect_str,read_preference=pymongo.ReadPreference.SECONDARY_ONLY)

    cur=conn.weibolist.user.find({'friend_list':2169751551},
        {'profile_url':True,'friend_list':True,'profile_url':True,'screen_name':True,'id':True,'tags':True,'followers_count':True})
    follow_count={}
    tag_count={}
    users_list={}

    count=0
    for line in cur:
        fl=line.get('friend_list')
        for friend in fl:
            follow_count[friend]=follow_count.get(friend,0)+1
        tl=line.get('tags')
        for tag in tl:
            for key in tag:
                if key != 'weight':
                    tag_name=tag[key]
                    tag_count[tag_name]=tag_count.get(tag_name,0)+1
                    break
        users_list[line['id']]=line

        count+=1
        if count>100:
            break
    follow_count=[[one,follow_count[one]] for one in follow_count]
    tag_count=[(one,tag_count[one]) for one in tag_count]
    users_list=[(one['id'],one['screen_name'],'http://weibo.com/'+one['profile_url'],one['followers_count']) for one in users_list.values()]

    follow_count.sort(lambda a,b:-cmp(a[1],b[1]))
    tag_count.sort(lambda a,b:-cmp(a[1],b[1]))
    users_list.sort(lambda a,b:-cmp(a[3],b[3]))

    follow_count=follow_count[0:500]
    client=weibo_tools.DefaultWeiboClient()
    for line in follow_count:
        try:
            res=client.users__show(line[0])
            screen_name=res.get('screen_name')
            profile_url=res.get('profile_url')
            line.append(screen_name)
            line.append('http://weibo.com/'+profile_url)

            print 'read %s'%(screen_name,)
        except Exception,e:
            line.append('')
            line.append('')
            print e

    fp=codecs.open('data/most_follow.csv','w','gbk')
    print >>fp,u'id,关注数,昵称,链接'
    for line in follow_count:
        print >>fp,u'%d,%d,%s,%s'%line
    fp.close()
    fp=codecs.open('data/most_tag.csv','w','gbk')
    print >>fp,u'标签,数量'
    for line in tag_count:
        print >>fp,u'%s,%d'%line
    fp.close()
    fp=codecs.open('data/user_list.csv','w','gbk')
    print >>fp,u'id,昵称,链接,粉丝'
    for line in users_list:
        print >>fp,u'%d,%s,%s,%d'%line
    fp.close()