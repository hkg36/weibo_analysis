#-*-coding:utf-8-*-
import pycurl
import json
import MySQLdb
import env_data
from StringIO import StringIO

if __name__ == '__main__':
    curl=pycurl.Curl()
    curl.setopt(pycurl.URL,'http://www.haomeiniu.com/backend/outputlist.php')
    curl.setopt(pycurl.TIMEOUT, 20)

    b = StringIO()
    curl.setopt(pycurl.WRITEFUNCTION, b.write)
    curl.perform()

    b.seek(0)
    data=json.load(b)

    sqlcon=MySQLdb.connect(host=env_data.mysql_host,user=env_data.mysql_user,passwd=env_data.mysql_psw,db='data_mining_xcj')
    sqlcon.set_character_set('utf8')
    sqlc=sqlcon.cursor()
    for line in data:
        sqlc.execute('insert into beautiful_girl(weibo_uid,access_token,expires_in,area,sex,time,headurl,avatar_large,screen_name)'+
            'values(%s,%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE weibo_uid=values(weibo_uid),access_token=values(access_token),'+
                     'expires_in=values(expires_in),area=values(area),sex=values(sex),time=values(time),headurl=values(headurl),'+
                     'avatar_large=values(avatar_large),screen_name=values(screen_name)',
            (line['weibo_uid'],line['oauth']['access_token'],line['oauth']['expires_in'],line['area'],1 if line['sex']=='f' else 0,line['time'],line['headurl'],line['avatar_large'],line['screen_name']))
    sqlcon.commit()
    sqlc.close()
    sqlcon.close()
