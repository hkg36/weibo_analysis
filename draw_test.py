#-*-coding:utf-8-*-
import multiprocessing
import time
import gzip
from StringIO import StringIO
import json

if __name__ == "__main__":
    buf=StringIO()
    file=gzip.GzipFile(fileobj=buf,mode='w')
    data=[]
    for i in xrange(30):
        data.append({'id':i})
    json.dump(data,file)
    file.close()

    buf.pos=0
    file=gzip.GzipFile(fileobj=buf)
    data2=json.load(file)
    file.close()