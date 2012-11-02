#-*-coding:utf-8-*-
import string
import math
settings = {
    'digi': 16,
    'add': 10,
    'plus': 7,
    'cha': 36,
    'center': {
        'lat': 34.957995,
        'lng': 107.050781,
        'isDef': True
    }
}
def intToStr(Num, radix):
    if Num<0:
        raise Exception('decode fail')
    _base = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    _res = ''
    while 1:
        _d = Num % radix
        _res += _base[_d]
        Num = Num / radix
        if Num == 0:
            return _res
def decodeDP_POI(a):
    b = -1
    c = 0
    d = ""
    e = len(a)
    g = ord(a[e - 1])
    a = a[0:e-1]
    e-=1
    for f in xrange(e):
        h = string.atoi(a[f], settings['cha']) - settings['add']
        if h >= settings['add']:
            h -= settings['plus']
        d += intToStr(h,settings['cha'])
        if h > c:
            b = f
            c = h

    a = string.atoi(d[0:b],settings['digi'])
    b = string.atoi(d[b + 1:], settings['digi'])
    g = (a + b - g) / 2
    b = float(b - g) / 1E5
    return {
        'lat': b,
        'lng': float(g) / 1E5
    }

def coordOffsetDecrypt(x,y):
    x = float(x)*100000%36000000;
    y = float(y)*100000%36000000;

    x1 = math.floor(-(((math.cos(y/100000))*(x/18000))+((math.sin(x/100000))*(y/9000)))+x);
    y1 = math.floor(-(((math.sin(y/100000))*(x/18000))+((math.cos(x/100000))*(y/9000)))+y);

    if x>0:
        xoff=1
    else:
        xoff=-1
    if y>0:
        yoff=1
    else:
        yoff=-1
    x2 = math.floor(-(((math.cos(y1/100000))*(x1/18000))+((math.sin(x1/100000))*(y1/9000)))+x+xoff);
    y2 = math.floor(-(((math.sin(y1/100000))*(x1/18000))+((math.cos(x1/100000))*(y1/9000)))+y+yoff);

    return [x2/100000.0,y2/100000.0]
def poi2coordinate(poi):
    pt1=decodeDP_POI(poi)
    pt2=coordOffsetDecrypt(pt1['lng'],pt1['lat'])
    return {'lat':pt2[1],'lng':pt2[0]}
if __name__ == '__main__':
    print poi2coordinate('GSTRWTZUBFDIFC')