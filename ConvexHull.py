#! /usr/bin/env python
# -*- coding: utf-8 -*-
# fileName : ConvexHull.py

class MyPoint(object):
    def __init__(self,point = (0,0)):
        self.point = point
        self.x = float(self.point[0])
        self.y = float(self.point[1])
    def __str__(self):
        return str(self.point)



class MyLine(MyPoint):
    def __init__(self,point):
        self.point = point
        super(MyLine,self).__init__(self.point)
    def __str__(self):
        return super(MyLine,self).__str__()

def multiply(pa,pb,pc): #判断pc是否在papb的左边，大于0则在左边
    pa = MyPoint(pa)
    pb = MyPoint(pb)
    pc = MyPoint(pc)
    return ((pb.x - pa.x)*(pc.y-pa.y) - (pc.x-pa.x)*(pb.y-pa.y))

def dis(pa,pb):         #得到pa,pb两点之间的距离
    pa = MyPoint(pa)
    pb = MyPoint(pb)
    return ((pa.x-pb.x)**2 + (pa.y-pb.y)**2)


def sortPoint(pointList):
    p0 = pointList[0]
    k = 0
    for i in xrange(len(pointList)):
        if MyPoint(p0).y  - MyPoint(pointList[i]).y > 0.001:
            p0 = pointList[i]
            k = i
        elif abs(MyPoint(p0).y  - MyPoint(pointList[i]).y) < 0.001:
            if MyPoint(p0).x > MyPoint(pointList[i]).x:
                p0 = pointList[i]
                k = i
        else:
            continue

    pointList.pop(k)

    num = []
    for i in xrange(1,len(pointList)):    #找出极值相同的点
        for j in xrange(i):
            if abs(multiply(p0,pointList[j],pointList[i])) < 0.001:
                if dis(p0,pointList[i]) > dis(p0,pointList[j]):
                    num.append(pointList[j])
                else:
                    num.append(pointList[i])
    num = [obj for obj in num if obj not in locals()['_[1]']]

    for obj in num:         #去除极值相同的点
        if obj in pointList:
            pointList.remove(obj)

    for i in xrange(len(pointList)):      #冒泡排序，以p0为中心，点集中的所有点按关于p0的极角逆时针排序,形成p1,p2,..pn-1 
        for j in xrange(i+1):
            if multiply(p0,pointList[j],pointList[i])<-0.001:
                pointList[i],pointList[j] = pointList[j],pointList[i]

    pointList.insert(0,p0)
    return pointList


def stack(pointOrder):
    pointStack = []
    pointList = pointOrder

    for i in xrange(3): #p0...p2入栈
        pointStack.append(pointList[i])

    for i in xrange(3,len(pointList)):  #对于 P[3..n-1]的每个点，若栈顶的两个点与它不构成"向左转"的关系,则将栈顶的点出栈,直至没有点需要出栈以后将当前点进栈
        pointStack.append(pointList[i])
        while multiply(pointStack[-3],pointStack[-2],pointStack[-1])<0:
            pointStack.pop(-2)

    return pointStack

if __name__ == '__main__':
    pointBuffer=[(0,0),(20,60),(90,70),(60,90),(50,50)]
    li = sortPoint(pointBuffer)
    res = stack(li)
    print res

