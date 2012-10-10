# -*- coding: utf-8 -*-
import Tkinter
import math
from PIL import Image,ImageTk,ImageDraw,ImageDraw2
import ConvexHull
class CoordinateTrans:
    def __init__(self,picture_size,coordinate_size):
        self.picture_size=picture_size
        self.coordinate_size=coordinate_size
        self.xscal=float(self.picture_size[0])/(self.coordinate_size[2]-self.coordinate_size[0])
        self.yscal=float(self.picture_size[1])/(self.coordinate_size[3]-self.coordinate_size[1])
    def PointTransCoordinate(self,point):
        xpos=float(point[0]-self.coordinate_size[0])*self.xscal
        ypos=float(point[1]-self.coordinate_size[1])*self.yscal
        return (xpos,self.picture_size[1]-ypos)

if __name__ == '__main__':
    pointBuffer=[(0,0),(2,6),(9,7),(6,9),(5,5)]
    li = ConvexHull.sortPoint(pointBuffer)
    res = ConvexHull.stack(li)
    print res

    top=Tkinter.Tk()
    picture_size=(800,800)
    coordinate_size=(-2,-2,10,10)
    ct=CoordinateTrans(picture_size,coordinate_size)
    res2=[ct.PointTransCoordinate(point) for point in res]
    imgf=Image.new('RGB',picture_size,(255,255,255))
    imgdraw=ImageDraw.Draw(imgf)
    imgdraw.polygon(res2, fill="yellow", outline="red")

    zero_point=ct.PointTransCoordinate((0,0))
    if zero_point[0]>0 and zero_point[0]<picture_size[0]:
        imgdraw.line([(zero_point[0],0),(zero_point[0],picture_size[1])],fill="black")
    if zero_point[1]>0 and zero_point[1]<picture_size[1]:
        imgdraw.line([(0,zero_point[1]),(picture_size[0],zero_point[1])],fill="black")

    img=ImageTk.PhotoImage(imgf)
    label2=Tkinter.Label(top,image=img)
    label2.pack()
    label2.image=img
    Tkinter.mainloop()