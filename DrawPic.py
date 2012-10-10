# -*- coding: utf-8 -*-
import Tkinter
import math
from PIL import Image,ImageTk,ImageDraw
import ConvexHull

def PointTransCoordinate(point,picturt_size,coordinate_size):
    xscal=float(picture_size[0])/(coordinate_size[2]-coordinate_size[0])
    yscal=float(picture_size[1])/(coordinate_size[3]-coordinate_size[1])
    xpos=float(point[0]-coordinate_size[0])*xscal
    ypos=float(point[1]-coordinate_size[1])*yscal
    return (xpos,picture_size[1]-ypos)

if __name__ == '__main__':
    pointBuffer=[(0,0),(2,6),(9,7),(6,9),(5,5)]
    li = ConvexHull.sortPoint(pointBuffer)
    res = ConvexHull.stack(li)
    print res

    top=Tkinter.Tk()
    picture_size=(200,200)
    coordinate_size=(-2,-2,10,10)
    res2=[PointTransCoordinate(point,picture_size,coordinate_size) for point in res]
    imgf=Image.new('RGB',picture_size,(255,255,255))
    imgdraw=ImageDraw.Draw(imgf)
    imgdraw.polygon(res2, fill="yellow", outline="red")
    img=ImageTk.PhotoImage(imgf)
    label2=Tkinter.Label(top,image=img)
    label2.pack()
    label2.image=img
    Tkinter.mainloop()