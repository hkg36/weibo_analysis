#-*-coding:utf-8-*-

from multiprocessing import Pool
import random

value=9
def increment(x):
    return value + 1

def decrement(x):
    return value - 1

def initer():
    global value
    value=random.randint(20,1000)

if __name__ == '__main__':
    pool = Pool(processes=2,initializer=initer)
    res1 = pool.map_async(increment, range(10))
    res1.wait()
    res2 = pool.map_async(decrement, range(10))
    res2.wait()
    print res1.get(timeout=1)
    print res2.get(timeout=1)