# -*- coding: utf-8 -*-
import timeit

# z = set()
# print type(z)

a = [{"Rid": 1}, {"Rid":2}, {"Rid":3}]

b = set(x["Rid"] for x in a)
c =[x["Rid"] for x in a]
# print b[1]

def set_add():
    for i in xrange(10): b.add(i)
    print b

def list_add():
    for i in xrange(10): c.append(i)
    print c

# print timeit.timeit("for i in xrange(10): b.add(i)", 'a = [{"Rid": 1}, {"Rid":2}]; b = set(x["Rid"] for x in a)', number=1000000)
# print timeit.timeit("for i in xrange(10): c.append(i)", 'a = [{"Rid": 1}, {"Rid":2}]; c = [x["Rid"] for x in a]', number=1000000)

# set_add()
# list_add()

print timeit.timeit('b = set(x["Rid"] for x in a)', 'a = [{"Rid": 1}, {"Rid":2},]', number = 100000)
print timeit.timeit('b = [x["Rid"] for x in a]', 'a = [{"Rid": 1}, {"Rid":2}]', number = 100000)