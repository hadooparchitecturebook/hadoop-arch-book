#!/usr/bin/python

import random

foo_f = open('foo_file','w')
bar_f = open('bar_file','w')

for i in xrange(0,500):
    foo_id = i
    fooval = random.randint(0,100)
    bar_id = i % 25
    barval = random.randint(100,200)
    foo_f.write('%s|%s|%s\n' % (foo_id, fooval, bar_id))

    if (i<25):
        bar_f.write('%s|%s\n' % (bar_id, barval))