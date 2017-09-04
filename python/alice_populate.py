#!/usr/bin/env python
from rsdb import Conf

c = Conf()
c.path(b"TREEEEEEE")
t = c.tree()
t.set(b"k1", b"v1")
t.close()
