from ctypes import *

rsdb = CDLL("target/debug/librsdb.so")

rsdb.rsdb_create_config.argtypes = ()
rsdb.rsdb_create_config.restype = c_void_p

rsdb.rsdb_free_config.argtypes = (c_void_p,)

rsdb.rsdb_open_tree.argtypes = (c_void_p,)
rsdb.rsdb_open_tree.restype = c_void_p

rsdb.rsdb_free_tree.argtypes = (c_void_p,)

rsdb.rsdb_get.argtypes = (c_void_p, c_char_p, c_size_t, POINTER(c_size_t))
rsdb.rsdb_get.restype = c_char_p

rsdb.rsdb_scan.argtypes = (c_void_p, c_char_p, c_size_t, POINTER(c_size_t))
rsdb.rsdb_scan.restype = c_void_p

rsdb.rsdb_set.argtypes = (c_void_p, c_char_p, c_size_t, c_char_p, c_size_t)
rsdb.rsdb_set.restype = None

rsdb.rsdb_del.argtypes = (c_void_p, c_char_p, c_size_t)
rsdb.rsdb_del.restype = None

rsdb.rsdb_cas.argtypes = (c_void_p,
                          c_char_p, c_size_t, # key
                          c_char_p, c_size_t, # old
                          c_char_p, c_size_t, # new
                          POINTER(c_char_p), POINTER(c_size_t), # actual ret
                          )
rsdb.rsdb_cas.restype = c_ubyte

class Conf:
    def __init__(self):
        self.ptr = c_void_p(rsdb.rsdb_create_config())

    def tree(self):
        tree_ptr = rsdb.rsdb_open_tree(self.ptr)
        return Tree(c_void_p(tree_ptr))

    def __del__(self):
        rsdb.rsdb_free_config(self.ptr)


class TreeIterator:
    def __init__(self, ptr):
        self.ptr = ptr

    def __del__(self):
        rsdb.rsdb_free_iter(self.ptr)


class Tree:
    def __init__(self, ptr):
        self.ptr = ptr

    def __del__(self):
        rsdb.rsdb_free_tree(self.ptr)

    def set(self, key, val):
        rsdb.rsdb_set(self.ptr, key, len(key), val, len(val))

    def get(self, key):
        vallen = c_size_t(0)
        ptr = rsdb.rsdb_get(self.ptr, key, len(key), byref(vallen))
        return ptr[:vallen.value]

    def delete(self, key):
        rsdb.rsdb_del(self.ptr, key, len(key))

    def cas(self, key, old, new):
        actual_vallen = c_size_t(0)
        actual_val = c_char_p(0)
        success = rsdb.rsdb_cas(self.ptr, key,
                len(key),
                old, len(old),
                new, len(new),
                byref(val), byref(vallen))
        if success == 1:
            return True
        else:
            return actual_val[:actual_vallen.value]


    def scan(self, key):
        return rsdb.rsdb_scan(self.ptr, key, len(key))


if __name__ == '__main__':
    c = Conf()
    t = c.tree()
    t.set(b"k1", b"v1")
    print(t.get(b"k1"))
