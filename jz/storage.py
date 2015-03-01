import simplejson as json
import random
import lmdb
import struct
import ctypes


datatypes = [
    'uint64',
]


class Scanner(object):
    def __init__(self, env, db, start=None, end=None):
        self.env = env
        self.db = db
        self.start = start
        self.end = end

    def produce(self):
        # TODO: use zerocopy
        # with self.env.begin(buffers=True) as txn:
        with self.env.begin() as txn:
            cursor = txn.cursor(self.db)
            if self.start:
                cursor.set_range(self.start)
            for k, v in cursor.iternext(keys=True, values=True):
                yield k, v


class Index(object):
    def __init__(self, name, datatype, db):
        self.name = name
        self.datatype = datatype
        self.db = db


class Storage(object):
    def __init__(self, path):
        self.path = path
        self.env = self.open_env('main')
        self.docs_db = self._open_db('docs')
        self.index_db = self._open_db('index')
        self.indices = {}
        self._read_index_db()

    def _read_index_db(self):
        """ Populate index metadata by reading DB """
        with self.env.begin() as txn:
            cursor = txn.cursor(self.index_db)
            for name, index in cursor.iternext(keys=True, values=True):
                index = json.loads(index)
                self.indices[name] = Index(name, index['datatype'], self._open_index_db(name))

    def _open_db(self, name):
        return self.env.open_db(name)

    def _open_index_db(self, name):
        return self.env.open_db(name, dupsort=True)

    def open_env(self, name):
        db_path = self.path
        MAP_SIZE = 1048576 * 400
        env = lmdb.open(db_path, map_size=MAP_SIZE, max_dbs=256)
        # print(env.info())
        # print(env.stat())
        return env

    def create_index(self, data):
        data = json.loads(data)
        if data['datatype'] not in datatypes:
            raise Exception('Bad datatype')
        name = data['name']
        self.indices[name] = Index(name, data['datatype'], self._open_index_db(name))
        index = json.dumps({'datatype': data['datatype']})
        with self.env.begin(db=self.index_db, write=True, buffers=True) as txn:
            txn.put(name, index)

    def load(self, rows):
        for row in rows:
            self.put(json.dumps(row))

    def doc_scanner(self):
        return Scanner(self.env, self.docs_db)

    def index_scanner(self, name, start=None, end=None):
        col = self.indices[name]
        return Scanner(self.env, col.db, start=start, end=end)

    def produce(self):
        for i in Scanner(self.env, self.docs_db).produce():
            yield i

    def put(self, doc):
        key = ctypes.c_uint64.from_buffer(bytearray(8))
        key.value = random.getrandbits(64)

        data = json.loads(doc)

        # Put into doc storage
        with self.env.begin(db=self.docs_db, write=True, buffers=True) as txn:
            txn.put(key, doc)

        # Update indexes
        for col_name, value in data.items():
            try:
                col = self.indices[col_name]
            except KeyError:
                raise Exception('Unknown index')
            else:
                with self.env.begin(db=col.db, write=True, buffers=True) as txn:
                    # TODO: coelece datatypes
                    if col.datatype == 'uint64':
                        val = ctypes.c_uint64.from_buffer(bytearray(8))
                        val.value = int(value)
                    txn.put(val, key)

    def get(self, key):
        with self.env.begin(db=self.docs_db, buffers=True) as txn:
            return txn.get(key)
