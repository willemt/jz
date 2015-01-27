import simplejson as json
import random
import lmdb
import struct
import ctypes






datatypes = [
    'INT',
]


class Scanner(object):
    def __init__(self, env, db):
        self.env = env
        self.db = db

    def produce(self):
        # TODO: use zerocopy
        # with self.env.begin(buffers=True) as txn:
        with self.env.begin() as txn:
            cursor = txn.cursor(self.db)
            for k, v in cursor.iternext(keys=True, values=True):
                yield k, v


class Column(object):
    def __init__(self, name, datatype, db):
        self.name = name
        self.datatype = datatype
        self.db = db


class Storage(object):
    def __init__(self):
        self.env = self.open_env('main')
        self.docs = self._open_db('docs')
        self.meta = self._open_db('meta')
        self.columns = {}
        self._read_meta()

    def _read_meta(self):
        """ Populate column metadata """
        with self.env.begin() as txn:
            cursor = txn.cursor(self.meta)
            for name, meta in cursor.iternext(keys=True, values=True):
                meta = json.loads(meta)
                self.columns[name] = Column(name, meta['datatype'], self._open_column_db(name))

    def _open_db(self, name):
        return self.env.open_db(name)

    def _open_column_db(self, name):
            return self.env.open_db(name, dupsort=True)

    def open_env(self, name):
        db_path = 'db{0}'.format(name)
        MAP_SIZE = 1048576 * 400
        env = lmdb.open(db_path, map_size=MAP_SIZE, max_dbs=256)
        # print(env.info())
        # print(env.stat())
        return env

    def create_column(self, data):
        data = json.loads(data)
        if data['datatype'] not in datatypes:
            raise Exception('Bad datatype')
        name = data['name']
        self.columns[name] = Column(name, data['datatype'], self._open_column_db(name))
        meta = json.dumps({'datatype': data['datatype']})
        with self.env.begin(db=self.meta, write=True, buffers=True) as txn:
            txn.put(name, meta)

    def load(self, rows):
        for row in rows:
            self.put(json.dumps(row))

    def doc_scanner(self):
        return Scanner(self.env, self.docs)

    def column_scanner(self, name):
        col = self.columns[name]
        return Scanner(self.env, col.db)

    def produce(self):
        for i in Scanner(self.env, self.docs).produce():
            yield i

    def put(self, doc):
        key = ctypes.c_uint64.from_buffer(bytearray(8))
        key.value = random.getrandbits(64)

        data = json.loads(doc)

        # Put into doc storage
        with self.env.begin(db=self.docs, write=True, buffers=True) as txn:
            txn.put(key, doc)

        # Update indexes
        for col_name, value in data.items():
            try:
                col = self.columns[col_name]
            except KeyError:
                raise Exception('Unknown column')
            else:
                with self.env.begin(db=col.db, write=True, buffers=True) as txn:
                    # TODO: coelece datatypes
                    if col.datatype == 'INT':
                        val = ctypes.c_uint64.from_buffer(bytearray(8))
                        val.value = int(value)
                    txn.put(val, key)

    def get(self, key):
        with self.env.begin(db=self.docs, buffers=True) as txn:
            return txn.get(key)
