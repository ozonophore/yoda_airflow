import threading

import psycopg2

wh_dict = dict()

class Warehouse:

    def __init__(self, code: str, source: str):
        self.code = code
        self.source = source

    def __hash__(self):
        return hash((self.code, self.source))

    def __eq__(self, other):
        return (self.code, self.source) == (other.code, other.source)

def read_warehouses(host: str, database: str, user: str, password: str) -> None:
    conn = psycopg2.connect(
        host=host,
        port=5432,
        database=database,
        user=user,
        password=password
    )
    try:
        wh_dict.clear()
        cursor = conn.cursor()
        cursor.execute("select code, source from dl.warehouse")
        warehouses = cursor.fetchall()
    finally:
        conn.close()

    for warehouse in warehouses:
        w = Warehouse(warehouse[0], warehouse[1])
        wh_dict[w] = True

def add_warehouse(code: str, source: str) -> dict:
    key = Warehouse(code, source)
    lock = threading.Lock()
    with lock:
        if wh_dict.get(key) is None:
            wh_dict[key] = False
    return wh_dict

def save_warehouses(host: str, database: str, user: str, password: str) -> None:
    conn = psycopg2.connect(
        host=host,
        port=5432,
        database=database,
        user=user,
        password=password
    )
    try:
        cursor = conn.cursor()
        for w in wh_dict.keys():
            if wh_dict[w] == False:
                cursor.execute("insert into dl.warehouse (code, source, cluster) values (%s, %s, 'NONE')", (w.code, w.source))
        conn.commit()
    finally:
        conn.close()

