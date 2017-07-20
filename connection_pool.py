from config import config
import psycopg2
from psycopg2 import pool


db = pool.SimpleConnectionPool(1, 10, **config())




        

con = db.getconn()

cur = con.cursor()
cur.execute("SELECT version();") 
con.commit()

id = cur.fetchone()
print id


cur.close()
db.putconn(con)