import sqlite3
from time import sleep

def create_db() :
    con = sqlite3.connect("paris_ve.db")
    cur = con.cursor()
    #cur.execute("DROP TABLE paris_station_act")
    #cur.execute("CREATE TABLE IF NOT EXISTS paris_station_act (time varchar(70),adress text,district varchar(5),status varchar(255),post_code varchar(5),lat float,long float,id_pdc varchar(50))")
    cur.execute("""CREATE TABLE IF NOT EXISTS paris_station_act (
                                            time,
                                            time_for_duplicate_rows,
                                            adress,
                                            district ,
                                            status ,
                                            post_code,
                                            lat ,
                                            long ,
                                            id_pdc, 
                                            CONSTRAINT ct_name UNIQUE(time_for_duplicate_rows,status, id_pdc) )
                                             """)
    con.commit()
    con.close()
create_db()
def SQL_con(val) :
    con = sqlite3.connect("database/paris_ve.db",timeout=1)
    cur = con.cursor()
    cur.execute("INSERT OR IGNORE INTO paris_station_act (time,time_for_duplicate_rows, adress, district, status, post_code, lat, long, id_pdc) VALUES (?,?,?,?,?,?,?,?,?)",val)
    con.commit()
    cur.execute(("SELECT * FROM paris_station_act"))
    res = cur.fetchall()
    print(res)
    con.close()
    sleep(1)

def delete_duplicate_rows() :
    con = sqlite3.connect("database/paris_ve.db",timeout=1)
    cur = con.cursor()
    cur.execute("""WITH CTE AS (SELECT time, adress, district, status, post_code, lat, long, id_pdc, ROW_NUMBER()
                OVER( PARTITION BY time,id_pdc ORDER BY time,id_pdc) row_num
                FROM paris_station_act )
                SELECT * from CTE where row_num = 1""")
    con.commit()
    res = cur.fetchall()
    print(res)
    con.close()
    sleep(1)

