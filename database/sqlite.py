import sqlite3

def creat_db() :
    con = sqlite3.connect("paris_ve.db")
    cur = con.cursor()
    cur.execute("DROP TABLE paris_station_act")

    cur.execute("CREATE TABLE IF NOT EXISTS paris_station_act (time varchar(70),adress text,district varchar(5),status varchar(255),post_code varchar(5),lat float,long float,id_pdc varchar(50))")

    con.commit()
    con.close()
#creat_db()
def SQL_con(val) :
    con = sqlite3.connect("database/paris_ve.db",timeout=1)
    cur = con.cursor()
    cur.execute("INSERT INTO paris_station_act (time, adress, district, status, post_code, lat, long, id_pdc) VALUES (?,?,?,?,?,?,?,?)",val)
    con.commit()
    cur.execute(("SELECT * FROM paris_station_act"))
    res = cur.fetchall()
    print(res)
    con.close()


