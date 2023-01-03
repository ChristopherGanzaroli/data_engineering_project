import sqlite3
con = sqlite3.connect("paris_ve.db")
cur = con.cursor()
cur.execute("DROP TABLE paris_station_act")

cur.execute("CREATE TABLE IF NOT EXISTS paris_station_act (time varchar(70),adress text,district varchar(5),status varchar(255),post_code varchar(5),lat float,long float,id_pdc varchar(50))")

con.commit()
con.close()


