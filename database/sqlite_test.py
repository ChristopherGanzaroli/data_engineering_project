import sqlite3
con = sqlite3.connect("paris_ve_db.db")
cur = con.cursor()
cur.execute("CREATE TABLE IF NOT EXISTS paris_station_act (time varchar(70),adress text,district varchar(5),status varchar(255),post_code varchar(5),lat_long float,lat float,long float,id_pdc varchar(50))")

con.commit()
con.close()