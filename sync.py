import cx_Oracle

# make connection
db_connection_string = 'DWT_GRUPPE3/a54bxVDR@coco.informatik.tu-cottbus.de:1521/dbis'
con = cx_Oracle.connect(db_connection_string)
cursor = con.cursor()

# load data from Filiale 2
filiale2_kauft = con.cursor()
filiale2_kunde = con.cursor()
filiale2_ort = con.cursor()
filiale2_produkt = con.cursor()
filiale2_kauft.execute("Select * from DWT_FILIALE_2.KAUFT")
filiale2_kunde.execute("Select * from DWT_FILIALE_2.KUNDE")
filiale2_ort.execute("Select * from DWT_FILIALE_2.ORT")
filiale2_produkt.execute("Select * from DWT_FILIALE_2.PRODUKT")
# load data from Filiale 3
filiale3_bestellung = con.cursor()
filiale3_bottle = con.cursor()
filiale3_kunde = con.cursor()
filiale3_produkt = con.cursor()
filiale_sort = con.cursor()
filiale3_bestellung.execute("Select * from DWT_FILIALE_3.BESTELLUNG")
filiale3_bottle.execute("Select * from DWT_FILIALE_3.BOTTLE")
filiale3_kunde.execute("Select * from DWT_FILIALE_3.KUNDE")
filiale3_produkt.execute("Select * from DWT_FILIALE_3.PRODUKT")
filiale_sort.execute("Select * from DWT_FILIALE_3.SORT")
# load data from Filiale 4
filiale4_bestellung = con.cursor()
filiale4_kunde = con.cursor()
filiale4_posten = con.cursor()
filiale4_produkt = con.cursor()
filiale4_sorte = con.cursor()
filiale4_bestellung.execute("Select * from DWT_FILIALE_4.BESTELLUNG")
filiale4_kunde.execute("Select * from DWT_FILIALE_4.KUNDE")
filiale4_posten.execute("Select * from DWT_FILIALE_4.POSTEN")
filiale4_produkt.execute("Select * from DWT_FILIALE_4.PRODUKT")
filiale4_sorte.execute("Select * from DWT_FILIALE_4.SORTE")

# normalize & write data for Filiale
# Filiale is not retrieved from the db itself so hardcode this
filialen = con.cursor()
filialen.execute("Select * from FILIALE")
filial_data = [
    (2, "West", "West"),
    (3, "Nord", "Nord"),
    (4, "Nord", "Nord"),
]
# delete all rows already in the filialen
for row in filialen:
    if row in filial_data:
        filial_data.remove(row)
# define db action
cursor.executemany("insert into FILIALE(FILIAL_ID, FILIAL_NAME, REGION) values (:1, :2, :3)", filial_data)
# run the action
con.commit()

# normalize & load data for Kunde
kunden = con.cursor()
kunden.execute("Select * from KUNDEN")
kunden_data = []
# import kunden from filiale 2
for row in filiale2_kunde:







# normalize & load data for Artikel

# normalize & load data for Tag

# normalize & load data for Verkauf

print("Database version:", con.version)
con.close()
