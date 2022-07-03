import cx_Oracle
from datetime import date
import datetime

import time
start_time = time.time()


def calculate_agegroup(birthdate):
    today = date.today()
    age = today.year - birthdate.year - ((today.month, today.day) < (birthdate.month, birthdate.day))
    if age < 10 :
        return "Kind"
    elif age < 18:
        return "Jugendlicher"
    elif age < 25:
        return "Junger Erwachsener"
    elif age < 50:
        return "Erwachsener"
    else:
        return "Alter Erwachsener"


# make connection
db_connection_string = 'DWT_GRUPPE3/a54bxVDR@coco.informatik.tu-cottbus.de:1521/dbis'
con = cx_Oracle.connect(db_connection_string)
cursor = con.cursor()
print("establishing connection")
# load data from Filiale 2
filiale2_kauft = con.cursor()
filiale2_kunde = con.cursor()
filiale2_ort = con.cursor()
# create array for orte
filiale2_produkt = con.cursor()
filiale2_kauft.execute("Select * from DWT_FILIALE_2.KAUFT")
filiale2_kunde.execute("Select * from DWT_FILIALE_2.KUNDE")
filiale2_ort.execute("Select * from DWT_FILIALE_2.ORT")
filiale2_produkt.execute("Select * from DWT_FILIALE_2.PRODUKT")
filiale2_orte = []
for row in filiale2_ort:
    filiale2_orte.append((row[0], row[1], row[2]))
print("Retrieved Data from Filiale 2")
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
print("Retrieved Data from Filiale 3")
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
print("Retrieved Data from Filiale 4")

# normalize & write data for Filiale
# Filiale is not retrieved from the db itself so hardcode this
filialen = con.cursor()
filialen.execute("Select * from FILIALE")
filial_data = [
    (str(2), "West", "West"),
    (str(3), "Nord", "Nord"),
    (str(4), "Nord", "Nord"),
]
# delete all rows already in the filialen
for row in filialen:
    if row in filial_data:
        filial_data.remove(row)
# define db action
cursor.executemany("insert into FILIALE(FILIAL_ID, FILIAL_NAME, REGION) values (:1, :2, :3)", filial_data)
# run the action
con.commit()
print("wrote filial-data")
# normalize & load data for Kunde
kunden = con.cursor()
kunden.execute("Select * from KUNDE")
kunden_data = []

# import kunden from filiale 2
for row in filiale2_kunde:
    k_id = str(row[0]) + "_f2"
    vorname = row[2]
    nachname = row[1]
    # not defined
    geschlecht = 0
    dates = str(row[4]).split("-")
    dates[2] = str(dates[2]).split(" ")[0]
    geburtsdatum = datetime.date(int(dates[0]), int(dates[1]), int(dates[2]))
    altersgruppe = calculate_agegroup(row[4])
    Straße_NR = filiale2_orte[row[3]-1][1]
    Plz_Ort = filiale2_orte[row[3]-1][2]
    Straße = Straße_NR.rsplit(" ", 1)[0]
    Nummer = Straße_NR.rsplit(" ", 1)[1]
    PLZ = Plz_Ort.rsplit(" ", 1)[0]
    Ort = Plz_Ort.rsplit(" ", 1)[1]
    kunde = (str(k_id), nachname, vorname,Straße,Nummer,PLZ,Ort, geschlecht, geburtsdatum, altersgruppe)
    kunden_data.append(kunde)
print("normalised and unified kunden data from filiale 2")
# import kunden from filiale 3
for row in filiale3_kunde:
    k_id = str(row[0]) + "_f3"
    vorname = row[2]
    nachname = row[1]
    # not defined
    geschlecht = row[7]
    dates = str(row[8]).split("-")
    dates[2] = str(dates[2]).split(" ")[0]
    geburtsdatum = datetime.date(int(dates[0]), int(dates[1]), int(dates[2]))
    altersgruppe = calculate_agegroup(row[8])
    kunde = (str(k_id), nachname, vorname,Straße,Nummer,PLZ,Ort, geschlecht, geburtsdatum, altersgruppe)
    Straße = row[3]
    Nummer = row[4]
    PLZ = row[5]
    Ort = row[6]
    if ((str(row[0]) + "_f2"), nachname, vorname,Straße,Nummer,PLZ,Ort, geschlecht, geburtsdatum, altersgruppe) in kunden_data:
        row = ((str(row[0]) + "_f2_f3"), nachname, vorname,Straße,Nummer,PLZ,Ort, geschlecht, geburtsdatum, altersgruppe)
    else:
        kunden_data.append(kunde)
print("normalised and unified kunden data from filiale 3")

# import kunden from filiale 4
for row in filiale4_kunde:
    k_id = str(row[0]) + "_f4"
    vorname = row[2]
    nachname = row[1]
    # not defined
    if row[6] == "w":
        geschlecht = 1
    else:
        geschlecht = 0
    dates = str(row[7]).split("-")
    dates[2] = str(dates[2]).split(" ")[0]
    geburtsdatum = datetime.date(int(dates[0]), int(dates[1]), int(dates[2]))
    Straße_NR = row[3]
    Straße = Straße_NR.rsplit(" ", 1)[0]
    Nummer = Straße_NR.rsplit(" ", 1)[1]
    PLZ = str(row[4])
    Ort = str(row[5])
    altersgruppe = calculate_agegroup(row[7])
    kunde = (str(k_id), nachname, vorname,Straße,Nummer,PLZ,Ort, geschlecht, geburtsdatum, altersgruppe)
    if ((str(row[0]) + "_f2"), nachname, vorname,Straße,Nummer,PLZ,Ort, geschlecht, geburtsdatum, altersgruppe) in kunden_data:
        row = ((str(row[0]) + "_f2_f4"), nachname, vorname,Straße,Nummer,PLZ,Ort, geschlecht, geburtsdatum, altersgruppe)
    elif ((str(row[0]) + "_f3"), nachname, vorname,Straße,Nummer,PLZ,Ort, geschlecht, geburtsdatum, altersgruppe) in kunden_data:
        row = ((str(row[0]) + "_f3_f4"), nachname, vorname,Straße,Nummer,PLZ,Ort, geschlecht, geburtsdatum, altersgruppe)
    elif ((str(row[0]) + "_f2_f3"), nachname, vorname,Straße,Nummer,PLZ,Ort, geschlecht, geburtsdatum, altersgruppe) in kunden_data:
        row = ((str(row[0]) + "f2_f3_f4"), nachname, vorname,Straße,Nummer,PLZ,Ort, geschlecht, geburtsdatum, altersgruppe)
    else:
        kunden_data.append(kunde)
print("normalised and unified kunden data from filiale 4")
# delete existing entries from kunden
existing_kunden = con.cursor()
existing_kunden.execute("Select * from KUNDE")
for row in existing_kunden:
    if row in kunden_data:
        kunden_data.remove(row)
kunden_data = list(set(kunden_data))
# define db action
print(kunden_data[234])
print(kunden_data[0])
print(kunden_data[1350])
cursor.executemany("insert into KUNDE(KUNDEN_ID, NACHNAME, VORNAME, STRAßE, STRAßENNUMMER, PLZ, ORT, GESCHLECHT, GEBURTSDATUM, ALTERSGRUPPE) values (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10)", kunden_data)
# run the action
con.commit()
print("Wrote Kunden Data")
# normalize & load data for Artikel
produkt_data = []
for row in filiale2_produkt:
    Artikel_ID = str(row[0])
    Artikel_Name = row[1]
    Produktkategorie = row[2]
    produkt = (Artikel_ID, Artikel_Name, Produktkategorie)
    produkt_data.append(produkt)

for row in filiale3_produkt:
    Artikel_ID = str(row[0])
    Artikel_Name = row[1]
    if row[2] == 1:
        Produktkategorie = "Alkoholfrei"
    else:
        Produktkategorie = "Alkoholisch"
    if Artikel_ID not in produkt_data:
        produkt = (Artikel_ID, Artikel_Name, Produktkategorie)
        produkt_data.append(produkt)

for row in filiale4_produkt:
    Artikel_ID = str(row[0])
    Artikel_Name = row[1]
    if row[2] == 1:
        Produktkategorie = "Alkoholfrei"
    else:
        Produktkategorie = "Alkoholisch"
    if Artikel_ID not in produkt_data:
        produkt = (Artikel_ID, Artikel_Name, Produktkategorie)
        produkt_data.append(produkt)

produkt_data = list(set(produkt_data))
# delete existing entries from kunden
existing_produkt = con.cursor()
existing_produkt.execute("Select * from ARTIKEL")
for row in existing_produkt:
    if row in produkt_data:
        produkt_data.remove(row)
# define db action
cursor.executemany("insert into ARTIKEL(ARTIKEL_ID, ARTIKEL_NAME, PRODUKTKATEGORIE) values (:1, :2, :3)", produkt_data)
# run the action
con.commit()
print("Wrote Produkt Data")

# normalize & load data for Tag
tag_data = []

for row in filiale2_kauft:
    date_string = row[3]
    dates = str(row[3]).split("-")
    dates[2] = str(dates[2]).split(" ")[0]
    tag = int(dates[2])
    monat = int(dates[1])
    jahr = int(dates[0])
    date = datetime.date(jahr, monat, tag)
    Zeit_ID = str(tag) + str(monat) + str(jahr)
    tage = ['Montag','Dienstag','Mittwoch', 'Donnerstag', 'Freitag', 'Samstag', 'Sonntag']
    Tag_Str = tage[date.weekday()]
    KW = date.isocalendar()[1]
    month2quarter = {
        1: 1, 2: 1, 3: 1,
        4: 2, 5: 2, 6: 2,
        7: 3, 8: 3, 9: 3,
        10: 4, 11: 4, 12: 4,
    }.get
    Quartal = month2quarter(monat)
    datum = (Zeit_ID, Tag_Str, KW, monat, Quartal, jahr)
    tag_data.append(datum)

for row in filiale3_bestellung:

    date_string = row[1]
    dates = str(row[1]).split("-")
    dates[2] = str(dates[2]).split(" ")[0]
    tag = int(dates[2])
    monat = int(dates[1])
    jahr = int(dates[0])
    date = datetime.date(jahr, monat, tag)
    Zeit_ID = str(tag) + str(monat) + str(jahr)
    if Zeit_ID not in tag_data:
        tage = ['Montag','Dienstag','Mittwoch', 'Donnerstag', 'Freitag', 'Samstag', 'Sonntag']
        Tag_Str = tage[date.weekday()]
        KW = date.isocalendar()[1]
        month2quarter = {
            1: 1, 2: 1, 3: 1,
            4: 2, 5: 2, 6: 2,
            7: 3, 8: 3, 9: 3,
            10: 4, 11: 4, 12: 4,
        }.get
        Quartal = month2quarter(monat)
        datum = (Zeit_ID, Tag_Str, KW, monat, Quartal, jahr)
        tag_data.append(datum)

for row in filiale4_bestellung:

    date_string = row[2]
    dates = str(row[2]).split("-")
    dates[2] = str(dates[2]).split(" ")[0]
    tag = int(dates[2])
    monat = int(dates[1])
    jahr = int(dates[0])
    date = datetime.date(jahr, monat, tag)
    Zeit_ID = str(tag) + str(monat) + str(jahr)
    if Zeit_ID not in tag_data:
        tage = ['Montag', 'Dienstag', 'Mittwoch', 'Donnerstag', 'Freitag', 'Samstag', 'Sonntag']
        Tag_Str = tage[date.weekday()]
        KW = date.isocalendar()[1]
        month2quarter = {
            1: 1, 2: 1, 3: 1,
            4: 2, 5: 2, 6: 2,
            7: 3, 8: 3, 9: 3,
            10: 4, 11: 4, 12: 4,
        }.get
        Quartal = month2quarter(monat)
        datum = (Zeit_ID, Tag_Str, KW, monat, Quartal, jahr)
        tag_data.append(datum)

tag_data = list(set(tag_data))

existing_tag = con.cursor()
existing_tag.execute("Select * from ARTIKEL")
for row in existing_tag:
    if row in tag_data:
        tag_data.remove(row)
cursor.executemany("insert into TAG(ZEIT_ID, TAG_STR, WOCHE, MONAT, QUARTAL, JAHR) values (:1, :2, :3, :4, :5, :6)", tag_data)
# run the action
con.commit()
print("Wrote Date Data")

# normalize & load data for Verkauf
verkauf_data = []
#
# for row in filiale2_kauft:
#    ZEIT_ID =
#    ARTIKEL_ID =
#    FILIAL_ID =
#    KUNDEN_ID =
#    MENGE =
#    UMSATZ =
#
#    Kauf = (ZEIT_ID, ARTIKEL_ID, FILIAL_ID, KUNDEN_ID, MENGE, UMSATZ)
#    verkauf_data.append()

#for row in filiale3_bestellung:
#for row in filiale4_bestellung:
#



print("Fertig")
print("Laufzeit:--- %s seconds ---" % (time.time() - start_time))

con.close()
