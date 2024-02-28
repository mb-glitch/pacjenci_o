# -*- coding: utf-8 -*-
#
import datetime, time
import pymssql


class ObslugaBazMS:
    ms = {
    'server':'192.168.2.110',
    'port':1433,
    'user':'ADMIN',
    'password':'***',
    'database':'lab3000',
    'charset':'cp1250'
    }

    def __init__(self):
        self.conn = pymssql.connect(**self.ms)

class ObslugaBazMSNEW:
    ms = {
    'server':'192.168.0.146',
    'port':1433,
    'user':'ADMIN',
    'password':'***',
    'database':'lab3000',
    'charset':'cp1250'
    }

    def __init__(self):
        self.conn = pymssql.connect(**self.ms)

class ObslugaBazBakter:
    bdl = {
    'server':'192.168.0.128',
    'port':1433,
    'user':'ADMIN',
    'password':'***',
    'database':'lab3000',
    'charset':'cp1250'
    }

    def __init__(self):
        self.conn = pymssql.connect(**self.bdl)



class ListaPacjentow:
    def __init__(self):
        pacjenci = []

    def pobierz_liste_do_zmiany2(self, ob_ms, ob_bakter):
        sql = 'select top 1 DATA_WPROWADZENIA from PACJENCI where ID_SZPITALA > 1 order by DATA_WPROWADZENIA desc'
        wczoraj = ''
        with ob_bakter.conn.cursor() as cursor:
            cursor.execute(sql)
            wczoraj = cursor.fetchone()[0]
        wczoraj = wczoraj.strftime("%Y-%m-%d %H:%M:%S")
        sql = """
select ID_PACJENTA from PACJENCI where 
DATA_WPROWADZENIA > CAST('{data1}' AS DateTime)
or 
DATA_MODYFIKACJI > CAST('{data2}' AS DateTime)
"""
        sql = sql.format(data1=wczoraj, data2=wczoraj)
        with ob_ms.conn.cursor() as cursor:
            cursor.execute(sql)
            self.pacjenci = cursor.fetchall()

    def pobierz_liste_do_zmiany(self, ob_ms, ob_bakter):
        sql = 'select top 1 DATA_WPROWADZENIA from PACJENCI where ID_SZPITALA > 1 order by DATA_WPROWADZENIA desc'
        wczoraj = ''
        with ob_bakter.conn.cursor() as cursor:
            cursor.execute(sql)
            wczoraj = cursor.fetchone()[0]
        wczoraj = wczoraj.strftime("%Y-%m-%d %H:%M:%S")
        sql = """
select ID_PACJENTA from zlecenie_badania where 
DATA_ZLECENIA > CAST('{data1}' AS DateTime)
"""
        sql = sql.format(data1=wczoraj, data2=wczoraj)
        with ob_ms.conn.cursor() as cursor:
            cursor.execute(sql)
            self.pacjenci = cursor.fetchall()


class Pacjent:
    PERSONEL_ID_SYSTEM = 8999
    def __init__(self):
        id_pacjenta_ms = ''
        id_pacjenta_bdl = ''
        imie = ''
        nazwisko = ''
        pesel = ''
        plec = ''
        data_urodzenia = ''
        panstwo = ''
        miasto = ''
        ulica = ''
        kod_pocztowy = ''
        numer_domu = ''
        numer_mieszkania = ''
        zmapowane = ''
    
    def zmapuj(self, ob):
        sql = """
select ID_PACJENTA from PACJENCI 
where ID_SZPITALA='{id_pacjenta}'
"""
        sql = sql.format(id_pacjenta=self.id_pacjenta_ms)
        with ob.conn.cursor() as cursor:
            cursor.execute(sql)
            rows = cursor.fetchall()
            if len(rows) > 0:
                self.id_pacjenta_bdl = rows[0][0]
                self.update_pacjenta(ob)
            else:
                self.dodaj_pacjenta(ob)


    def pobierz_dane_pacjenta(self, id_pacjenta, ob):
        sql = 'select * from PACJENCI where ID_PACJENTA={id_pacjenta}'.format(id_pacjenta=id_pacjenta[0])       
        with ob.conn.cursor(as_dict=True) as cursor:
            cursor.execute(sql)
            p = cursor.fetchone()
            self.id_pacjenta_ms=p['ID_PACJENTA']  # Wykorzystam pole ID_SZPITALA do zapamiętania id
            self.pesel=p['PESEL']
            self.nazwisko=p['NAZWISKO']
            self.imie=p['IMIE']
            self.data_urodzenia=p['DATA_URODZENIA']
            self.plec=p['PLEC']
            self.adres=p['MIEJSCE_ZAM']
            self.wprowadzajacy=self.PERSONEL_ID_SYSTEM
            self.created=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


    def dodaj_pacjenta(self, ob):
        with ob.conn.cursor() as cursor:
            sql = """
declare @nr_pacjenta int
declare @id_pacjenta int
select @nr_pacjenta = max(NR_PACJENTA) + 1 from PACJENCI
select @id_pacjenta = max(ID_PACJENTA) + 1 from PACJENCI

begin
insert into PACJENCI (PESEL, NR_DOWODU, NAZWISKO, IMIE, DATA_URODZENIA, PLEC, MIEJSCE_ZAM, TYP_UBEZPIECZENIA, NUMER_UBEZPIECZENIA, RUM, ID_INSTYTUCJI_UBEZP, ID_PLATNIKA, ID_PACJENTA, NR_PACJENTA, INICJALY_PAC, KOD_PAC, ID_ODDZ_REJ, NR_HCH, OS_WPROWADZAJACA, DATA_WPROWADZENIA, NIP, GRUPA_KRWI, RH, POPRZEDNIE_NAZWISKO, DATA_MODYFIKACJI, ID_OSOBY_MODYF, KOD_POCZTOWY, MIASTO, NR_DOMU, ULICA, ID_SZPITALA, EMAIL, PACJENT_BAD_SRODOW)
    values ('{pesel}', NULL, '{nazwisko}', '{imie}', {data_urodzenia}, '{plec}', '{adres}', NULL, NULL, NULL, NULL, NULL, @id_pacjenta, @nr_pacjenta, NULL, NULL, NULL, NULL, '{wprowadzajacy}', '{created}', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '{id_szpitala}', NULL, NULL)
end
"""
            data_urodzenia = self.data_urodzenia
            if data_urodzenia:
                data_urodzenia = data_urodzenia.strftime("%Y-%m-%d %H:%M:%S")
                data_urodzenia = "'{data}'".format(data=data_urodzenia)
            else:
                data_urodzenia = 'NULL'
            sql = sql.format(pesel=self.pesel,
                    nazwisko=self.nazwisko,
                    imie=self.imie,
                    data_urodzenia=data_urodzenia,
                    plec=self.plec,
                    adres=self.adres,
                    wprowadzajacy=self.PERSONEL_ID_SYSTEM,
                    created=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    id_szpitala=self.id_pacjenta_ms,
                    )
            cursor.execute(sql)
            ob.conn.commit()


    def update_pacjenta(self, ob):
        with ob.conn.cursor() as cursor:
            sql = """
update PACJENCI
set PESEL='{pesel}', NAZWISKO='{nazwisko}', IMIE='{imie}', DATA_URODZENIA={data_urodzenia}, PLEC='{plec}', MIEJSCE_ZAM='{adres}', OS_WPROWADZAJACA='{wprowadzajacy}', DATA_MODYFIKACJI='{created}'
WHERE ID_PACJENTA={id_pacjenta}
"""
            data_urodzenia = self.data_urodzenia
            if data_urodzenia:
                data_urodzenia = data_urodzenia.strftime("%Y-%m-%d %H:%M:%S")
                data_urodzenia = "'{data}'".format(data=data_urodzenia)
            else:
                data_urodzenia = 'NULL'
            sql = sql.format(pesel=self.pesel,
                    nazwisko=self.nazwisko,
                    imie=self.imie,
                    data_urodzenia=data_urodzenia,
                    plec=self.plec,
                    adres=self.adres,
                    wprowadzajacy=self.PERSONEL_ID_SYSTEM,
                    created=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    id_pacjenta=self.id_pacjenta_bdl,
                    )
            cursor.execute(sql)
            ob.conn.commit()






if __name__ == "__main__":
    ob_ms = ObslugaBazMS()
    ob_bakter = ObslugaBazBakter()
    lista = ListaPacjentow() 
    lista.pobierz_liste_do_zmiany(ob_ms, ob_bakter)
    for id_pacjenta in lista.pacjenci:
        pacjent = Pacjent()
        pacjent.pobierz_dane_pacjenta(id_pacjenta, ob_ms)
        pacjent.zmapuj(ob_bakter)








