# -*- coding: utf-8 -*-
#
import os
import datetime, time
import pymssql
import logging
import logging.config

settings = {
    # czas pomiędzy sprawdzeniem zleceń w katalogu
    'TIMEOUT': '5',

    # format czasu do konwerowania pomiędzy systemami
    'FORMAT_CZASU_BAZA': "%Y-%m-%d %H:%M:%S",
    'CDA_FORMAT_CZASU': '%Y%m%d%H%M%S',


    # id automatycznego systemu inf w bazie med-star
    'PERSONEL_ID_SYSTEM': '8999',

    # lokalizacja katalogów ze zleceniami i wynikami, rozszeżenia
    'KATALOG_NOWYCH_ZLECEN': 'media/zlecenia_nowe',
    'KATALOG_ARCHIWUM_ZLECEN': 'media/zlecenia_archiwum',
    'KATALOG_BLEDNYCH_ZLECEN': 'media/zlecenia_bledne',
    'ROZSZERZENIE_ZLECENIE': '.ord',
    'ROZSZERZENIE_WYNIK': '.res',

    # logowanie
    'POZIOM_LOGOWANIA': 'DEBUG',
    'POZIOM_LOGOWANIA_KONSOLA': 'DEBUG',
    'POZIOM_LOGOWANIA_PLIK': 'DEBUG',
    'PLIK_LOGOWANIA': 'log/pacjenci.log',
    'LOGGER_NAZWA': 'pacjenci',
}


# wrzucam setting do zmiennych środowiskowych
for k, v in settings.items():    
    os.environ[k] = v

# konfiguracja logowania
logging.config.dictConfig({
    'version': 1,
    'formatters': {
        'default': {'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'}
    },
    'handlers': {
        'console': {
            'level': os.getenv('POZIOM_LOGOWANIA_KONSOLA'),
            'class': 'logging.StreamHandler',
            'formatter': 'default',
            'stream': 'ext://sys.stdout'
        },
        'file': {
            'level': os.getenv('POZIOM_LOGOWANIA_PLIK'),
            'class': 'logging.handlers.RotatingFileHandler',
            'formatter': 'default',
            'filename': os.getenv('PLIK_LOGOWANIA'),
            'maxBytes': 1024*1024,
            'backupCount': 10
        }
    },
    'loggers': {
        os.getenv('LOGGER_NAZWA'): {
            'level': os.getenv('POZIOM_LOGOWANIA'),
            'handlers': ['console', 'file']
        }
    },
    'disable_existing_loggers': False
})





logger = logging.getLogger('pacjenci')


class ObslugaBazBakter:
    bdl = {
    #'server':'192.168.2.110',
    'server':'192.168.0.128',
    'port':1433,
    'user':'ADMIN',
    'password':'***',
    #'database':'punktp',
    'database':'lab3000',
    'charset':'cp1250'
    }
    count = 0

    def __init__(self):
        self.conn = pymssql.connect(**self.bdl)



class ListaPacjentow:
    def __init__(self):
        self.pacjenci = []
        self.zlecenia = []
        self.pacjenci_all = []


    def pobierz_liste_pacjentow(self, ob_bakter):
        sql = "select distinct IMIE, NAZWISKO, PESEL from PACJENCI where PESEL != '            '"
        with ob_bakter.conn.cursor(as_dict=True) as cursor:
            cursor.execute(sql)
            self.pacjenci = cursor.fetchall()

    def pobierz_liste_wszystkich_pacjentow(self, ob_bakter):
        sql = "select ID_PACJENTA from PACJENCI"
        with ob_bakter.conn.cursor() as cursor:
            cursor.execute(sql)
            pacjenci_all = cursor.fetchall()
            for p in pacjenci_all:
                self.pacjenci_all.append(p[0])

    def pobierz_liste_zlecen(self, ob_bakter):
        sql = 'select distinct ID_PACJENTA from zlecenie_badania'
        with ob_bakter.conn.cursor() as cursor:
            cursor.execute(sql)
            zlec = cursor.fetchall()
            for z in zlec:
                self.zlecenia.append(z[0])

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
        self.pacjent_glowny = ''
        self.pacjenci = []
    
    def zmapuj(self, ob):
        sql = """
select ID_PACJENTA, MIEJSCE_ZAM, DATA_WPROWADZENIA, DATA_MODYFIKACJI from PACJENCI 
where IMIE='{imie}'
and
NAZWISKO='{nazwisko}'
and
PESEL='{pesel}'
ORDER BY DATA_MODYFIKACJI desc, DATA_WPROWADZENIA desc, MIEJSCE_ZAM desc
"""
        sql = sql.format(imie=self.imie, nazwisko=self.nazwisko, pesel=self.pesel)
        with ob.conn.cursor(as_dict=True) as cursor:
            cursor.execute(sql)
            rows = cursor.fetchall()
            if len(rows)>1:
                self.pacjent_glowny = rows.pop(0)['ID_PACJENTA']
                logger.debug("Pacjent główny: {}".format(self.pacjent_glowny))
                for row in rows:
                    logger.debug("Pacjent   inny: {}".format(row))
                    self.pacjenci.append(row['ID_PACJENTA'])
                self.update_pacjenta(ob)

    def pobierz_dane_pacjenta(self, pacjent_z_listy):
        self.pesel=pacjent_z_listy['PESEL']
        self.nazwisko=pacjent_z_listy['NAZWISKO']
        self.imie=pacjent_z_listy['IMIE']

    def update_pacjenta(self, ob):
        with ob.conn.cursor() as cursor:
            sql = "update zlecenie_badania "\
                  "set ID_PACJENTA={id_pacjenta_glownego} "\
                  "WHERE ID_PACJENTA in ({id_pacjentow})"
            id_pacjentow = ', '.join(map(str, self.pacjenci))
            sql = sql.format(id_pacjenta_glownego=self.pacjent_glowny,
                    id_pacjentow=id_pacjentow)
            logger.debug('{}'.format(sql))
            cursor.execute(sql)
            sql2 = 'delete from PACJENCI where ID_PACJENTA in ({id_pacjentow})'
            sql2 = sql2.format(id_pacjentow=id_pacjentow)
            logger.debug('Usuwam pacjentów numer: {}'.format(id_pacjentow))
            logger.debug('{}'.format(sql2))
            cursor.execute(sql2)
            ob.conn.commit()

if __name__ == "__main__":
    ob_bakter = ObslugaBazBakter()
    lista = ListaPacjentow() 
    lista.pobierz_liste_pacjentow(ob_bakter)
    lista.pobierz_liste_wszystkich_pacjentow(ob_bakter)
    lista.pobierz_liste_zlecen(ob_bakter)
    i = 1
    a = len(lista.pacjenci)
    for p in lista.pacjenci:
        logger.debug('{i}/{a}'.format(i=i, a=a))
        pacjent = Pacjent()
        pacjent.pobierz_dane_pacjenta(p)
        pacjent.zmapuj(ob_bakter)
        i += 1
    logger.debug(len(lista.pacjenci_all))
    logger.debug(len(lista.zlecenia))







