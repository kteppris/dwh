### crud.py ###

from sqlalchemy import create_engine
 
from sqlalchemy.orm import sessionmaker
from config import *
from models import *

# engine = create_engine(DATABASE_URI)

# Session = sessionmaker(bind=engine)

# s = Session()

def recreate_database(engine):
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


def create_kunde(s, kunden_id, wohnort, straße, nachname, vorname):

    kunde = Kunde(
        kunden_id = kunden_id,
        wohnort = wohnort,
        straße = straße,
        nachname = nachname,
        vorname = vorname
    )
    s.add(kunde)
    s.commit()
    print(kunde)

def create_produkt(s, id, name, verkaufspreis, kaufpreis, modell, hersteller):

    produkt = Produkt(
        id = id,
        name = name,
        verkaufspreis = verkaufspreis,
        kaufpreis = kaufpreis,
        modell = modell,
        hersteller = hersteller  
    )
    print(produkt)
    s.add(produkt)
    s.commit()

def create_auftrag(s, auftrags_id, datum, kunden_id, produkt_id):

    auftrag = Auftrag(
        auftrags_id = auftrags_id,
        datum = datum,
        kunden_id = kunden_id,
        produkt_id = produkt_id   
    )
    s.add(auftrag)
    s.commit()
    print(auftrag)


def query_entries(s, model):
    s.query(model).all()

if __name__ == '__main__':
    engine = create_engine(DATABASE_URI)

    Session = sessionmaker(bind=engine)

    s = Session()
    s.close_all()
    recreate_database()
    s = Session()

    # kunde = Kunde(
    #     kunden_id = 1004,
    #     wohnort = 'Testort',
    #     straße = 'Teststraße',
    #     nachname = 'Testname',
    #     vorname = 'Testname'
    # )
    # produkt = Produkt(
    #     id = 10,
    #     name = 'Testprodukt',
    #     verkaufspreis = 10.10,
    #     kaufpreis = 15.10,
    #     model = 'Testmodel',
    #     hersteller = 'Testhersteller'
    # )
    # auftrag = Auftrag(
    #     auftrags_id = 10,
    #     datum = datetime.now().date(),
    #     kunden_id = 1004,
    #     produkt_id = 10
    # )
    create_kunde(
        s = s,
        kunden_id = 1004,
        wohnort = 'Testort',
        straße = 'Teststraße',
        nachname = 'Testname',
        vorname = 'Testname'
    )
    query_entries(s, Kunde)
    create_produkt(
        s = s,
        id = 10,
        name = 'Testprodukt',
        verkaufspreis = 10.10,
        kaufpreis = 15.10,
        modell = 'Testmodel',
        hersteller = 'Testhersteller'
    )
    query_entries(s, Produkt)
    create_auftrag(
        s = s,
        auftrags_id = 10,
        datum = datetime.now().date(),
        kunden_id = 1004,
        produkt_id = 10
    )
    query_entries(s, Auftrag)



    s.close()

