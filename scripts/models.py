from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, ForeignKey
from datetime import datetime

Base = declarative_base()

class Kunde(Base):
    __tablename__ = 'kunde'
    kunden_id = Column(Integer, primary_key=True)
    wohnort = Column(String)
    straße = Column(String)
    nachname = Column(String)
    vorname = Column(String)
    
    def __repr__(self):
        return "<Kunde(kunden_id={}, wohnort='{}', straße='{}', nachname='{}', vorname='{}')>"\
                .format(self.kunden_id, self.wohnort, self.straße, self.nachname, self.vorname)

class Produkt(Base):
    __tablename__ = 'produkt'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    verkaufspreis = Column(Integer)
    kaufpreis = Column(Integer)
    modell = Column(String)
    hersteller = Column(String)
    
    def __repr__(self):
        return "<Produkt(id={}, name='{}', verkaufspreis={}, kaufpreis={}, modell='{}', hersteller='{}')>"\
                .format(self.id, self.name, self.verkaufspreis, self.kaufpreis, self.modell, self.hersteller)

class Auftrag(Base):
    __tablename__ = 'auftraege'
    auftrags_id = Column(Integer, primary_key=True)
    datum = Column(Date)
    kunden_id = Column(Integer, ForeignKey('kunde.kunden_id'))
    produkt_id = Column(Integer, ForeignKey('produkt.id'))
    
    def __repr__(self):
        return "<Auftrag(auftrags_id={}, datum='{}', kunden_id={}, produkt_id={})>"\
                .format(self.auftrags_id, self.datum, self.kunden_id, self.produkt_id)


if __name__ == '__main__':
    kunde = Kunde(
        kunden_id = 1004,
        wohnort = 'Testort',
        straße = 'Teststraße',
        nachname = 'Testname',
        vorname = 'Testname'
    )
    produkt = Produkt(
        id = 10,
        name = 'Testprodukt',
        verkaufspreis = 10.10,
        kaufpreis = 15.10,
        modell = 'Testmodel',
        hersteller = 'Testhersteller'
    )
    auftrag = Auftrag(
        auftrags_id = 10,
        datum = datetime.now().date(),
        kunden_id = 1004,
        produkt_id = 10
    )
    print(kunde)
    print(produkt)
    print(auftrag)