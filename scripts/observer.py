### observer.py ###
from sqlalchemy import create_engine
 
from sqlalchemy.orm import sessionmaker
from config import *
from models import *
from crud import create_kunde, create_auftrag, create_produkt, query_entries, recreate_database

# from extract_transform_load import ExtractTransformLoad
import psycopg2


class DbTable:
    def __init__(self, table_name, model, create_function):
        self.table_name = table_name
        self.model = model
        self.create = create_function

    def __repr__(self):
        return "<DbTable(tablename={}, model='{}', create_function='{}')>"\
                .format(self.table_name, self.model.__name__, self.create.__name__)

    def update_database(self, session, data):
        for entry in data:
            self.create(session, **entry)
        query_entries(session, self.model)


        
class DbPublisher:
    def __init__(self):
        self.subscribers = set()
    def register(self, table):
        self.subscribers.add(table)
    def unregister(self, table):
        self.subscribers.discard(table)
    def dispatch(self, session, table_name, data):
        for subscriber in self.subscribers:
            if subscriber.table_name == table_name:
                subscriber.update_database(session, data)


if __name__ == '__main__':
    engine = create_engine(DATABASE_URI)

    Session = sessionmaker(bind=engine)
    s = Session()
    s.close_all()
    recreate_database()
    s = Session()


    pub = DbPublisher()

    kunde = DbTable(
        table_name='kunde',
        model= Kunde,
        create_function = create_kunde
    )

    pub.register(kunde)

    pub.dispatch(
        session = s, 
        table_name='kunde', 
        data = [{
            'kunden_id' : 1005,
            'wohnort' : 'Testort',
            'straße' : 'Teststraße',
            'nachname' : 'Testname',
            'vorname' : 'Testname'
        },
        {
            'kunden_id' : 1010,
            'wohnort' : 'Testort2',
            'straße' : 'Teststraße2',
            'nachname' : 'Testname2',
            'vorname' : 'Testname2'
        }
        ]
    )

    print(Kunde)