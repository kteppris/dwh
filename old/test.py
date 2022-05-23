import crud

from sqlalchemy import create_engine
 
from sqlalchemy.orm import sessionmaker
from config import *
from models import *
from crud import create_kunde

# from extract_transform_load import ExtractTransformLoad
import psycopg2

engine = create_engine(DATABASE_URI)

Session = sessionmaker(bind=engine)

s = Session()

class DbSubscriber:
    def __init__(self, table_name, model):
        self.table_name = table_name
        self.model = model

#    def update_database(self, input_file, etl):

        
class DbPublisher:
    def __init__(self):
        self.subscribers = dict()
    def register(self, who, callback=None):
        if callback == None:
            callback = getattr(who, 'update')
        self.subscribers[who] = callback
    def unregister(self, who):
        del self.subscribers[who]
    def dispatch(self, session, table_name, data):
        for subscriber, callback in self.subscribers.items():
            if subscriber.table_name == table_name:
                callback(session, **data)

pub = DbPublisher()

kunde = DbSubscriber(
    table_name='kunde',
    model= Kunde
)

pub.register(kunde, create_kunde)

pub.dispatch(
    session = s, 
    table_name='kunde', 
    data = {
        'kunden_id' : 1004,
        'wohnort' : 'Testort',
        'straße' : 'Teststraße',
        'nachname' : 'Testname',
        'vorname' : 'Testname'
    }
)




# getattr(crud, 'create_kunde')(
#         s = s,
#         kunden_id = 1004,
#         wohnort = 'Testort',
#         straße = 'Teststraße',
#         nachname = 'Testname',
#         vorname = 'Testname'
# )
# getattr(crud, 'Kunde')



# for data in yaml.load_all(open('books.yaml')):
#     book = Book(**data)
#     s.add(book)


# s.query(Kunde).all()