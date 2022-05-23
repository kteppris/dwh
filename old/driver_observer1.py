from observer1 import DbPublisher, DbSubscriber
from file_finder import FileFinder
from extract_transform_load import ExtractTransformLoad

pub = DbPublisher()

db1_db_info = {
    "host" : "localhost",
    "port" : "5432",
    "dbname" : "DataScience",
    "user" : "postgres",
    "password" : "dolomit900"
}
db1 = DbSubscriber('Database1', [0], db1_db_info)
#db2 = Subscriber('Database2')

pub.register(db1)
#pub.register(db2)

file_finder = FileFinder(pub)
file_finder.run_search()


# pub.dispatch("File1")

# pub.dispatch("File2")