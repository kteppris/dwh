from asyncio.log import logger
from pathlib import Path
import psycopg2
import csv
from file_finder_2 import FileFinder
import time
import json
from datetime import datetime
import coloredlogs, logging

db_info = {
    "host" : "localhost",
    "port" : "5432",
    "dbname" : "DataScience",
    "user" : "postgres",
    "password" : "dolomit900"
}

table_integers = {
    "kunde" : [0],
    "produkt" : [0,2,3],
    "auftrag" : [0,2,3]
}

class ExtractTransformLoad:
    def __init__(self, db_info: dict, table_integers: dict) -> None:
        self.starttime = time.time()
        self.uploaded_files = []
        self.data_path = Path(__file__).parent.parent / "Input"
        self.file_finder = FileFinder()
        self.db_info = db_info
        self.table_integers = table_integers
        coloredlogs.install()
        logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)

    def start_search(self, search_interval=10):
        while True:
            input_files = self.file_finder.get_files()
            for file in input_files:
                if file in self.uploaded_files:
                    logger.info('File already uploaded, skipping: %s.', file['filename'])
                    continue
                value_list = self.run_preprocessing(file)
                insert_query = self.get_query(file['table_name'])
                self.upload_data(insert_query, value_list, file['table_name'])
                self.update_uploaded_files(file)
            self.sleep_interval(search_interval)


    def run_preprocessing(self, file: dict):
        filetype = file['filetype']
        filename = file['filename']
        table_name = file['table_name']
        integer_index = self.table_integers[table_name]
        file_path = Path(self.data_path) / filename
        logging.info('Start preprocessing of: %s', filename)
        if filetype == "csv":
            values_list = self.preprocess_csv(filename, file_path, integer_index)
        elif filetype == "json":
            values_list = self.preprocess_json(filename, file_path,  integer_index)
        else:
            logging.warning('Unknown filetype!')
        logging.info('Preprocessing finished.')
        return values_list
            
    
    def preprocess_csv(self,filename, file_path, integer_index):
        values = []
        with open(file_path, newline='', encoding='utf-8-sig') as csvfile:
            reader = csv.reader(csvfile, delimiter=',')
            reader2 = csv.DictReader(csvfile, delimiter=',')
            for row in reader2:
                # print(row)
                pass
            for row in reader:
                row = [entry.strip() for entry in row]
                transformed = self.transform(row, integer_index)
                values.append(tuple(transformed))
        return values

    def preprocess_json(self, filename, file_path, integer_index):
        with open(file_path, 'r') as f:
            data = json.load(f)
            order_list = []
            for id, order in data.items():
                id = id.strip("kauf")
                order_data = []
                order_data.append(id)
                date = datetime.strptime(order['datum'], '%d.%m.%Y %H:%M')
                order['datum'] = date.strftime('%m-%d-%y')
                for value in order.values():
                    order_data.append(value)
                order_list.append(order_data)
        print(order_list)
        return order_list


    def transform(self, row, integer_index):
        try:
            for index in integer_index:
                row[index] = int(row[index])
        except:
            logging.warning('Could not transform data to integer.')
        return row
    
    def upload_data(self, postgres_insert_query, data_rows, tablename):
        conn = None
        try:
            conn = psycopg2.connect(user = self.db_info["user"],
                                    password = self.db_info["password"],
                                    host = self.db_info["host"],
                                    port = self.db_info["port"],
                                    database = self.db_info["dbname"])
            cur = conn.cursor()
            for row in data_rows:
                cur.execute(postgres_insert_query, row)
            conn.commit()
            count = cur.rowcount
            logging.info(f"{count} record have been inserted successfully into {tablename} table")
        except (Exception, psycopg2.Error) as error:
            logging.warning("Error in update operation %s", error)
            # logging.info("Moving file to archive folder")

        finally:
            # closing database connection.
            if conn is not None:
                cur.close()
                conn.close()
                logging.info("PostgreSQL connection is closed")
    
    def sleep_interval(self, search_interval: int):
        """Based on the starttime and the actual time, calculates the time to sleep until next run
            and uses the sleep function for that time.   

        Args:
            search_interval (int): Search interval in seconds
        """
        time.sleep(search_interval - ((time.time() - self.starttime) % search_interval))

    def get_query(self, tablename):
        postgres_insert_query = None
        if tablename == "kunde":
            postgres_insert_query = f""" INSERT INTO kunde ("KundenId", "Vorname", "Nachname", "Straße", "Wohnort") VALUES (%s,%s,%s,%s,%s)"""
        if tablename == "produkt":
            postgres_insert_query = f""" INSERT INTO produkt (id, name, verkaufspreis, kaufpreis, modell, hersteller) VALUES (%s,%s,%s,%s,%s,%s)"""
        if tablename == "auftrag":
            postgres_insert_query = f""" INSERT INTO auftraege ("auftrags_ID", "datum", "Kunde_ID", "Produkt_ID") VALUES (%s,%s,%s,%s)"""
        return postgres_insert_query

    def update_uploaded_files(self, file) -> None:
       if file not in self.uploaded_files:
           self.uploaded_files.append(file) 
            
if __name__ == '__main__':
    etl = ExtractTransformLoad(db_info, table_integers)
    etl.start_search()