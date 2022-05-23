from asyncio.log import logger
from pathlib import Path
import csv
from file_observer import FileObserver
import time
import json
from datetime import datetime
import coloredlogs, logging
from sqlalchemy import create_engine
from crud import create_produkt, create_auftrag, create_kunde, recreate_database
from sqlalchemy.orm import sessionmaker
from config import *
from models import *
from observer import DbPublisher, DbTable
from tqdm import tqdm


class ExtractTransformLoad:
    def __init__(self) -> None:
        self.starttime = time.time()
        self.uploaded_files = []
        self.data_path = Path(__file__).parent.parent / "Input"
        self.file_observer = FileObserver()
        self.init_logger()
        self.session = self.init_session()
        self.pub = DbPublisher()
        self.init_tables()


    def init_logger(self):
        coloredlogs.install()
        logging.basicConfig(format = '%(levelname)s:%(message)s', level = logging.INFO)

    def init_session(self) -> object:
        """Preparing the session for the Datebase connection.

        Returns:
            object: Session object
        """
        logger.info("Initialize session...")
        self.engine = create_engine(DATABASE_URI)
        Session = sessionmaker(bind = self.engine)
        return Session()
        
    def init_tables(self) -> None:
        """Register all tables of the tables variable at the Publisher. Tables are global.
        """
        for table in tables:
            self.pub.register(table)
            logger.info("Table registered: %s", table)

    def start_observing(self, search_interval = 10, debug = False) -> None:
        """Main loop that calls all other functions.

        Args:
            search_interval (int, optional): Length between to search processes. Defaults to 10.
            debug (bool, optional): When true all tables get dropped and recreated. Defaults to False.
        """
        if debug:
            self.reset_db()
        while True:
            input_files = self.file_observer.get_files()
            for file in input_files:
                if file in self.uploaded_files:
                    logger.info('File already uploaded, skipping: %s.', file['filename'])
                    continue
                data = self.load_data(file)
                self.upload_data(file['table_name'], data)
                self.update_uploaded_files(file)
            self.sleep_interval(search_interval)
            

    def reset_db(self):
        """Drop all tables that are in the models.py and recreates them.
        """
        logger.info("Debug mode on, resetting Database...")
        self.session.close_all()
        recreate_database(self.engine)

    def load_data(self, file: dict):
        """Loads the file date depending on the filetype

        Args:
            file (dict): {filename: '', filetype: '', tablename: ''}

        Returns:
            dict: Dict of off the data, where the column headers are the keys. (json the keys are the keys)
        """
        filetype = file['filetype']
        filename = file['filename']
        file_path = Path(self.data_path) / filename
        logging.info('Loading data of: %s', filename)
        if filetype == "csv":
            file_data = self.load_csv(file_path)
        elif filetype == "json":
            file_data = self.load_json(file_path)
        else:
            logging.warning('Unknown filetype!')
        logging.info('Preprocessing finished.')
        return file_data
            
    
    def load_csv(self, file_path):
        """Load data from csv file and also strips any space, as well as lowercases the keys.

        Args:
            file_path (str): full path of the file

        Returns:
            dict: Data of the file, where the column headers are the keys.
        """
        data = []
        with open(file_path, newline = '', encoding = 'utf-8-sig') as csvfile:
            reader2 = csv.DictReader(csvfile, delimiter = ',')
            for row in reader2:
                row = { k.lower().strip():v.strip() for k, v in row.items()}
                data.append(row)
        return data

    def load_json(self, file_path: str):
        """Load the data of the json file to a dict. 

        Args:
            file_path (str): Full path of the json file.

        Returns:
            dict: Json data converted to a dict, keys lowercase and data is stripped. Date column is converted in datetime obj
        """
        with open(file_path, 'r') as f:
            data = json.load(f)
            out_data = []
            for id, order in data.items():
                id_num = id.strip("kauf")
                out_order = { k.lower():v.strip() for k, v in order.items()}
                out_order['datum'] = datetime.strptime(order['datum'], '%d.%m.%Y %H:%M')
                out_order['auftrags_id'] = id_num
                out_data.append(out_order)
        return out_data


    
    def upload_data(self, tablename: str, data: list):
        """Sends the publisher the session table_name and data, so that subscribers get informed.

        Args:
            tablename (str): Name of the table where the data should go
            data (list): list of dictioniers, one for each entry of the file.
        """
        logger.info("Following entries have been uploaded to %s:", tablename)
        self.pub.dispatch(
            session = self.session, 
            table_name = tablename, 
            data = data
        )
    
    def sleep_interval(self, search_interval: int):
        """Based on the starttime and the actual time, calculates the time to sleep until next run
            and uses the sleep function for that time.   

        Args:
            search_interval (int): Search interval in seconds
        """
        sleep_interval = search_interval - ((time.time() - self.starttime) % search_interval)
        pbar = tqdm(total = 100, desc = "Time until next search.")
        for i in range(10):
            time.sleep(sleep_interval/10)
            pbar.update(10)
        pbar.close() 


    def update_uploaded_files(self, file: dict) -> None:
        """Saves the uploaded files in a dictionary, to track them and skip in the next search.

        Args:
            file (dict): Fileinformation of a file that has been uploaded.
        """
        if file not in self.uploaded_files:
            self.uploaded_files.append(file)

            
if __name__ == '__main__':
    kunde_table = DbTable(
        table_name = 'kunde', 
        model = Kunde, 
        create_function = create_kunde
    )
    produkt_table = DbTable(
        table_name = 'produkt', 
        model = Produkt, 
        create_function = create_produkt
    )
    auftrag_table = DbTable(
        table_name = 'auftrag', 
        model = Auftrag, 
        create_function = create_auftrag
    )
    tables = kunde_table, produkt_table, auftrag_table
    etl = ExtractTransformLoad()
    etl.start_observing(debug = True)