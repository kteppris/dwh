import os
from pathlib import Path
import coloredlogs, logging

class FileObserver:
    """Searches in the time interval of search_interval for files in the Input folder and saves the filename, filetype
        and tablename in a list of list in the object atribute input_files. 
        For instance: [{filename: 'file1', filetype: 'json', tablename:'kunden'}, 
                        {filename: 'file2', filetype: 'csv', tablename:'produkt'}]
    """
    def __init__(self) -> None:
        self.data_path = Path(__file__).parent.parent / "Input"
        self.input_files = []
        coloredlogs.install()
        logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

    
    def get_files(self) -> list:
        """calls the search function and returns the 

        Returns:
            list: the table the content of the file should go, or none    
        """
        # delete files in list von previous searches
        self.input_files.clear()
        self.search_files()
        self.input_files = sorted(self.input_files, key=lambda d: d['table_name'], reverse=True)
        return self.input_files
            

    def search_files(self):
        """Searching for files in data_path und calles the set_input_file function to update filename and filetype
        """
        logging.info('Start file searching process..')
        for filename in os.listdir(self.data_path):
            logging.info('Adding file to list: %s', filename)
            tablename = self.get_table_name(filename)
            if not tablename:
                # skip the file if tablename is None
                logging.warning('Could not identify talbe name, skipping file: %s.', filename)
                continue
            # split filname in name and extension
            root, ext = os.path.splitext(filename)
            # call add_input_file function to set depending on the filetype
            if root.startswith('application') and ext == '.csv':
                self.add_input_file(filename,"csv", tablename)
            elif root.startswith('application') and ext == '.json':
                self.add_input_file(filename,"json", tablename)
            else:
                logging.warning('Filename or type incorrect, skipping file: %s.', filename)
        if not os.listdir(self.data_path):
            # Info that there are no files in the folder
            logging.info("The input folder is empty.")

    def add_input_file(self, filename: str, filetype: str, table_name: str):
        """Set the object input_file variable to the filename filetype and table_name as a list in a list.

        Args:
            filename (str): The filename with filetypeextension. 
            filetype (str): The filetype of the file.
            filetype (str): The table_name where the data is spossed to go.
        """
        if filename:
            self.input_files.append({"filename": filename,
                                     "filetype": filetype, 
                                     "table_name": table_name})

    def get_table_name(self, filename):
        """Checking for strings in the filename and returns the table the content sould go

        Args:
            filename (str): filename with filetype extension

        Returns:
            str: the table the content of the file should go, or none
        """
        if "produkt" in filename:
            return "produkt"
        elif "kunde" in filename:
            return "kunde"
        elif "application2" in filename:
            return "auftrag"
        else:
            return None





if __name__ == '__main__':
    observer = FileObserver()
    input_files = observer.get_files()
    print(input_files)
    for file in input_files:
        print(f'Filename: {file["filename"]} | Filetype {file["filetype"]} | Tablename: {file["table_name"]}')