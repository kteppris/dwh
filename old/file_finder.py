import os
import time
from pathlib import Path
from observer1 import DbPublisher, DbSubscriber

class FileFinder:
    """Searches in the distance of search_interval for files in the Input folder. 
    """
    def __init__(self, publisher: object) -> None:
        self.starttime = time.time()
        self.data_path = Path(__file__).parent.parent / "Input"
        self.input_file = {}
        self.filetype = str
        self.filename = str
        self.publisher = publisher
    
    def run_search(self, search_interval=10) -> None:
        """Main while loop that runs the search every search_interval seconds and update the input_file variable
           of the object with the new filename and filetype as a dictionary.

        Args:
            search_interval (int, optional): Intervall time between the searches. Defaults to 10.
        """
        while True:
            self.search_files()
            self.publisher.dispatch(self.input_file)
            # 
            self.sleep_interval(search_interval)
            

    def search_files(self):
        """Searching for files in data_path und calles the set_input_file function to update filename and filetype
        """
        for filename in os.listdir(self.data_path):
            # split filname in name and extension
            root, ext = os.path.splitext(filename)
            # call set_input_file function to set depending on the filetype
            if root.startswith('application') and ext == '.csv':
                self.set_input_file(filename,"csv")
            elif root.startswith('application') and ext == '.json':
                self.set_input_file(filename,"json")
            else:
                # if there are files with other names, extensions, call set function with default None
                self.set_input_file()
        if not os.listdir(self.data_path):
            # if there are no files in the folder, call set function without arguments(default is None)
            self.set_input_file()

    
    def set_input_file(self, filename=None, filetype=None):
        """Set the object input_file variable to the filename and filetype.

        Args:
            filename (str, optional): The filename with filetypeextension. Default None.
            filetype (str, optional): The filetype of the file. Default None.
        """
        self.input_file = {filename:filetype}

    def sleep_interval(self, search_interval: int):
        """Based on the starttime and the actual time, calculates the time to sleep until next run
           and uses the sleep function for that time.   

        Args:
            search_interval (int): Search interval in seconds
        """
        time.sleep(search_interval - ((time.time() - self.starttime) % search_interval))

if __name__ == '__main__':
    finder = FileFinder()
    finder.run_search()