# -*- coding: utf-8 -*-

import os
import time
from pathlib import Path
    




class InputFile(object):
    def __init__(self):
        self._input_file = {}
        self._observers = []


    @property
    def input_file(self):
        return self._input_file

    @input_file.setter
    def input_file(self, value):
        self._input_file = value
        for callback in self._observers:
            print('file change')
            callback(self._input_file)

    def bind_to(self, callback):
        print('bound')
        self._observers.append(callback)
    

class FileFinder:
    def __init__(self) -> None:
        self.starttime = time.time()
        self.data_path = Path(__file__).parent.parent / "Input"
        
    def run_search(self, search_interval=10) -> None:
        while True:
            time.sleep(search_interval - ((time.time() - self.starttime) % search_interval))
            filename, filetype = self.search_files()
            self.input_file = {filename:filetype}
            print(self.input_file)

    def search_files(self) -> list:
        try:
            print(f"Try:{self.data_path}")
            for filename in os.listdir(self.data_path):
                root, ext = os.path.splitext(filename)
                if root.startswith('auftrags_daten') and ext == '.csv':
                    filetype = "csv"
                    return filename, filetype
                elif root.startswith('auftrags_daten') and ext == '.json':
                    filetype = "json"
                    return filename, filetype
        except:
            return "", ""


class ExtractTransformLoad(object):
    def __init__(self, data):
        self.filename = "Test"
        self.filetype = "csv"
        self.data = data
        self.data.bind_to(self.update_filename)
        self.happiness = self.data.input_file
        print("test")

    def update_filename(self, input_file):
        self.happiness = input_file


if __name__ == '__main__':
    data = InputFile()
    p = ExtractTransformLoad(data)
    print(p.happiness)
    data.global_wealth = 1.0
    print(p.happiness)
