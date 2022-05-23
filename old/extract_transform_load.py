from pathlib import Path
import psycopg2
import csv

class ExtractTransformLoad:
    def __init__(self) -> None:
        self.input_file = dict()
        self.data_path = Path(__file__).parent.parent / "Input"

    def run_preprocessing(self, input_file: dict, integer_index: list):
        for filename, filetype in input_file.items():
            if filetype == None:
                pass
            elif filetype == "csv":
                return self.preprocess_csv(filename, integer_index)
            elif filetype == "json":
                return self.preprocess_json(filename, integer_index)

    def preprocess_csv(self,filename, integer_index):
        file_path = Path(self.data_path) / filename
        values = []
        with open(file_path, newline='') as csvfile:
            reader = csv.reader(csvfile, delimiter=',')
            headers = next(reader, None)
            for row in reader:
                transformed = self.transform(row, integer_index)
                values.append(tuple(transformed))
        return values

    def preprocess_json(self):
        pass
    def transform(self, row, integer_index):
        for index in integer_index:
            row[index] = int(row[index])
        return row
    def upload_data(self, db_info, data_rows):
        conn = None
        try:
            conn = psycopg2.connect(user = db_info["user"],
                                    password = db_info["password"],
                                    host = db_info["host"],
                                    port = db_info["port"],
                                    database = db_info["dbname"])
            cur = conn.cursor()
            postgres_insert_query = """ INSERT INTO kunde ("KundenId", "Vorname", "Nachname", "Straße", "Wohnort") VALUES (%s,%s,%s,%s,%s)"""
            print(data_rows)
            for row in data_rows:
                print(row)
                cur.execute(postgres_insert_query, row)
            conn.commit()
            count = cur.rowcount
            print(count, "Record inserted successfully into mobile table")
        except (Exception, psycopg2.Error) as error:
            print("Error in update operation", error)

        finally:
            # closing database connection.
            if conn is not None:
                cur.close()
                conn.close()
                print("PostgreSQL connection is closed")
if __name__ == '__main__':
    etl = ExtractTransformLoad()
    etl.run_preprocessing({None:None})