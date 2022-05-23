from extract_transform_load import ExtractTransformLoad
import psycopg2

class DbSubscriber:
    def __init__(self, subscriber_name, integer_index, db_info: dict):
        self.subscriber_name = subscriber_name
        self.integer_index = integer_index
        self.db_info = db_info

    def update_database(self, input_file, etl):
        preprocessed_data = etl.run_preprocessing(input_file, self.integer_index)
        print(f"Preprocessed Data {preprocessed_data}")
        etl.upload_data(self.db_info, preprocessed_data)
        print('{} got updated with "{}"'.format(self.subscriber_name, list(input_file.keys())))
        
class DbPublisher:
    def __init__(self):
        self.subscribers = set()
        self.etl = ExtractTransformLoad()
    def register(self, who):
        self.subscribers.add(who)
    def unregister(self, who):
        self.subscribers.discard(who)
    def dispatch(self, input_file):
        for subscriber in self.subscribers:
            subscriber.update_database(input_file, self.etl)