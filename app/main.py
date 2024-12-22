from app.kafka_dir.flow import send_data
from app.processing.load_data import load_csv
from app.processing.normalization import normalization

if __name__ == '__main__':
    send_data(normalization(load_csv("big_data.csv")))