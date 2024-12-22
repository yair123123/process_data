import pytest
from pandas import DataFrame

from app.processing.load_data import load_csv
from app.processing.normalization import normalization


@pytest.fixture()
def load_data():
    return load_csv("data_test.csv")

def test_process_data(load_data):
    df_data:DataFrame = normalization(load_data)
    print(df_data.columns)
    assert len(df_data.columns) == 19