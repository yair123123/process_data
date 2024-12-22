import os
import pandas as pd

def load_csv(filename,encode="latin1"):
    file_path = os.path.join(os.path.dirname(__file__),'..','..','data', filename)
    return pd.read_csv(file_path,encoding=encode)
