from typing import List, Dict

import pandas as pd
from toolz import pipe
from toolz.curried import partial


def column_filtering(list_columns: List[str], pd_data: pd.DataFrame) -> pd.DataFrame:
    return pd_data[list_columns]


def replace_invalid_values(val):
    if pd.isna(val) or val in ['unknown', "Unknown", "NaN"]:
        return None
    return val


def rename_columns(rename: Dict[str, str], pd_data: pd.DataFrame) -> pd.DataFrame:
    return pd_data.rename(columns=rename)


def delete_unknown_and_none(df):
    return df.applymap(replace_invalid_values)


def delete_none(pd_data: pd.DataFrame) -> pd.DataFrame:
    return pd_data.dropna(how='all')


def sum_kills_and_spread(pd_data: pd.DataFrame) -> pd.DataFrame:
    pd_data["num_spread"] = pd_data["nwound"] - pd_data["nwoundte"]
    pd_data["num_killed"] = pd_data["nkill"] - pd_data["nkillter"]
    pd_data.drop(columns=["nkill", "nkillter", "nwound", "nwoundte"], inplace=True)
    return pd_data

def convert_unknown_to_none(val):
    if isinstance(val, str) and val in ["unknown", "Unknown", "none", "None"]:
        return None
    elif isinstance(val, float) and pd.isna(val):  # טיפול ב-NaN
        return None
    return val
def normalization(pd_data: pd.DataFrame) -> pd.DataFrame:
    return pipe(
        pd_data,
        partial(column_filtering,
                ["eventid", "iyear", "imonth", "iday", "country_txt", "city", "latitude", "longitude", "region_txt",
                 "targtype1_txt", "target1", "natlty1_txt", "gname", "gname2", "attacktype1_txt", "nperps", "nwound",
                 "nwoundte", "nkill", "nkillter", "summary"]),
        sum_kills_and_spread,
        partial(rename_columns, {"iyear": "year", "imonth": "month", "iday": "day", "region_txt": "region",
                                 "targtype1_txt": "target_type", "country_txt": "country",
                                 "attacktype1": "attacked_type",
                                 "gname": "group_name", "gname2": "group_name2", "nperps": "num_terrorists",
                                 "natlty1_txt": "target_nationality"}),
        lambda df: df.applymap(convert_unknown_to_none),
        delete_none,
        delete_unknown_and_none,
    )
