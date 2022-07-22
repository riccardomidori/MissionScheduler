import calendar
import datetime
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from mysql import connector
import pymongo
import configparser
import requests as req
import joblib
import pgeocode
import jwt

TYPE_BUILDING = "Type Building"
FAMILY_COMPONENTS = "Family Components"
LOCATION_NCS = "LocationNCS"
LOCATION = "Location"
CHILDREN = "Children"
ADULTS = "Adults"
DEVICES = "Devices"
POWER = "ContractPower"
RESIDENT = "Resident"
SIZE = "Size"
RENEWABLE = "Renewable"
ELDERS = "num_anziani"

mapper_device_name = {
    1: "lavastoviglie",
    2: "Asciugatrice",
    3: "Frigorifero",
    4: "Stufetta/termo elettrico",
    6: "Forno elettrico",
    8: "Lavatrice",
    10: "Standby",
    11: "Ferro da stiro",
    15: "Piano cottura",
    19: "Condizionatore",
    21: "Boiler",
    22: "Fornetto",
    93: "Intrattenimento"
}

base_url = "https://certmidori.fairconnect.it/Midori-EXT/Ned"
secret = "acC3AKAHispvRm0OBIyBJ8ENkkHcMUFI"
username = "midori"


def check_token(token: [str, None]):
    if token is None or token == "":
        response = req.post(url=base_url + "/login", data={"username": username, "password": secret})
        if response.status_code == 200:
            new_token = response.json()
            return new_token["message"]

    if "Bearer" in token:
        token = token.replace("Bearer ", "")
    try:
        decoded = jwt.decode(token, secret, algorithms=["HS256"], options={"verify_signature": False})
        if decoded["exp"] < datetime.datetime.now().timestamp() + 3 * 60:
            response = req.post(url=base_url + "/login", data={"username": username, "password": secret})
            if response.status_code == 200:
                new_token = response.json()
                return new_token["message"]
    except:
        response = req.post(url=base_url + "/login", data={"username": username, "password": secret})
        if response.status_code == 200:
            new_token = response.json()
            return new_token["message"]

    if not token.startswith("Bearer "):
        return "Bearer " + token
    return token


def check_device(house_id, token):
    response = req.post(base_url + f"/nuovi-consumi/{house_id}", headers={"Authorization": token})
    if response.status_code == 200:
        print(response.json())


def from_id_to_location(s):
    nomi = pgeocode.Nominatim("it")
    return nomi.query_postal_code(s)["latitude"], nomi.query_postal_code(s)["longitude"]


def score():
    pass


def get_mysql_connection():
    username = "usrim021"
    hostname = "192.168.124.139"
    port = 6446
    password = "FCNusr21"
    db = "ned_sql"
    cnx = connector.connect(host=hostname, user=username, password=password, port=port, database=db)
    return cnx


def get_trend_index(x, starts, ends, wait_threshold, value_threshold, day_threshold, pad=0):
    anomalous_obj = []
    anomalous_index = []
    return_series = pd.Series(index=x.index, dtype="int32").fillna(0)
    for j, w in zip(starts, ends):
        if w > j + datetime.timedelta(days=1):
            s = datetime.datetime.strftime(j.date(), "%Y-%m-%d")
            e = datetime.datetime.strftime(w.date(), "%Y-%m-%d")
            v = round(float((x.loc[j:w, 'MA'].median() -
                             x.loc[j:w, 'MA2'].median()) /
                            x.loc[j:w, 'MA2'].median()) * 100, 2)
            if np.absolute(v) > 0:
                if (w - j).days > day_threshold:
                    anomalous_index.append([j - datetime.timedelta(days=pad), w])
                    return_series.loc[j - datetime.timedelta(days=pad):w] = 1

    return anomalous_obj, anomalous_index, return_series


def get_trend(ts: pd.DataFrame, target_label='Active', t1=3,
              t2=30, k=1,
              wait_threshold=5,
              value_threshold=0.05,
              day_threshold=3,
              method="span",
              ignore_na=True,
              show=False, only_above=False, only_below=False):
    if type(ts.index[0]) == datetime.date:
        ts.index = pd.DatetimeIndex(ts.index)
    x = ts.copy(deep=True)

    x_mean = x[target_label].mean()
    x_std = x[target_label].std()

    anomalies_above = x[x[target_label] > x_mean + 2 * x_std]
    anomalies_below = x[x[target_label] < x_mean - 2 * x_std]
    x.loc[anomalies_above.index, target_label] = None
    x.loc[anomalies_below.index, target_label] = None

    m0 = t1 * k
    m1 = t2 * k

    if method == "halflife":
        x['MA'] = x[target_label].ewm(halflife=m0, ignore_na=ignore_na).mean()
        x['MA2'] = x[target_label].ewm(halflife=m1, ignore_na=ignore_na).mean()
    elif method == "alpha":
        x['MA'] = x[target_label].ewm(alpha=m0, ignore_na=ignore_na).mean()
        x['MA2'] = x[target_label].ewm(alpha=m1, ignore_na=ignore_na).mean()
    elif method == "com":
        x['MA'] = x[target_label].ewm(com=m0, ignore_na=ignore_na).mean()
        x['MA2'] = x[target_label].ewm(com=m1, ignore_na=ignore_na).mean()
    elif method == "span" or method is None:
        x['MA'] = x[target_label].ewm(span=m0, ignore_na=ignore_na).mean()
        x['MA2'] = x[target_label].ewm(span=m1, ignore_na=ignore_na).mean()
    elif method == "rolling" or method is None:
        x['MA'] = x[target_label].rolling(m0).mean()
        x['MA2'] = x[target_label].rolling(m1).mean()
    else:
        print("Method must be one of: span, halflife, alpha, com, rolling")
        return ValueError

    above = x['MA'] > (x['MA2'] + value_threshold * x["MA2"])
    below = x['MA'] < (x['MA2'] - value_threshold * x["MA2"])
    starts_above = ts.index[above & ~ above.shift(1).fillna(False)]
    ends_above = ts.index[above & ~ above.shift(-1).fillna(False)]
    starts_below = ts.index[below & ~ below.shift(1).fillna(False)]
    ends_below = ts.index[below & ~ below.shift(-1).fillna(False)]

    above_obj, above_index, above_series = get_trend_index(x, starts_above, ends_above,
                                                           wait_threshold=wait_threshold,
                                                           value_threshold=0,
                                                           day_threshold=day_threshold)
    below_obj, below_index, below_series = get_trend_index(x, starts_below, ends_below,
                                                           wait_threshold=wait_threshold,
                                                           value_threshold=0,
                                                           day_threshold=day_threshold)
    if show:
        if only_above:
            indices = [above_index]
            labels = ["Increasing"]
            colors = [[204 / 255, 0 / 255, 0 / 255, 1]]
        elif only_below:
            indices = [below_index]
            labels = ["Decreasing"]
            colors = [[130 / 255, 238 / 255, 98 / 255, 1]]
        else:
            indices = [above_index, below_index]
            labels = ["Increasing", "Decreasing"]
            colors = [[204 / 255, 0 / 255, 0 / 255, 1], [130 / 255, 238 / 255, 98 / 255, 1]]
        # plot_activities(x[[target_label, "MA", "MA2"]].fillna(0),
        #                 indices=indices,
        #                 labels=labels,
        #                 colors=colors,
        #                 main_line=target_label)
        # plt.show()
    return above_index, below_index, above_series, below_series, x


def get_mongodb_connection():
    config = configparser.ConfigParser()
    config.read('config.ini')
    client = pymongo.MongoClient(
        f'mongodb://{config["DEFAULT"]["user"]}:{config["DEFAULT"]["password"]}@{config["DEFAULT"]["server"]}'
        f'/?authSource=ned'
    )
    cnx = client.ned
    return cnx


def get_data(db_connection,
             house_id: [int, str],
             db: str = "mysql",
             s: [int, str] = None,
             e: [int, str] = None,
             ts_type: str = "seconds"
             ) -> [pd.DataFrame, None]:
    if ts_type == "seconds":
        if s is not None:
            start = s
        else:
            start = datetime.datetime(year=2021, month=1, day=1).timestamp()
        if e is not None:
            end = e
        else:
            end = datetime.datetime.now().timestamp()
    elif ts_type == "datetime":
        if s is not None:
            start = datetime.datetime.strptime(s, "%Y-%m-%d")
        else:
            start = datetime.datetime(year=2021, month=1, day=1)
        if e is not None:
            end = datetime.datetime.strptime(e, "%Y-%m-%d")
        else:
            end = datetime.datetime.now()
    else:
        if s is not None:
            start = s
        else:
            start = datetime.datetime(year=2021, month=1, day=1).strftime("%Y-%m-%d %H:%M:%S")
        if e is not None:
            end = e
        else:
            end = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if db == "mysql_ned_input":
        columns = ["timestamp", "v0", "f0", "i1", "pf1", "p1", "q1", "s1", "i2", "ch1", "ch2"]
        query = "SELECT * FROM ned_data_%d WHERE t>=%s AND t<=%s".replace("%d", str(house_id))
        cursor = db_connection.cursor()
        cursor.execute(query % (start, end))
        df = pd.DataFrame(list(cursor), columns=columns).astype(float)
        df = df.set_index("timestamp", drop=True)
        df.index = pd.to_datetime(df.index, unit="s")
        return df
    elif db == "mysql_fifteen_input":
        columns = ["pod", "timestamp", "p", "q"]
        query = "SELECT * FROM tab_hf_input WHERE codice_fornitura=%s AND timestamp>=%s AND timestamp<=%s"
        cursor = db_connection.cursor()
        cursor.execute(query % (house_id, start, end))
        df = pd.DataFrame(list(cursor), columns=columns)
        df = df.set_index("timestamp", drop=True)
        df.index = pd.to_datetime(df.index, unit="s")
        return df
    elif db == "mysql_ned_daily":
        columns = ["id", "house_id", "timestamp", "Total", "Standby"]
        query = "SELECT * FROM tab_rt_dailyresults WHERE id_abitazione=%s AND date>=%s AND date<=%s"
        cursor = db_connection.cursor()
        cursor.execute(query % (house_id, start, end))
        df = pd.DataFrame(list(cursor), columns=columns)
        df = df.set_index("timestamp", drop=True)
        df.index = pd.to_datetime(df.index, unit="s").date
        df = df.asfreq("D").interpolate()
        return df
    elif db == "mongodb_daily":
        query = {
            "building_id": house_id,
            "date": {
                "$gte": start,
                "$lte": end
            }
        }
        df = []
        cursor = db_connection["dailyResults"].find(query)
        for c in cursor:
            obj = {
                "timestamp": c["date"],
                "Total": c["aggregateEnergy"],
                "Standby": c["standby"],
                "Entertainment": 0,
                "Washingmachine": 0,
                "Oven": 0,
                "Diswhasher": 0,
                "Fridge": 0
            }
            for a in c["appliances"]:
                device_name = a["device_name"]
                if device_name == "Baseline" or device_name == "ElectronicDevice":
                    device_name = "Entertainment"
                if device_name in obj.keys():
                    obj[device_name] += a["totalenergy"]
            df.append(obj)
        df = pd.DataFrame(df)
        df = df.set_index("timestamp")
        df.index = pd.to_datetime(df.index, unit="s").date
        df = df[~df.index.duplicated(keep="first")]
        return df
    else:
        return None


def energetic_alerts(connection, house_id, df: pd.DataFrame, db: str):
    mapper_devices = {
        "Total": {
            "value": 0.02,
            "t1": 5,
            "t2": 20,
            "n": 0,
            "change": []
        },
        "Standby": {
            "value": 0.2,
            "t1": 5,
            "t2": 30,
            "n": 0,
            "change": []
        },
        "Night": {
            "value": 0.02,
            "t1": 1,
            "t2": 10,
            "n": 0,
            "change": []
        },
        "Fridge": {
            "value": 0.03,
            "t1": 10,
            "t2": 20,
            "n": 0,
            "change": []
        },
        "Entertainment": {
            "value": 0.05,
            "t1": 5,
            "t2": 20,
            "n": 0,
            "change": []
        },
        "Washing": {
            "value": 0.05,
            "t1": 5,
            "t2": 30,
            "n": 0,
            "change": []
        },
        "Washingmachine": {
            "value": 0.05,
            "t1": 5,
            "t2": 7,
            "n": 0,
            "change": []
        },
        "Oven": {
            "value": 0.05,
            "t1": 2,
            "t2": 7,
            "n": 0,
            "change": []
        },
        "Dishwasher": {
            "value": 0.05,
            "t1": 5,
            "t2": 30,
            "n": 0,
            "change": []
        }
    }
    current_trends = []
    if db == "mongodb_daily":
        query = ""
        pass
    else:
        query_insert = "INSERT INTO tab_trend (id_abitazione, start_time, end_time, " \
                       "target, id_dispositivo, id_activity, 'change', ongoing) " \
                       "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
        query_select = "SELECT * FROM tab_trend WHERE id_abitazione=%s AND ongoing=1"

    for k, v in mapper_devices.items():
        if k in df.columns:
            above_index, below_index, above_series, below_series, x = get_trend(df,
                                                                                target_label=k,
                                                                                method="halflife",
                                                                                day_threshold=3,
                                                                                t1=v["t1"],
                                                                                t2=v["t2"],
                                                                                value_threshold=v["value"],
                                                                                ignore_na=False,
                                                                                show=True,
                                                                                only_above=True
                                                                                )
            if len(above_index) > 0 and k != "Total":
                last_trend = above_index[-1]
                start_trend = last_trend[0]
                end_trend = last_trend[1]
                today = datetime.datetime.today()
                if (today - end_trend).days < 2:
                    change = (x.loc[start_trend:end_trend, "MA"].median() - x.loc[start_trend:end_trend, "MA2"].median()
                              ) / x.loc[start_trend:end_trend, "MA2"]
                    current_trends.append({
                        "type": k,
                        "start_time": start_trend,
                        "change": change * 100
                    })
                    params = {

                    }
                    send_push(connection, house_id, params, "midori_alert_" + k)
    return current_trends


def send_push(connection,
              house_id,
              params,
              intent,
              base_url="https://fairconnect-services.conversation-inc.ovh/api/v1/trigger",
              auth="Basic ZmFpcmNvbm5lY3Q6dHBZS0cyYm1iOXBMWjI5NHc2V0duOEViZVIzSjlEUDNocHNyZnJNUFF1",
              company="TESTMIDORI"):
    query_user = "SELECT ul.owner_id, ul.user_id FROM user_login ul INNER JOIN " \
                 "user_ned un on ul.owner_id = un.owner_id_login " \
                 "WHERE un.id_abitazione=%s"
    cursor = connection.cursor()
    cursor.execute(query_user % house_id)
    df_user = pd.DataFrame(list(cursor), columns=["owner", "user_id"])
    if len(df_user) > 0:
        if house_id == 11:
            user_id = "74e267fdd74c73b368d3a48052f32902040c96158f0e7b7801fc332e83c21050"
        else:
            user_id = df_user["user_id"].values[0]
        owner = df_user["owner"].values[0]
        params.update({"owner_id": owner})
        params.update({"house_id": house_id})
        params.update({"id_dispositivo": 1})
        data = [{
            "users": [{
                "user_id": user_id,
                "lang": "it"
            }],
            "intent": intent,
            "company": company,
            "params": params
        }]
        r = req.post(base_url, json=data, headers={"Authorization": auth})
        print(r.json())


def send_push2():
    base_url = "https://fairconnect-services.conversation-inc.ovh/api/v1/trigger"
    data = [{"company": "TESTMIDORI",
             "params": {"owner_id": "00393407515435",
                        "house_id": 49,
                        "id_dispositivo": 1,
                        "nome_elettrodomestico_articolo": "il forno",
                        "ora_utilizzo": "16:20",
                        "list_tag": [
                            {'code': 'P_15', 'label': 'Piano cottura elettrico'},
                            {'code': 'P_22', 'label': 'Fornetto'}],
                        "start_time": "16:20"},
             "intent": "missione_TAG_forno_6",
             "users": [{"user_id": "26908a1839e7a091d05006fcf9d95069460f9cae0020c43a3b02f885ddcd6229", "lang": "it"}]}]
    auth = "Basic ZmFpcmNvbm5lY3Q6dHBZS0cyYm1iOXBMWjI5NHc2V0duOEViZVIzSjlEUDNocHNyZnJNUFF1"
    r = req.post(base_url, json=data, headers={"Authorization": auth})
    print(r.json())


def fifteen_analysis(df: pd.DataFrame):
    pass


def main():
    connection = get_mysql_connection()
    query = "SELECT * FROM tab_abitazione WHERE " \
            "(tipo_ned>0 AND stato_attivazione_ned=1) OR (tipo_ned=0 AND stato_attivazione_ned>0)"
    df_house = pd.read_sql_query(query, connection, index_col="id")
    token = check_token(None)
    ts = 1658151061
    for house_id, col in df_house.iterrows():
        print(house_id)
        check_device(house_id, token)


if __name__ == '__main__':
    main()
