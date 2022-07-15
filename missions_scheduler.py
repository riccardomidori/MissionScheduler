import base64
import datetime

import pandas as pd
import requests as req
from mysql import connector
import jwt


def get_mysql_connection():
    username = "usrim021"
    hostname = "192.168.124.139"
    port = 6446
    password = "FCNusr21"
    db = "ned_sql"
    cnx = connector.connect(host=hostname, user=username, password=password, port=port, database=db)
    return cnx


def get_houses(connection):
    query = "SELECT tab.id, tab2.owner_id_login FROM tab_abitazione tab " \
            "INNER JOIN user_ned tab2 " \
            "ON tab.id = tab2.id_abitazione " \
            "WHERE tab.stato_attivazione_ned != 0"
    df = pd.read_sql_query(query, connection, index_col="id")
    return df


def check_token(token: [str, None],
                secret="acC3AKAHispvRm0OBIyBJ8ENkkHcMUFI",
                base_url="https://certmidori.fairconnect.it/Midori-EXT/Ned",
                username="midori"):
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


def generate_tag(token: [str, None],
                 house_id: int,
                 real_time_id: int,
                 base_url="https://certmidori.fairconnect.it/Midori-EXT/Ned"):
    token = check_token(token)
    tag_response = req.post(base_url + f"/generatag/{house_id}/{real_time_id}", headers={"Authorization": token})
    if tag_response.status_code == 200:
        print(tag_response.json())
    else:
        print(tag_response)
    return token


def main(base_url="https://certmidori.fairconnect.it/Midori-EXT/Ned",
         username="midori",
         password="acC3AKAHispvRm0OBIyBJ8ENkkHcMUFI"):
    connection = get_mysql_connection()
    df = get_houses(connection)
    r = req.post(url=base_url + "/login", data={"username": username, "password": password})
    if r.status_code == 200:
        response = r.json()
        token = response["message"]
        for house_id, owner_id in df["owner_id_login"].iteritems():
            o = str(owner_id)
            if house_id == 91:
                mission = req.post(base_url + f"/generamissione/{o}/{house_id}",
                                   headers={"Authorization": token})
                if mission.status_code == 200:
                    print(mission.json())
                else:
                    print(mission)


if __name__ == '__main__':
    token = generate_tag(None, 49, 493)
    print(token)
