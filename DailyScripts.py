import datetime
import requests as req
import pandas as pd
import concurrent.futures
from argparse import ArgumentParser
from mysql import connector
import jwt
import logging


secret = "acC3AKAHispvRm0OBIyBJ8ENkkHcMUFI"
username = "midori"
base_url = "https://certmidori.fairconnect.it/Midori-EXT/Ned"





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


def get_mysql_connection():
    username = "usrim021"
    hostname = "192.168.124.139"
    port = 6446
    password = "FCNusr21"
    db = "ned_sql"
    cnx = connector.connect(host=hostname, user=username, password=password, port=port, database=db)
    return cnx


def send_push(url, token):
    url = base_url + f"/{url}"
    token = check_token(token)
    headers = {"Authorization": token}

    response = req.post(url, headers=headers)
    if response.status_code == 200:
        print(response.json())
    else:
        print(response.text)


def run_parallel(arguments, df_houses, token):
    executor = concurrent.futures.ProcessPoolExecutor()
    futures = []
    if arguments["op"] == "discovery":
        futures = [
            executor.submit(send_push,
                            url=f"nuovi-consumi/{int(col['id'])}",
                            token=token)
            for row, col in df_houses.iterrows()
        ]

    elif arguments["op"] == "header":
        futures = [
            executor.submit(send_push,
                            url=f"energy-coaching/{int(col['id'])}",
                            token=token)
            for row, col in df_houses.iterrows()
        ]
    elif arguments["op"] == "score":
        futures = [
            executor.submit(send_push,
                            url=f"punteggio/{int(col['id'])}",
                            token=token)
            for row, col in df_houses.iterrows()
        ]
    elif arguments["op"] == "alert":
        futures = [
            executor.submit(send_push,
                            url=f"alert/{int(col['id'])}",
                            token=token)
            for row, col in df_houses.iterrows()
        ]

    if len(futures) > 0:
        concurrent.futures.wait(futures)
        result = [f.result() for f in futures]


def run(arguments, df_houses, token):
    logging.basicConfig(filename=f"logger_{arguments['op']}.txt",
                        filemode='a',
                        format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                        datefmt='%H:%M:%S',
                        level=logging.DEBUG)
    if arguments["op"] == "discovery":
        for row, col in df_houses.iterrows():
            print(col)
            send_push(url=f"nuovi-consumi/{int(col['id'])}",
                      token=token)
    elif arguments["op"] == "header":
        for row, col in df_houses.iterrows():
            send_push(url=f"energy-coaching/{int(col['id'])}",
                      token=token)
    elif arguments["op"] == "score":
        for row, col in df_houses.iterrows():
            send_push(url=f"punteggio/{int(col['id'])}",
                      token=token)
    elif arguments["op"] == "alert":
        for row, col in df_houses.iterrows():
            send_push(url=f"alert/{int(col['id'])}",
                      token=token)


def main(arguments):
    connection = get_mysql_connection()
    query_houses = "SELECT ta.id, ta.tipo_ned, un.owner_id_login FROM tab_abitazione ta " \
                   "INNER JOIN user_ned un ON ta.id=un.id_abitazione " \
                   "WHERE ta.stato_attivazione_ned = 1"
    df_houses = pd.read_sql_query(query_houses, connection)
    connection.close()
    token = check_token(None)
    futures = []
    timestamp = int(datetime.datetime.now().timestamp())

    run(arguments, df_houses, token)


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("--op", "--operation")
    args = vars(parser.parse_args())
    print(args)
    main(args)
