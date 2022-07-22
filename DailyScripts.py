import datetime
import json
import traceback

import requests as req
import pandas as pd
from DailyAnalysis import get_mysql_connection, base_url, check_token
import concurrent.futures
from argparse import ArgumentParser


def send_push(url, token):
    url = base_url + f"/{url}"
    token = check_token(token)
    headers = {"Authorization": token}

    response = req.get(url, headers=headers)
    if response.status_code == 200:
        print(response.json())
    else:
        print(response.text)


def main(arguments):
    executor = concurrent.futures.ProcessPoolExecutor()

    connection = get_mysql_connection()
    query_houses = "SELECT ta.id, ta.tipo_ned, un.owner_id_login FROM tab_abitazione ta " \
                   "INNER JOIN user_ned un ON ta.id=un.id_abitazione " \
                   "WHERE ta.stato_attivazione_ned = 1"
    df_houses = pd.read_sql_query(query_houses, connection)
    connection.close()
    token = check_token(None)
    futures = []
    timestamp = datetime.datetime.now().timestamp()

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
                            url=f"energy-coaching/{int(col['id'])}/{timestamp}",
                            token=token)
            for row, col in df_houses.iterrows()
        ]
    elif arguments["op"] == "score":
        futures = [
            executor.submit(send_push,
                            url=f"score/{int(col['id'])}",
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


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("--op", "--operation")
    args = vars(parser.parse_args())
    print(args)
    main(args)
