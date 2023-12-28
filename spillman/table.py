# **********************************************************
# * CATEGORY  SOFTWARE
# * GROUP     DISPATCH/WAREHOUSING
# * AUTHOR    LANCE HAYNIE <LHAYNIE@SCCITY.ORG>
# **********************************************************
# Spillman-ETL
# Copyright Santa Clara City
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.#
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import sys
import urllib.request as urlreq
import json
import logging
import requests
import xmltodict
import traceback
import datetime
from .loadtable import *
from .settings import settings_data
from .database import db

from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

logging.basicConfig(
    format="%(levelname)s - %(message)s", level=settings_data["global"]["loglevel"]
)

api_url = settings_data["spillman"]["url"]
api_usr = settings_data["spillman"]["user"]
api_pwd = settings_data["spillman"]["password"]

session = requests.Session()
session.auth = (api_usr, api_pwd)


def spillman(table):
    logging.info(f"Processing Spillman Table {table}")
    headers = {"Content-Type": "application/xml"}

    try:
        request = f"""
            <PublicSafetyEnvelope version="1.0">
                <PublicSafety id="">
                    <Query>
                        <{table}>
                        </{table}>
                    </Query>
                </PublicSafety>
            </PublicSafetyEnvelope>
        """
        table_xml = session.post(api_url, data=request, headers=headers, verify=False)
        table_decoded = table_xml.content.decode("utf-8")
        tabledata = json.loads(json.dumps(xmltodict.parse(table_decoded)))
        tabledata = tabledata["PublicSafetyEnvelope"]["PublicSafety"]["Response"][f"{table}"]

        create_table(table, tabledata)

    except Exception as e:
        logging.error(f"Error processing Spillman Table {table}: {e}")
            
def create_table(table_name, tabledata):
    keys = list(tabledata[0].keys())

    try:
        db = connect()
        cursor = db.cursor()

        cursor.execute(f"DROP TABLE IF EXISTS {table_name}_test;")
        cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
        db.commit()

        cursor.execute(f"CREATE TABLE {table_name} ({', '.join([f'`{key}` VARCHAR(255)' for key in keys])})")
        db.commit()

        for row in tabledata:
            values = [row.get(key, None) for key in keys]
            insert_query = f"INSERT INTO {table_name} ({', '.join(['`' + key + '`' for key in keys])}) VALUES ({', '.join(['%s' for _ in keys])})"
            cursor.execute(insert_query, tuple(values))

        db.commit()
        cursor.close()
        db.close()
    except Exception as e:
        cursor.close()
        db.close()
        logging.error(f"Error with table {table_name}: {e}")
        logging.error(traceback.format_exc())
