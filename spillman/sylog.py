# **********************************************************
# * CATEGORY  SOFTWARE
# * GROUP     DISPATCH
# * AUTHOR    LANCE HAYNIE <LHAYNIE@SCCITY.ORG>
# **********************************************************
# Spillman ETL
# Copyright Santa Clara City
# Developed for Santa Clara - Ivins Fire & Rescue
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
import re
from .loadTable import *
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


def extract(date):
    date_time = datetime.datetime.strptime(date, "%Y-%m-%d")
    start_date = date_time - datetime.timedelta(seconds=1)
    end_date = date_time + datetime.timedelta(days=1)

    logging.info(f"Processing system logs from {start_date} to {end_date}")

    try:
        headers = {"Content-Type": "application/xml"}

        try:
            request_sylog = f"""
                <PublicSafetyEnvelope version="1.0">
                    <PublicSafety id="">
                        <Query>
                            <SystemLogTable>
                                <TimeOfAccess search_type="greater_than">{start_date}</TimeOfAccess>
                                <TimeOfAccess search_type="less_than">{end_date}</TimeOfAccess>
                            </SystemLogTable>
                        </Query>
                    </PublicSafety>
                </PublicSafetyEnvelope>
                 """

            sylog_xml = session.post(
                api_url, data=request_sylog, headers=headers, verify=False
            )
            sylog_decoded = sylog_xml.content.decode("utf-8")
            sylog = json.loads(json.dumps(xmltodict.parse(sylog_decoded)))
            sylog = sylog["PublicSafetyEnvelope"]["PublicSafety"]["Response"][
                "SystemLogTable"
            ]

        except Exception as e:
            error = format(str(e))
            if error.find("'NoneType'") != -1:
                print(f"Zero results from table.")
                return
            else:
                print(traceback.print_exc())
                return

        for results in sylog:
            try:
                userid = results["UserID"]
            except KeyError:
                continue
            except TypeError:
                continue

            try:
                mode = results["ModeUsed"]
            except KeyError:
                mode = ""

            try:
                table = results["TableBeingAccessed"]
            except KeyError:
                table = ""

            try:
                data = results["MiscellaneousData"]
                data = data.encode("utf-8", "ignore").decode("utf-8")
                data = re.sub(r"[^a-zA-Z0-9\s\t]", "", data)
                data = data.strip()
            except KeyError:
                data = ""

            try:
                logdate = results["TimeOfAccess"]
                logdate = (
                    f"{logdate[15:19]}-{logdate[9:11]}-{logdate[12:14]} {logdate[0:8]}"
                )
            except KeyError:
                logdate = "1900:01:01 00:00:00"

            load_sylog(userid, mode, table, data, logdate)

    except Exception as e:
        logging.error(traceback.print_exc())
