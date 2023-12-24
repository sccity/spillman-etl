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
import re
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
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
    start_date = str(start_date.strftime("%m/%d/%Y"))
    end_date = date_time + datetime.timedelta(days=0)
    end_date = str(end_date.strftime("%m/%d/%Y"))

    process(f"{start_date} 23:59:59", f"{end_date} 06:00:00")
    process(f"{end_date} 05:59:59", f"{end_date} 12:00:00")
    process(f"{end_date} 11:59:59", f"{end_date} 18:00:00")
    process(f"{end_date} 18:59:59", f"{end_date} 23:59:59")


def process(start_date, end_date):
    logging.info(f"Processing Message Logs from {start_date} to {end_date}")

    try:
        headers = {"Content-Type": "application/xml"}

        try:
            request_msglog = f"""
                <PublicSafetyEnvelope version="1.0">
                    <PublicSafety id="">
                        <Query>
                            <MessengerMessageTable>
                                <WhenReceived search_type="greater_than">{start_date}</WhenReceived>
                                <WhenReceived search_type="less_than">{end_date}</WhenReceived>
                            </MessengerMessageTable>
                        </Query>
                    </PublicSafety>
                </PublicSafetyEnvelope>
                 """

            msglog_xml = session.post(
                api_url, data=request_msglog, headers=headers, verify=False
            )
            msglog_decoded = msglog_xml.content.decode("utf-8")
            msglog = json.loads(json.dumps(xmltodict.parse(msglog_decoded)))
            msglog = msglog["PublicSafetyEnvelope"]["PublicSafety"]["Response"][
                "MessengerMessageTable"
            ]

        except Exception as e:
            error = format(str(e))
            if error.find("'NoneType'") != -1:
                print(f"Zero results from table.")
                return
            else:
                print(traceback.print_exc())
                return

        for results in msglog:
            try:
                msgid = results["MessageNumber"]
            except KeyError:
                continue
            except TypeError:
                continue

            try:
                from_user = results["MessageSender"]
            except KeyError:
                from_user = ""

            try:
                to_user = results["Recipient"]
            except KeyError:
                to_user = ""

            try:
                subject = results["MessageSubject"]
            except KeyError:
                subject = ""

            try:
                message_xml = results["MessageData"]
                root = ET.fromstring(message_xml)
                data_text = root.find(".//html").text
                soup = BeautifulSoup(data_text, "html.parser")
                message = soup.get_text()
            except KeyError:
                message = ""

            try:
                msgdate = results["WhenReceived"]
                msgdate = (
                    f"{msgdate[15:19]}-{msgdate[9:11]}-{msgdate[12:14]} {msgdate[0:8]}"
                )
            except KeyError:
                msgdate = "1900:01:01 00:00:00"

            load_msglog(msgid, from_user, to_user, subject, message, msgdate)

    except Exception as e:
        logging.error(traceback.print_exc())
