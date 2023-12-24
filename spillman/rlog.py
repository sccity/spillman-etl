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

    logging.info(f"Processing Radio Logs from {start_date} to {end_date}")

    try:
        headers = {"Content-Type": "application/xml"}

        try:
            request_rlmain = f"""
                <PublicSafetyEnvelope version="1.0">
                    <PublicSafety id="">
                        <Query>
                            <rlmain>
                                <logdate search_type="greater_than">{start_date}</logdate>
                                <logdate search_type="less_than">{end_date}</logdate>
                            </rlmain>
                        </Query>
                    </PublicSafety>
                </PublicSafetyEnvelope>
                 """
            rlog_xml = session.post(
                api_url, data=request_rlmain, headers=headers, verify=False
            )
            rlog_decoded = rlog_xml.content.decode("utf-8")
            rlog = json.loads(json.dumps(xmltodict.parse(rlog_decoded)))
            rlog = rlog["PublicSafetyEnvelope"]["PublicSafety"]["Response"]["rlmain"]

        except Exception as e:
            error = format(str(e))
            if error.find("'NoneType'") != -1:
                logging.debug(f"Zero results from rlmain table.")
                return
            else:
                logging.error(traceback.print_exc())
                return

        for results in rlog:
            date = results["logdate"]
            logdate = f"{date[15:19]}-{date[9:11]}-{date[12:14]} {date[0:8]}"
            gps_x = f"{results['xpos'][:4]}.{results['xpos'][4:]}"
            gps_y = f"{results['ypos'][:2]}.{results['ypos'][2:]}"

            sequence = results["seq"]

            try:
                callid = results["callid"]
            except KeyError:
                callid = ""

            try:
                agency = results["agency"]
            except KeyError:
                agency = ""

            try:
                zone = results["zone"]
            except KeyError:
                zone = ""

            try:
                tencode = results["tencode"]
            except KeyError:
                tencode = ""

            try:
                unit = results["unit"]
            except KeyError:
                unit = ""

            rlog_key = f"{date[15:19]}{date[9:11]}{date[12:14]}{date[0:2]}{date[3:5]}{date[6:8]}{unit}{agency}{tencode}"

            try:
                description = results["desc"]
                description = description.replace('"', "")
                description = description.replace("'", "")
            except KeyError:
                description = ""

            try:
                dispatcher = results["dpatchr"]
                dispatcher = dispatcher.replace('"', "")
                dispatcher = dispatcher.replace("'", "")
            except KeyError:
                dispatcher = ""

            try:
                calltype = results["calltyp"]
            except KeyError:
                calltype = ""

            load_rlog(
                callid,
                rlog_key,
                dispatcher,
                logdate,
                gps_x,
                gps_y,
                unit,
                zone,
                agency,
                tencode,
                description,
                sequence,
                calltype,
            )

    except Exception as e:
        logging.error(traceback.print_exc())
