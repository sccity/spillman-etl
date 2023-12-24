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
    end_date = date_time + datetime.timedelta(days=1)
    end_date = str(end_date.strftime("%m/%d/%Y"))

    process(f"{start_date} 23:59:59", f"{end_date} 06:00:00")
    process(f"{end_date} 05:59:59", f"{end_date} 12:00:00")
    process(f"{end_date} 11:59:59", f"{end_date} 18:00:00")
    process(f"{end_date} 18:59:59", f"{end_date} 23:59:59")


def process(start_date, end_date):
    logging.info(f"Processing AVL logs from {start_date} to {end_date}")
    try:
        headers = {"Content-Type": "application/xml"}

        try:
            request_avl = f"""
                <PublicSafetyEnvelope version="1.0">
                    <PublicSafety id="">
                        <Query>
                            <rlavllog>
                                <logdate search_type="greater_than">{start_date}</logdate>
                                <logdate search_type="less_than">{end_date}</logdate>
                            </rlavllog>
                        </Query>
                    </PublicSafety>
                </PublicSafetyEnvelope>
                 """
            avl_xml = session.post(
                api_url, data=request_avl, headers=headers, verify=False
            )
            avl_decoded = avl_xml.content.decode("utf-8")
            avl = json.loads(json.dumps(xmltodict.parse(avl_decoded)))
            avl = avl["PublicSafetyEnvelope"]["PublicSafety"]["Response"]["rlavllog"]

        except Exception as e:
            error = format(str(e))
            if error.find("'NoneType'") != -1:
                logging.debug(f"Zero results from table.")
                return
            else:
                logging.error(traceback.print_exc())
                return

        for results in avl:
            try:
                callid = results["callid"]
            except KeyError:
                callid = ""

            try:
                agency = results["agency"]
            except KeyError:
                agency = ""

            try:
                unit = results["assgnmt"]
            except KeyError:
                unit = ""

            try:
                unit_status = results["stcode"]
            except KeyError:
                unit_status = ""

            try:
                gps_x = results["xlng"]
            except:
                gps_x = 0

            try:
                gps_y = results["ylat"]
            except:
                gps_y = 0

            try:
                heading = results["heading"]
            except:
                heading = 0

            try:
                speed = results["speed"]
            except:
                speed = 0

            try:
                date = results["logdate"]
                logdate = f"{date[15:19]}-{date[9:11]}-{date[12:14]} {date[0:8]}"
            except:
                logdate = "1900-01-01 00:00:00"

            load_avl(
                callid,
                agency,
                unit,
                unit_status,
                gps_x,
                gps_y,
                heading,
                speed,
                logdate,
            )
    except Exception as e:
        logging.error(traceback.print_exc())
