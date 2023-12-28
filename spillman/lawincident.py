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


def extract(date):
    date_time = datetime.datetime.strptime(date, "%Y-%m-%d")
    start_date = date_time - datetime.timedelta(seconds=1)
    start_date = str(start_date.strftime("%m/%d/%Y"))
    end_date = date_time + datetime.timedelta(days=1)
    end_date = str(end_date.strftime("%m/%d/%Y"))

    logging.info(f"Processing Law Incidents from {start_date} to {end_date}")

    try:
        headers = {"Content-Type": "application/xml"}

        try:
            request_lwmain = f"""
                <PublicSafetyEnvelope version="1.0">
                    <PublicSafety id="">
                        <Query>
                            <lwmain>
                                <dispdat search_type="greater_than">{start_date}</dispdat>
                                <dispdat search_type="less_than">{end_date}</dispdat>
                            </lwmain>
                        </Query>
                    </PublicSafety>
                </PublicSafetyEnvelope>
                 """

            lwmain_xml = session.post(
                api_url, data=request_lwmain, headers=headers, verify=False
            )
            lwmain_decoded = lwmain_xml.content.decode("utf-8")
            lwmain = json.loads(json.dumps(xmltodict.parse(lwmain_decoded)))
            lwmain = lwmain["PublicSafetyEnvelope"]["PublicSafety"]["Response"][
                "lwmain"
            ]

        except Exception as e:
            error = format(str(e))
            if error.find("'NoneType'") != -1:
                logging.debug(f"Zero results from rlmain table.")
                return
            else:
                logging.error(traceback.print_exc())
                return

        for results in lwmain:
            try:
                callid = results["callid"]
            except KeyError:
                continue
            except TypeError:
                continue

            try:
                incident_id = results["number"]
            except KeyError:
                continue
            except TypeError:
                continue

            try:
                nature = results["nature"]
            except KeyError:
                nature = ""

            try:
                address = results["address"]
                address = address.replace('"', "")
                address = address.replace("'", "")
                address = address.replace(";", "")
            except KeyError:
                address = ""

            try:
                city = results["city"]
            except KeyError:
                city = ""

            try:
                state = results["state"]
            except KeyError:
                state = ""

            try:
                zip = results["zip"]
            except KeyError:
                zip = ""

            try:
                location = results["locatn"]
            except KeyError:
                location = ""

            try:
                agency = results["agency"]
            except KeyError:
                agency = ""
            except TypeError:
                agency = ""

            try:
                responsible_officer = results["respoff"]
            except KeyError:
                responsible_officer = ""

            try:
                geo_addr = results["geoaddr"]
            except KeyError:
                geo_addr = ""

            try:
                name_id = results["nameid"]
            except KeyError:
                name_id = ""

            try:
                received_by = results["rcvby"]
            except KeyError:
                received_by = ""

            try:
                occurred_dt1 = results["ocurdt1"]
                occurred_dt1 = f"{occurred_dt1[15:19]}-{occurred_dt1[9:11]}-{occurred_dt1[12:14]} {occurred_dt1[0:8]}"
            except KeyError:
                occurred_dt1 = "1900:01:01 00:00:00"

            try:
                occurred_dt2 = results["ocurdt2"]
                occurred_dt2 = f"{occurred_dt2[15:19]}-{occurred_dt2[9:11]}-{occurred_dt2[12:14]} {occurred_dt2[0:8]}"
            except KeyError:
                occurred_dt2 = "1900:01:01 00:00:00"
            try:
                reported_dt = results["dtrepor"]
                reported_dt = f"{reported_dt[15:19]}-{reported_dt[9:11]}-{reported_dt[12:14]} {reported_dt[0:8]}"
            except KeyError:
                reported_dt = "1900:01:01 00:00:00"

            try:
                dispatch_dt = results["dispdat"]
                dispatch_dt = (
                    f"{dispatch_dt[6:10]}-{dispatch_dt[0:2]}-{dispatch_dt[3:5]}"
                )
            except KeyError:
                dispatch_dt = "1900:01:01 00:00:00"

            try:
                contact = results["contact"]
                contact = contact.replace('"', "")
                contact = contact.replace("'", "")
            except KeyError:
                contact = ""

            try:
                condition = results["condtkn"]
            except KeyError:
                condition = ""

            try:
                disposition = results["dispos"]
            except KeyError:
                disposition = ""

            load_incident(
                callid,
                incident_id,
                nature,
                address,
                city,
                state,
                zip,
                location,
                agency,
                responsible_officer,
                geo_addr,
                name_id,
                received_by,
                occurred_dt1,
                occurred_dt2,
                reported_dt,
                dispatch_dt,
                contact,
                condition,
                disposition,
                "Law",
            )

    except Exception as e:
        logging.error(traceback.print_exc())
