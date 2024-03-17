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


def processdetail(agency, inc_id):
    logging.debug(f"Processing Offenses for {inc_id}")

    try:
        headers = {"Content-Type": "application/xml"}

        try:
            request_offenses = f"""
                <PublicSafetyEnvelope version="1.0">
                    <PublicSafety id="">
                        <Query>
                            <LawIncidentOffensesDetail>
                                <number search_type="equal_to">{inc_id}</number>
                            </LawIncidentOffensesDetail>
                        </Query>
                    </PublicSafety>
                </PublicSafetyEnvelope>
                 """

            offenses_xml = session.post(
                api_url, data=request_offenses, headers=headers, verify=False
            )
            offenses_decoded = offenses_xml.content.decode("utf-8")
            offenses = json.loads(json.dumps(xmltodict.parse(offenses_decoded)))
            offenses = offenses["PublicSafetyEnvelope"]["PublicSafety"]["Response"][
                "LawIncidentOffensesDetail"
            ]

        except Exception as e:
            error = format(str(e))
            if error.find("'NoneType'") != -1:
                logging.debug(f"Zero results from offenses table.")
                return
            else:
                logging.error(traceback.print_exc())
                return

        for results in offenses:
            try:
                incident_id = results["IncidentNumber"]
            except KeyError:
                continue
            except TypeError:
                continue

            try:
                offense_cd = results["OffenseCode"]
            except KeyError:
                offense_cd = ""

            load_offenses(
                incident_id,
                offense_cd,
                agency,
            )

    except Exception as e:
        logging.error(traceback.print_exc())

    logging.debug(f"Processing Circumstances for {inc_id}")

    try:
        headers = {"Content-Type": "application/xml"}

        try:
            request_circumstances = f"""
                <PublicSafetyEnvelope version="1.0">
                    <PublicSafety id="">
                        <Query>
                            <LawIncidentCircumstances>
                                <number search_type="equal_to">{inc_id}</number>
                            </LawIncidentCircumstances>
                        </Query>
                    </PublicSafety>
                </PublicSafetyEnvelope>
                 """

            circumstances_xml = session.post(
                api_url, data=request_circumstances, headers=headers, verify=False
            )
            circumstances_decoded = circumstances_xml.content.decode("utf-8")
            circumstances = json.loads(
                json.dumps(xmltodict.parse(circumstances_decoded))
            )
            circumstances = circumstances["PublicSafetyEnvelope"]["PublicSafety"][
                "Response"
            ]["LawIncidentCircumstances"]

        except Exception as e:
            error = format(str(e))
            if error.find("'NoneType'") != -1:
                logging.debug(f"Zero results from circumstances table.")
                return
            else:
                logging.error(traceback.print_exc())
                return

        for results in circumstances:
            try:
                incident_id = results["IncidentNumber"]
            except KeyError:
                continue
            except TypeError:
                continue

            try:
                circumstance_cd = results["CircumstanceCode"]
            except KeyError:
                circumstance_cd = ""

            load_circumstances(
                incident_id,
                circumstance_cd,
                agency,
            )

    except Exception as e:
        logging.error(traceback.print_exc())


def detailhistory():
    logging.info(f"Processing Law Incident Offenses and Circumstances")
    try:
        db = connect()
        cursor = db.cursor()
        sql = "SELECT agency, incident_id FROM dispatch.incident WHERE type = 'Law' AND dispatch_dt >= CURDATE() - INTERVAL 7 DAY;"
        cursor.execute(sql)
        results = cursor.fetchall()
        for row in results:
            processdetail(row[0], row[1])
        cursor.close()
        db.close()

    except Exception as e:
        logging.error(traceback.print_exc())
        cursor.close()
        db.close()


def detailhistoryall():
    logging.info(f"Processing Law Incident Offenses and Circumstances for all Incidents")
    try:
        db = connect()
        cursor = db.cursor()
        sql = "SELECT agency, incident_id FROM dispatch.incident WHERE type = 'Law';"
        cursor.execute(sql)
        results = cursor.fetchall()
        for row in results:
            processdetail(row[0], row[1])
        cursor.close()
        db.close()

    except Exception as e:
        logging.error(traceback.print_exc())
        cursor.close()
        db.close()
