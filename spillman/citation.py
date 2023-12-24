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

    logging.info(f"Processing Citations from {start_date} to {end_date}")

    try:
        headers = {"Content-Type": "application/xml"}

        try:
            request_emmain = f"""
                <PublicSafetyEnvelope version="1.0">
                    <PublicSafety id="">
                        <Query>
                            <MasterCitationTable>
                                <DateOfCitation search_type="greater_than">{start_date}</DateOfCitation>
                                <DateOfCitation search_type="less_than">{end_date}</DateOfCitation>
                            </MasterCitationTable>
                        </Query>
                    </PublicSafety>
                </PublicSafetyEnvelope>
                 """

            citation_xml = session.post(
                api_url, data=request_emmain, headers=headers, verify=False
            )
            citation_decoded = citation_xml.content.decode("utf-8")
            citation = json.loads(json.dumps(xmltodict.parse(citation_decoded)))
            citation = citation["PublicSafetyEnvelope"]["PublicSafety"]["Response"][
                "MasterCitationTable"
            ]

        except Exception as e:
            error = format(str(e))
            if error.find("'NoneType'") != -1:
                logging.debug(f"Zero results from MasterCitationTable table.")
                return
            else:
                logging.error(traceback.print_exc())
                return

        for results in citation:
            try:
                citation_id = results["CitationNumber"]
            except KeyError:
                continue
            except TypeError:
                continue

            try:
                name_id = results["NameNumber"]
            except KeyError:
                name_id = ""

            try:
                citation_dt = results["DateOfCitation"]
                citation_dt = f"{citation_dt[15:19]}-{citation_dt[9:11]}-{citation_dt[12:14]} {citation_dt[0:8]}"
            except KeyError:
                citation_dt = "1900:01:01 00:00:00"

            try:
                court_dt = results["DateOfCitation"]
                court_dt = f"{court_dt[15:19]}-{court_dt[9:11]}-{court_dt[12:14]} {court_dt[0:8]}"
            except KeyError:
                court_dt = "1900:01:01 00:00:00"

            try:
                agency = results["AgencyCode"]
            except KeyError:
                agency = ""

            try:
                violation_dt = results["ViolationDate"]
                violation_dt = f"{violation_dt[15:19]}-{violation_dt[9:11]}-{violation_dt[12:14]} {violation_dt[0:8]}"
            except KeyError:
                violation_dt = citation_dt

            try:
                bond_amt = results["BondAmount"]
            except KeyError:
                bond_amt = ""

            try:
                actual_amt = results["Actual"]
            except KeyError:
                actual_amt = ""

            try:
                posted_amt = results["Posted"]
            except KeyError:
                posted_amt = ""

            try:
                safe_amt = results["Safe"]
            except KeyError:
                safe_amt = ""

            try:
                issuing_officer = results["IssuingOfficer"]
            except KeyError:
                issuing_officer = ""

            try:
                court_cd = results["CourtCode"]
            except KeyError:
                court_cd = ""

            try:
                zone = results["AreaLocationCode"]
            except KeyError:
                zone = ""

            try:
                address = results["StreetAddress"]
                address = address.replace('"', "")
                address = address.replace("'", "")
                address = address.replace(";", "")
            except KeyError:
                address = ""

            try:
                city = results["City"]
            except KeyError:
                city = ""

            try:
                state = results["StateAbbreviation"]
            except KeyError:
                state = ""

            try:
                zipcode = results["ZIPCode"]
            except KeyError:
                zipcode = ""

            try:
                vehicle_id = results["VehicleNumber"]
            except KeyError:
                vehicle_id = ""

            try:
                citation_type_cd = results["CitationType"]
            except KeyError:
                citation_type_cd = ""

            try:
                geo_addr = results["GeobaseAddressID"]
            except KeyError:
                geo_addr = ""

            try:
                incident_id = results["LawIncident"]
            except KeyError:
                incident_id = ""

            load_citation(
                citation_id,
                name_id,
                citation_dt,
                court_dt,
                agency,
                violation_dt,
                bond_amt,
                actual_amt,
                posted_amt,
                safe_amt,
                issuing_officer,
                court_cd,
                zone,
                address,
                city,
                state,
                zipcode,
                vehicle_id,
                citation_type_cd,
                geo_addr,
                incident_id,
            )

    except Exception as e:
        logging.error(traceback.print_exc())
