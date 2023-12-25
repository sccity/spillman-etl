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


def extract():
    # process("0", "1000")
    process("0", "50000")
    process("49999", "100000")
    process("99999", "150000")
    process("149999", "200000")
    process("199999", "250000")
    process("249999", "300000")


def process(start_id, end_id):
    logging.info(f"Processing GeoBase Address")

    try:
        headers = {"Content-Type": "application/xml"}

        try:
            request_geobase = f"""
                <PublicSafetyEnvelope version="1.0">
                    <PublicSafety id="">
                        <Query>
                            <GeobaseAddressIDMaintenance>
                                <IDNumberOfAddress search_type="greater_than">{start_id}</IDNumberOfAddress>
                                <IDNumberOfAddress search_type="less_than">{end_id}</IDNumberOfAddress>
                            </GeobaseAddressIDMaintenance>
                        </Query>
                    </PublicSafety>
                </PublicSafetyEnvelope>
                 """

            geobase_xml = session.post(
                api_url, data=request_geobase, headers=headers, verify=False
            )
            geobase_decoded = geobase_xml.content.decode("utf-8")
            geobase = json.loads(json.dumps(xmltodict.parse(geobase_decoded)))
            geobase = geobase["PublicSafetyEnvelope"]["PublicSafety"]["Response"][
                "GeobaseAddressIDMaintenance"
            ]

        except Exception as e:
            error = format(str(e))
            if error.find("'NoneType'") != -1:
                logging.debug(f"Zero results from table.")
                return
            else:
                logging.error(traceback.print_exc())
                return

        for results in geobase:
            try:
                geobase_id = results["IDNumberOfAddress"]
            except KeyError:
                continue
            except TypeError:
                continue

            try:
                house_number = results["HouseNumber"]
            except KeyError:
                house_number = ""

            try:
                street_address = results["StreetAddress"]
                street_address = street_address.replace('"', "")
                street_address = street_address.replace("'", "")
                street_address = street_address.replace(";", "")
            except KeyError:
                street_address = ""

            try:
                city_cd = results["CityCode"]
            except KeyError:
                city_cd = ""

            try:
                zipcode = results["ZIP"]
            except KeyError:
                zipcode = ""

            try:
                zone_law = results["ZoneLa"]
            except KeyError:
                zone_law = ""

            try:
                zone_fire = results["ZoneFa"]
            except KeyError:
                zone_fire = ""

            try:
                zone_ems = results["ZoneEa"]
            except KeyError:
                zone_ems = ""

            try:
                latitude = float(results["YCoordinate"]) / 1e6
            except KeyError:
                latitude = 0

            try:
                longitude = float(results["XCoordinate"]) / 1e6
            except KeyError:
                longitude = 0

            load_geobase(
                geobase_id,
                house_number,
                street_address,
                city_cd,
                zipcode,
                zone_law,
                zone_fire,
                zone_ems,
                latitude,
                longitude,
            )

    except Exception as e:
        logging.error(traceback.print_exc())
