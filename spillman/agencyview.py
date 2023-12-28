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
import datetime
import traceback
import logging
import requests
import time
from .settings import settings_data
from .database import connect, connect_read

requests.packages.urllib3.disable_warnings(
    requests.packages.urllib3.exceptions.InsecureRequestWarning
)

logging.basicConfig(
    format="%(levelname)s - %(message)s", level=settings_data["global"]["loglevel"]
)


def runQuery(sql):
    max_retries = 5
    try:
        db = connect()
        retry_count = 0
        while retry_count < max_retries:
            try:
                cursor = db.cursor()
                cursor.execute(f"{sql}")
                db.commit()
                break
            except Exception as e:
                retry_count += 1
                logging.info(f"Retrying ({retry_count}/{max_retries}) Error: {e}")
                time.sleep(60)
        if retry_count == max_retries:
            logging.error(traceback.print_exc())
        cursor.close()
        db.close()
    except Exception as e:
        cursor.close()
        db.close()


def create_view(agency, view, source):
    view = view.lower()
    runQuery(
        f"create view {agency}.{view} as SELECT * FROM {source} where agency = '{agency}';"
    )


def create(agency, type):
    agency = agency.lower()
    type = type.lower()
    logging.info(f"Setting up agency {agency} with type {type}")
    runQuery(f"drop schema if exists {agency}")
    runQuery(
        f"create schema {agency} DEFAULT CHARACTER SET utf8mb4 DEFAULT COLLATE utf8mb4_general_ci;"
    )
    runQuery(
        f"create view {agency}.agency as select * from dispatch.apagncy where abbr = '{agency}'"
    )
    runQuery(f"create view {agency}.city as select * from dispatch.apcity")
    runQuery(
        f"create view {agency}.cad as SELECT i.agency, c.* FROM dispatch.cad_calls c left join dispatch.incident i on c.callid = i.callid where i.agency = '{agency}'"
    )
    runQuery(f"create view {agency}.geobase as select * from dispatch.geobase")
    create_view(agency, "DM_INC_RLOG_3Y", "spillman_dm.DM_INC_RLOG_3Y")
    create_view(agency, "DM_INC_RLOG_1Y", "spillman_dm.DM_INC_RLOG_1Y")
    create_view(agency, "DM_INC_RLOG_6M", "spillman_dm.DM_INC_RLOG_6M")
    create_view(agency, "DM_INC_RLOG_3M", "spillman_dm.DM_INC_RLOG_3M")
    create_view(agency, "DM_INC_RLOG_1M", "spillman_dm.DM_INC_RLOG_1M")
    create_view(agency, "incidents", "dispatch.incident")
    create_view(agency, "users", "dispatch.apnames")
    create_view(agency, "avl", "dispatch.avl")
    create_view(agency, "units", "dispatch.cdunit")
    create_view(agency, "system_units", "dispatch.syunit")
    create_view(agency, "radiolog", "dispatch.radiolog")

    if type == "law":
        runQuery(
            f"""
                create view {agency}.citations as
                SELECT * FROM dispatch.citations where agency = '{agency}';
                """
        )
