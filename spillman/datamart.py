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


def runProcedure(procName):
    max_retries = 5
    try:
        db = connect()
        retry_count = 0
        while retry_count < max_retries:
            try:
                cursor = db.cursor()
                logging.info(f"Running Stored Procedure: {procName}")
                cursor.callproc(f"{procName}")
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
        logging.error(traceback.print_exc())


def daily():
    runProcedure("spillman_dm.CREATE_DM_INC_RLOG_3Y")
    runProcedure("spillman_dm.CREATE_DM_INC_RLOG_1Y")
    runProcedure("spillman_dm.CREATE_DM_INC_RLOG_6M")
    runProcedure("spillman_dm.CREATE_DM_INC_RLOG_3M")
    runProcedure("spillman_dm.CREATE_DM_INC_RLOG_1M")
