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
import logging
from datetime import date, timedelta


def header():
    logging.info(
        "   _____       _ _ _                             ______ _______ _      "
    )
    logging.info(
        "  / ____|     (_) | |                           |  ____|__   __| |     "
    )
    logging.info(
        " | (___  _ __  _| | |_ __ ___   __ _ _ __ ______| |__     | |  | |     "
    )
    logging.info(
        "  \___ \| '_ \| | | | '_ ` _ \ / _` | '_ \______|  __|    | |  | |     "
    )
    logging.info(
        "  ____) | |_) | | | | | | | | | (_| | | | |     | |____   | |  | |____ "
    )
    logging.info(
        " |_____/| .__/|_|_|_|_| |_| |_|\__,_|_| |_|     |______|  |_|  |______|"
    )
    logging.info(
        "        | |                                                            "
    )
    logging.info(
        "        |_|                                                            "
    )


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)
