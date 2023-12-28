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
import pymysql
from sqlalchemy import create_engine
from .settings import settings_data


def connect():
    return pymysql.connect(
        host=settings_data["databases"]["warehouse"]["host"],
        user=settings_data["databases"]["warehouse"]["user"],
        password=settings_data["databases"]["warehouse"]["password"],
        database=settings_data["databases"]["warehouse"]["schema"],
    )


def connect_read():
    return pymysql.connect(
        host=settings_data["databases"]["warehouse"]["host_ro"],
        user=settings_data["databases"]["warehouse"]["user"],
        password=settings_data["databases"]["warehouse"]["password"],
        database=settings_data["databases"]["warehouse"]["schema"],
    )


db = connect()
cursor = db.cursor()
cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
cursor.close()
db.close()

db_ro = connect_read()
cursor = db_ro.cursor()
cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
cursor.close()
db_ro.close()
