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

db_info = {}
db_info["user"] = settings_data["databases"]["warehouse"]["user"]
db_info["password"] = settings_data["databases"]["warehouse"]["password"]
db_info["host"] = settings_data["databases"]["warehouse"]["host"]
db_info["schema"] = settings_data["databases"]["warehouse"]["schema"]

db = pymysql.connect(
    host=db_info["host"],
    user=db_info["user"],
    password=db_info["password"],
    database=db_info["schema"],
)

db_engine = create_engine(
    f"mysql+pymysql://{db_info['user']}:{db_info['password']}@{db_info['host']}:3306/{db_info['schema']}",
    connect_args={"connect_timeout": 300},
)

cursor = db.cursor()
cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
cursor.close()
