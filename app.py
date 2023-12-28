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
import os
import click
import spillman as s
from datetime import date, timedelta
from datetime import datetime
from spillman.database import db
from spillman.settings import settings_data

logging.basicConfig(
    format="%(levelname)s - %(message)s",
    level=settings_data["global"]["loglevel"],
    filename="spillman-etl.log",
)


@click.group()
def main():
    """Spillman ETL"""


@main.command()
def daily():
    """Daily ETL Processing"""
    s.functions.header()
    start_date = datetime.today() - timedelta(days=1)
    end_date = datetime.today() - timedelta(days=0)

    for single_date in s.functions.daterange(start_date, end_date):
        process_date = single_date.strftime("%Y-%m-%d")
        logging.info(f"Running Spillman-ETL for {process_date}")

        s.cad.extract(process_date)
        s.fireincident.extract(process_date)
        s.emsincident.extract(process_date)
        s.lawincident.extract(process_date)
        s.rlog.extract(process_date)
        s.citation.extract(process_date)
        s.msglog.extract(process_date)
        s.avl.extract(process_date)


@main.command()
@click.option("--start", type=str, help="Start date (YYYY-MM-DD)")
@click.option("--end", type=str, help="End date (YYYY-MM-DD)")
def history(start, end):
    """Historical ETL Processing"""
    s.functions.header()
    logging.info(f"Running Spillman-ETL history from {start} to {end}")

    start_date = datetime.strptime(start, "%Y-%m-%d").date()
    end_date = datetime.strptime(end, "%Y-%m-%d").date()
    
    for single_date in s.functions.daterange(start_date, end_date):
        process_date = single_date.strftime("%Y-%m-%d")
        logging.info(f"Running Spillman-ETL for {process_date}")

        s.cad.extract(process_date)
        s.fireincident.extract(process_date)
        s.emsincident.extract(process_date)
        s.lawincident.extract(process_date)
        s.rlog.extract(process_date)
        s.citation.extract(process_date)
        s.msglog.extract(process_date)
        s.avl.extract(process_date)


@main.command()
def ddm():
    """Daily DataMart Process"""
    s.functions.header()
    logging.info(f"Executing Daily DataMart Stored Procedures")
    s.datamart.daily()


@main.command()
def geo():
    """Update Geobase Table"""
    s.functions.header()
    s.geobase.extract()


@main.command()
@click.option("--agency", type=str, help="Specify the Agency ID")
@click.option("--type", type=str, help="Specify the Agency Type (Law/Fire/EMS)")
def createagency(agency, type):
    """Create Agency Views in Warehouse"""
    s.functions.header()
    s.agencyview.create(agency, type)


if __name__ == "__main__":
    main()
