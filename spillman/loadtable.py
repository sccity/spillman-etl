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
from .settings import settings_data
from .database import connect, connect_read

requests.packages.urllib3.disable_warnings(
    requests.packages.urllib3.exceptions.InsecureRequestWarning
)

logging.basicConfig(
    format="%(levelname)s - %(message)s", level=settings_data["global"]["loglevel"]
)


def execute_sql(sql, values):
    try:
        db = connect()
        cursor = db.cursor()
        cursor.execute(sql, values)
        db.commit()
        cursor.close()
        db.close()
    except Exception as e:
        cursor.close()
        db.close()
        handle_db_error(e, sql)


def handle_db_error(exception, sql):
    error = format(str(exception))
    if "Duplicate entry" in error:
        logging.debug("Entry already exists in database")
    else:
        logging.error(traceback.print_exc())
        logging.error(sql)


def load_incident(
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
    type,
):
    try:
        sql = """
        INSERT INTO incident(
            callid, incident_id, nature, address, city, state, zip, location, agency,
            responsible_officer, geo_addr, name_id, received_by, occurred_dt1, occurred_dt2,
            reported_dt, dispatch_dt, contact, `condition`, disposition, type
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        values = (
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
            type,
        )
        logging.debug(f"Processing incident: {callid}")
        execute_sql(sql, values)

    except Exception as e:
        logging.error(traceback.print_exc())


def load_sylog(userid, mode, table, data, logdate):
    try:
        sql = """
        INSERT INTO sylog (user_id, table, mode, date, data)
        VALUES (%s, %s, %s, %s, %s);
        """
        values = (userid, table, mode, logdate, data)
        logging.debug(f"Processing system log for user: {user_id} and date: {date}")
        execute_sql(sql, values)

    except Exception as e:
        logging.error(traceback.print_exc())


def load_avl(callid, agency, unit, unit_status, gps_x, gps_y, heading, speed, logdate):
    try:
        sql = """
        INSERT INTO avl (callid, agency, unit, unit_status, gps_x, gps_y, heading, speed, logdate)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        values = (
            callid,
            agency,
            unit,
            unit_status,
            gps_x,
            gps_y,
            heading,
            speed,
            logdate,
        )
        logging.debug(f"Processing AVL log for unit: {unit} and date: {logdate}")
        execute_sql(sql, values)

    except Exception as e:
        logging.error(traceback.print_exc())


def load_msglog(msgid, from_user, to_user, subject, message, msgdate):
    try:
        sql = """
        INSERT INTO msglog (msgid, from_user, to_user, subject, message, msgdate)
        VALUES (%s, %s, %s, %s, %s, %s);
        """
        values = (msgid, from_user, to_user, subject, message, msgdate)
        logging.debug(
            f"Processing message log for user: {from_user} and date: {msgdate}"
        )
        execute_sql(sql, values)

    except Exception as e:
        logging.error(traceback.print_exc())


def load_cad(
    callid,
    call_type,
    nature,
    priority,
    reported,
    occur_dt_1,
    occur_dt_2,
    address,
    city_cd,
    complainant_id,
    received_type,
    call_taker,
    emd,
):
    try:
        sql = """
        INSERT INTO cad(
            callid, call_type, nature, priority, reported, occur_dt_1, occur_dt_2, address,
            city_cd, complainant_id, received_type, call_taker, emd
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        values = (
            callid,
            call_type,
            nature,
            priority,
            reported,
            occur_dt_1,
            occur_dt_2,
            address,
            city_cd,
            complainant_id,
            received_type,
            call_taker,
            emd,
        )
        logging.debug(f"Processing CAD call: {callid}")
        execute_sql(sql, values)

    except Exception as e:
        logging.error(traceback.print_exc())


def load_rlog(
    callid,
    rlog_key,
    dispatcher,
    logdate,
    gps_x,
    gps_y,
    unit,
    zone,
    agency,
    tencode,
    description,
    sequence,
    calltype,
):
    try:
        sql = """
        INSERT INTO radiolog(
            rlog_key, callid, dispatcher, logdate, gps_x, gps_y, unit, zone, agency,
            tencode, description, sequence, calltype
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        values = (
            rlog_key,
            callid,
            dispatcher,
            logdate,
            gps_x,
            gps_y,
            unit,
            zone,
            agency,
            tencode,
            description,
            sequence,
            calltype,
        )
        logging.debug(f"Processing radio log: {rlog_key}")
        execute_sql(sql, values)

    except Exception as e:
        logging.error(traceback.print_exc())


def load_citation(
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
):
    try:
        sql = """
        INSERT INTO citation(
            citation_id, name_id, citation_dt, court_dt, agency, violation_dt, bond_amt,
            actual_amt, posted_amt, safe_amt, issuing_officer, court_cd, zone, address, city,
            state, zip, vehicle_id, citation_type_cd, geo_addr, incident_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        values = (
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
        logging.debug(f"Processing citation: {citation_id}")
        execute_sql(sql, values)

    except Exception as e:
        logging.error(traceback.print_exc())


def load_geobase(
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
):
    try:
        sql = """
        INSERT INTO geobase (geobase_id, house_number, street_address, city_cd, zipcode, zone_law, zone_fire, zone_ems, latitude, longitude)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        values = (
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
        logging.debug(f"Processing geobase for ID: {geobase_id}")
        db = connect()
        cursor = db.cursor()
        cursor.execute(sql, values)
        db.commit()
        cursor.close()
        db.close()

    except Exception as e:
        cursor.close()
        db.close()
        error = format(str(e))
        if "Duplicate entry" in error:
            try:
                sql = f"SELECT geobase_id, house_number, street_address, city_cd, zipcode, zone_law, zone_fire, zone_ems, latitude, longitude from geobase where geobase_id = '{geobase_id}';"
                db_ro = connect_read()
                cursor = db_ro.cursor()
                cursor.execute(sql)
                cursor.close()
                db_ro.close()
                geobase_results = cursor.fetchone()

                (
                    db_geobase_id,
                    db_house_number,
                    db_street_address,
                    db_city_cd,
                    db_zipcode,
                    db_zone_law,
                    db_zone_fire,
                    db_zone_ems,
                    db_latitude,
                    db_longitude,
                ) = geobase_results

                if house_number != db_house_number:
                    try:
                        db = connect()
                        cursor = db.cursor()
                        cursor.execute(
                            f"update geobase set house_number = '{house_number}' where geobase_id = '{geobase_id}'"
                        )
                        db.commit()
                        cursor.close()
                        db.close()
                    except:
                        cursor.close()
                        db.close()
                        logging.error(traceback.format_exc())
                        return

                if street_address != db_street_address:
                    try:
                        db = connect()
                        cursor = db.cursor()
                        cursor.execute(
                            f"update geobase set street_address = '{street_address}' where geobase_id = '{geobase_id}'"
                        )
                        db.commit()
                        cursor.close()
                        db.close()
                    except:
                        cursor.close()
                        db.close()
                        logging.error(traceback.format_exc())
                        return

                if city_cd != db_city_cd:
                    try:
                        db = connect()
                        cursor = db.cursor()
                        cursor.execute(
                            f"update geobase set city_cd = '{city_cd}' where geobase_id = '{geobase_id}'"
                        )
                        db.commit()
                        cursor.close()
                        db.close()
                    except:
                        cursor.close()
                        db.close()
                        logging.error(traceback.format_exc())
                        return

                if zipcode != db_zipcode:
                    try:
                        db = connect()
                        cursor = db.cursor()
                        cursor.execute(
                            f"update geobase set zipcode = '{zipcode}' where geobase_id = '{geobase_id}'"
                        )
                        db.commit()
                        cursor.close()
                        db.close()
                    except:
                        cursor.close()
                        db.close()
                        logging.error(traceback.format_exc())
                        return

                if zone_law != db_zone_law:
                    try:
                        db = connect()
                        cursor = db.cursor()
                        cursor.execute(
                            f"update geobase set zone_law = '{zone_law}' where geobase_id = '{geobase_id}'"
                        )
                        db.commit()
                        cursor.close()
                        db.close()
                    except:
                        cursor.close()
                        db.close()
                        logging.error(traceback.format_exc())
                        return

                if zone_fire != db_zone_fire:
                    try:
                        db = connect()
                        cursor = db.cursor()
                        cursor.execute(
                            f"update geobase set zone_fire = '{zone_fire}' where geobase_id = '{geobase_id}'"
                        )
                        db.commit()
                        cursor.close()
                        db.close()
                    except:
                        cursor.close()
                        db.close()
                        logging.error(traceback.format_exc())
                        return

                if zone_ems != db_zone_ems:
                    try:
                        db = connect()
                        cursor = db.cursor()
                        cursor.execute(
                            f"update geobase set zone_ems = '{zone_ems}' where geobase_id = '{geobase_id}'"
                        )
                        db.commit()
                        cursor.close()
                        db.close()
                    except:
                        cursor.close()
                        db.close()
                        logging.error(traceback.format_exc())
                        return

                if latitude != db_latitude:
                    try:
                        db = connect()
                        cursor = db.cursor()
                        cursor.execute(
                            f"update geobase set latitude = '{latitude}' where geobase_id = '{geobase_id}'"
                        )
                        db.commit()
                        cursor.close()
                        db.close()
                    except:
                        cursor.close()
                        db.close()
                        logging.error(traceback.format_exc())
                        return

                if longitude != db_longitude:
                    try:
                        db = connect()
                        cursor = db.cursor()
                        cursor.execute(
                            f"update geobase set longitude = '{longitude}' where geobase_id = '{geobase_id}'"
                        )
                        db.commit()
                        cursor.close()
                        db.close()
                    except:
                        cursor.close()
                        db.close()
                        logging.error(traceback.format_exc())
                        return

            except Exception as update_error:
                logging.error(f"Error updating geobase: {update_error}")
                logging.error(traceback.format_exc())

        else:
            logging.error(f"Error inserting geobase: {error}")
            logging.error(traceback.format_exc())
