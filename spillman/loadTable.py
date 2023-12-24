import datetime
import traceback
import logging
import requests
from .settings import settings_data
from .database import db

requests.packages.urllib3.disable_warnings(
    requests.packages.urllib3.exceptions.InsecureRequestWarning
)

logging.basicConfig(
    format="%(levelname)s - %(message)s", level=settings_data["global"]["loglevel"]
)


def execute_sql(sql, values):
    with db.cursor() as cursor:
        try:
            cursor.execute(sql, values)
            db.commit()
        except Exception as e:
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
