import os
import logging
import sqlite3
from sqlite3 import Error
import textwrap

log = logging.getLogger('db_helper')

def run_sql(sql):
    '''
    Run a sql query on the DB
    '''
    log.debug('Running SQL: %s', sql)
    db_file = os.getenv('SQLITE_DATABASE','/etc/sqlite/trigger.db')
    con = sqlite3.connect(db_file)
    cur = con.cursor()
    cur.execute(sql)
    con.commit()
    result = cur.fetchall()
    con.close()
    return result

def create_dummy_job():
    '''
    Create a dummy job in the sqlite DB 
    for testing
    '''
    check_sql = 'SELECT * FROM jobs'
    all_jobs = run_sql(check_sql)
    log.debug(all_jobs)
    if len(all_jobs) == 0:
        log.debug('Adding dummy jobs...')
        dummy_json = textwrap.dedent('''{ "free_search": "bent","originator_edmo": "729"}''')
        create_job_dict = {'name' : 'Dummy Job',
                           'active' : 1,
                           'retrigger' : 0,
                           'query' : dummy_json,
                           'owner' : 'Rory',
                           'owner_email': 'rory.meyer@vliz.be'}
        result = create_job(create_job_dict)
    else: 
        log.debug('Some jobs already exists, not going to add dummy jobs.')
        result = None
    return result

def ensure_db():
    '''
    ensure that the sqlite DB exists, if not
    then create it
    '''
    db_file = os.getenv('SQLITE_DATABASE','/etc/sqlite/trigger.db')
    """ create a database connection to a SQLite database """
    conn = None
    try:
        log.debug('Connecting to database...')
        conn = sqlite3.connect(db_file)
    except Error as e:
        log.warning(e)
    finally:
        if conn:
            conn.close()

    log.info("Creating tables, if they don't exist...")
    try:
        log.debug('Creating jobs table...')
        create_sql = '''CREATE TABLE IF NOT EXISTS jobs(
                        id INTEGER PRIMARY KEY,
                        name TEXT NOT NULL,
                        active INTEGER,
                        order_placed INTEGER,
                        retrigger INTEGER,
                        query TEXT NOT NULL,
                        last_run TEXT,
                        last_data_file TEXT,
                        last_meta_file TEXT,
                        order_id INTEGER,
                        owner TEXT,
                        owner_email TEXT
                        table_constraints
                    );'''
        run_sql(create_sql)
    except Error as e:
        log.warning(e)

    try:
        log.debug('Inserting dummy table...')
        create_dummy_job()
    except Error as e:
        log.warning(e)
 
# def update_job_state(job_id, new_state):
#     '''
#     Update the state of a job
#     '''
#     update_sql = f"UPDATE jobs SET state='{new_state}' WHERE id={job_id}"
#     result = run_sql(update_sql)
#     log.info(result)
#     return result

# def update_order_id(job_id, order_id):
#     '''
#     Update the order_id for a job
#     '''
#     update_sql = f"UPDATE jobs SET order_id='{order_id}' WHERE id={job_id}"
#     result = run_sql(update_sql)
#     log.info(result)
#     return result

# def update_last_run(job_id, last_run):
#     '''
#     Update the last_run time for a job
#     '''
#     update_sql = f"UPDATE jobs SET last_run='{str(last_run)}' WHERE id={job_id}"
#     result = run_sql(update_sql)
#     log.info(result)
#     return result

def update_job(job_dict):
    # Take the job dict and overwrite the old one in the DB.
    query = f'''UPDATE jobs SET
    order_placed = {job_dict.get('order_placed')},
    retrigger = {job_dict.get('retrigger')},
    last_run = '{job_dict.get('last_run')}',
    last_data_file = '{job_dict.get('last_data_file')}',
    last_meta_file = '{job_dict.get('last_meta_file')}',
    order_id = {job_dict.get('order_id')}
    WHERE id = {job_dict.get('job_id')}
    '''
    result = run_sql(query)
    log.debug(result)

def create_job(job_dict):
    # Create a new job
    query = f'''INSERT INTO jobs 
    (name, active, order_placed, retrigger, query, last_run, last_data_file, last_meta_file, order_id, owner, owner_email)
    VALUES
    ('{job_dict.get('name')}',
     {job_dict.get('active',1)},{job_dict.get('order_placed',0)},{job_dict.get('retrigger',0)},
    '{job_dict.get('query')}',
    '{job_dict.get('last_run','1900-01-01 00:00:00.000')}',
    '{job_dict.get('last_data_file')}',
    '{job_dict.get('last_meta_file')}',
    {job_dict.get('order_id',-1)},
    '{job_dict.get('owner')}',
    '{job_dict.get('owner_email')}');'''
    result = run_sql(query)
    log.info(result) 

    return result