    #!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
"""
Created on Jul 2022

This is a the trigger subsystem for the bio-pipes system. The goal is 
to determine whether a dataset should be reloaded or not by checking
the last modified time of the dataset on the SDN API server. 

There should also be room for manual triggers via the luigi 
web interface.

@author: rory
"""

import sys 
import argparse
import logging 
import datetime
import os
import json
import traceback  
from pathlib import Path
import urllib

# from more_itertools import last
# import pysqlite3

from apscheduler.schedulers.blocking import BlockingScheduler
import app.db_helper as db_helper
import app.cdi_helper as cdi_helper
import app.odv_to_dwc as odv_to_dwc
import app.alerting as alerting

log = logging.getLogger('main') 

def trigger_pipeline(job_dict): 
    '''
    Trigger the pipeline that needs to run after all the raw data has been
    downloaded. 
    '''
    log.info('Triggering ODV-to-DwC conversion for job "{0}"'.format(job_dict.get('name')))
    status = odv_to_dwc.odv_to_dwc(job_dict)
    
    # alert_msg = alerting.Alerter(os.getenv('WEBHOOK'))
    # alert_msg.create_msg_card(title = 'Message',
    #                           text = '', 
    #                           activity_title = 'Activity',
    #                           activity_text = '', 
    #                           fact_dict = {}, 
    #                           loglevel = "INFO")
    # alert_msg.send()
    
 
def check_new_data(job_dict, api_client):
    '''
    Check if there is new/updated data to add to a dataset
    '''
    log.debug('Checking if job {0} has new data...'.format(job_dict.get('name')))
    last_update = api_client.get_last_update(job_dict)
    if (last_update is None):
        log.warning('  -Job {0} has no records.'.format(job_dict.get('name')))
        return False
    elif (last_update > job_dict.get('last_run')):
        log.debug('  -Job {0} has new data!'.format(job_dict.get('name')))
        return True
    else:
        log.debug('  -Job {0} has no new data...'.format(job_dict.get('name')))
        return False

def check_order_status(job_dict, api_client):
    '''
    Check if the order is ready to download
    '''
    log.debug('Checking order status for job {0}'.format(job_dict.get('job_id')))
    order_status = api_client.get_order(job_dict)
    return order_status

def place_order(job_dict, api_client):
    '''
    Place an order for a dataset
    '''
    log.info('  -Placing order for job {0}'.format(job_dict.get('job_id'))) 
    result = api_client.place_order(job_dict)
    if result.get('OrderNumber') is not None:
        job_dict['order_id'] = result.get('OrderNumber')
        job_dict['last_run'] = datetime.datetime.now() 
        job_dict['order_placed'] = 1
    else:
        log.warning('Order placing failed...')
    return job_dict

def download_order(job_dict, order_status, api_client):
    '''
    Download the order

    Example API response :
    {'dateCreated': '2022-08-22 10:29:08.947',
    'dateLastUpdated': '2022-08-22 10:29:08.947',
    'download': {'csv': {'unrestricted': {'downloadUrl': 'https://seadatanet-buffer5.maris.nl/api_v5.1/order/download/csv/60838',
                                        'fileSize': 1834}},
                'data': {'unrestricted': {'downloadUrl': 'seadata.csc.fi/api/orders/60838/download/00/c/9HsQTDF9EEc.%3A_N',
                                        'fileSize': 14037,
                                        'name': 'order_60838_unrestricted.zip'}}},
    'main_order': None,
    'order_lines': 10,
    'order_lines_processing': 0,
    'order_lines_ready_for_download': 10,
    'order_number': 60838,
    'sub_order': None, 
    'userOrderName': 'from gui test'}
    '''
    log.info('Downloading order for job {0}'.format(job_dict.get('job_id')))
    log.debug(f'Job Dict: {job_dict}')
    log.debug(f'Order Status: {order_status}') 

    job_id = job_dict.get('job_id')
    order_number = order_status.get('order_number')
    order_name = order_status.get('userOrderName')
    download_complete = False
    download_urls = order_status.get('download',{})
    
    log.info(f'Getting download urls: {download_urls}')

    # NOTE: For some reason the 'csv' metadata file can take some time to generate
    # after the 'data' file is produced. Not sure why... 
    # {'data': {'unrestricted': 
    #                         {'name': 'order_60838_unrestricted.zip', 
    #                          'downloadUrl': 'seadata.csc.fi/api/orders/60838/download/00/c/jE6%5Eo%3AlR%7Co%2BT%249I', 
    #                         'fileSize': 14037}
    #           }, 
    #  'csv': {'unrestricted': []}
    # }
    try:
        download_csv_url = download_urls.get('csv',{}).get('unrestricted',{}).get('downloadUrl') + '/unrestricted'
    except:
        download_csv_url = None
        log.warning('Problem getting CSV download URL from order')
    try:
        download_data_url = download_urls.get('data',{}).get('unrestricted',{}).get('downloadUrl')
        download_data_name = download_urls.get('data',{}).get('unrestricted',{}).get('name')
    except:
        log.error('Problem getting ZIP download URL from order')
        download_data_url = None
        download_data_name = None 
    
    try:
        Path(f"/code/datasets/{order_name}/{order_number}").mkdir(parents=True, exist_ok=True)
        download_path = f"/code/datasets/{order_name}/{order_number}/{download_data_name}"

        if download_csv_url is not None:
            # requests.get(download_csv_url) 
            p = urllib.parse.urlparse(download_csv_url,'http')
            csv_file = api_client.download_order(p.geturl())
            meta_path = f"/code/datasets/{order_name}/{order_number}/meta.zip"
            open(meta_path, "wb").write(csv_file.content)
            log.debug(f'Downloaded meta data file {len(csv_file.content)}')
            job_dict['last_meta_file'] = meta_path

        if download_data_url is not None:
            # data_file = requests.get("http://" + download_data_url) 
            p = urllib.parse.urlparse('//' + download_data_url,'http')
            data_file = api_client.download_order(p.geturl()) 
            data_path = f"/code/datasets/{order_name}/{order_number}/{download_data_name}"
            open(data_path, "wb").write(data_file.content)
            log.debug(f'Downloaded data file {len(data_file.content)}')
            job_dict['last_data_file'] = data_path

        # Mkdir folder for this job_id and write csv and datafile into it.
        download_complete = True
    except Exception as e:
        log.error('Error downloading order for job {0}'.format(job_dict.get('job_id')))
        log.error(e)
        download_complete = False
        download_path = ''
        
    if download_complete: 
        log.info('Download complete. ')
    else:
        log.error('Error downloading job {0}'.format(job_id))

    return job_dict

def parse_job(job_tuple):
    '''
    Turns the job db tuple into a nice dict 

    CREATE TABLE IF NOT EXISTS jobs(
    0                   id INTEGER PRIMARY KEY,
    1                   name TEXT NOT NULL,
    2                   active INTEGER,
    3                   order_placed INTEGER,
    4                   retrigger INTEGER,
    5                   query TEXT NOT NULL,
    6                   last_run TEXT,
    7                   last_data_file TEXT,
    8                   last_meta_file TEXT,
    9                   order_id INTEGER,
    10                  owner TEXT,
    11                  owner_email TEXT 
    '''
    job_dict = {}
    try: 
        job_dict['job_id'] = job_tuple[0]
        job_dict['name'] = job_tuple[1]
        job_dict['active'] = job_tuple[2]
        job_dict['order_placed'] = job_tuple[3]
        job_dict['retrigger'] = job_tuple[4]
        job_dict['query'] = json.loads(job_tuple[5])

        # If the job has never been run before 
        if job_tuple[6] == '' or job_tuple[6] == None:
            job_dict['last_run'] = datetime.datetime(1900,1,1)
        else:
            job_dict['last_run'] = datetime.datetime.strptime(job_tuple[6], '%Y-%m-%d %H:%M:%S.%f')
        job_dict['last_data_file'] = job_tuple[7]
        job_dict['last_meta_file'] = job_tuple[8]
        job_dict['order_id'] = job_tuple[9]
        job_dict['owner'] = job_tuple[10]
        job_dict['owner_email'] = job_tuple[11]

    except Exception as e:
        log.error('Error parsing job tuple: {0}'.format(e))
    return job_dict

def check_status():  
    '''
    Combine a prebuilt header/footer with snippets produced by other
    containers into a new_datasets.xml file. 
    '''
    
    log.info('Fetching all jobs...')
    jobs =  db_helper.run_sql('SELECT * FROM jobs')
    
    log.info('Setting up API client...')
    api_client = cdi_helper.SeadatanetAPI()

    log.info('Checking if any jobs need to be run...')
    for job in jobs:
        try:
            job_dict = parse_job(job)
            log.info('=====================')  
            log.info('Checking trigger for job "{0}"...'.format(job_dict.get('name')))  

            if job_dict.get('active'):
                # Job is active and should be checked
                if not(job_dict.get('order_placed')):
                    # Check if new data exists, and order should be placed
                    log.info('  -Job {0} is being checked for new data...'.format(job_dict.get('job_id')))
                    new_data = check_new_data(job_dict, api_client)
                    if new_data:
                        log.info('  -Job {0} has new data!'.format(job_dict.get('job_id')))
                        job_dict = place_order(job_dict, api_client)
                        db_helper.update_job(job_dict)
                    else:
                        log.info('  -No new data for job {0}'.format(job_dict.get('job_id')))

                else: # job_dict.get('order_placed')
                    # Check if new data is ready to download
                    log.info('Checking if order {0} is ready for download...'.format(job_dict.get('order_id')))
                    order_status = check_order_status(job_dict, api_client)
                    #Added a 's' at the end of the key 'download' because the output (dictionary) from the API have changed
                    if order_status.get('downloads') is not None:
                        order_ready = True
                    else:
                        order_ready = False

                    if order_ready:
                        log.info('Order {0} is ready for download'.format(job_dict.get('order_id')))
                        job_dict = download_order(job_dict, order_status, api_client)
                        trigger_pipeline(job_dict)

                        # Download complete, remove order placed and start watching
                        job_dict['order_placed'] = 0
                    else:
                        log.info('Order {0} is not ready for download'.format(job_dict.get('order_id')))
            else:
                log.info('Job inactive...')
                pass

            if job_dict.get('retrigger'):
                # Rerun the ODV-to-DwC pipeline if there are downloaded
                # files available to use.
                trigger_pipeline(job_dict)

                # Job triggered, turn it off now. 
                job_dict['retrigger'] = 0

            # All the work done, keep the job_dict up to date in the DB
            db_helper.update_job(job_dict)
            log.info('=====================')

        except Exception as e:
            log.error('Job Error: {0}'.format(e)) 

    log.debug('Jobs Summary:')
    log.debug(jobs)


def main(args):
    '''
    Setup logging, and args, then "do_work"
    '''
    logging.basicConfig(
        stream=sys.stdout,
        format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
        level=getattr(logging, args.loglevel))

    log.setLevel(getattr(logging, args.loglevel))
    log.info('ARGS: {0}'.format(ARGS)) 

    db_helper.ensure_db()

    scheduler = BlockingScheduler()
    scheduler.add_job(lambda: check_status(), 'interval', minutes=int(os.getenv('RECHECK_MINS')))
    try: 
        check_status()
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
    log.info('Script Ended...') 

if __name__ == "__main__":
    '''
    This takes the command line args and passes them to the 'main' function
    '''
    PARSER = argparse.ArgumentParser(
        description='Luigi Pipeline Trigger Service')
    PARSER.add_argument(
        '-ll', '--loglevel', default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help="Set log level for service (%s)" % 'INFO')
    ARGS = PARSER.parse_args()
    try:
        main(ARGS)
    except KeyboardInterrupt:
        log.warning('Keyboard Interrupt. Exiting...')
        os._exit(0)
    except Exception as error:
        log.error('Other exception. Exiting with code 1...')
        log.error(traceback.format_exc())
        log.error(error)
        os._exit(1)
