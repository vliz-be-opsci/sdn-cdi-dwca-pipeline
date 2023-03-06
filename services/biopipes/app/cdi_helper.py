'''
Set of tools to talk to the Seadatanet API, run queries and check
whether new data exists.
'''

import os
import logging
import datetime
import requests
# Special SDN python library auto generated from OpenAPI standards
import cdi_sdn_py 
# from sdnclient.api import InfoApi
from cdi_sdn_py.api import security_api 
from cdi_sdn_py.model.login import Login
from cdi_sdn_py.model.errors_return import ErrorsReturn
from cdi_sdn_py.model.login_post200_response import LoginPost200Response
from cdi_sdn_py.api import metadata_api 
from cdi_sdn_py.model.metadata_query_return import MetadataQueryReturn
from cdi_sdn_py.model.metadata_query import MetadataQuery
from cdi_sdn_py.model.order_query_return import OrderQueryReturn
from cdi_sdn_py.model.order_query import OrderQuery
from cdi_sdn_py.model.order_query_query_fields import OrderQueryQueryFields
from cdi_sdn_py.api import orders_api
from cdi_sdn_py.model.order_details_return import OrderDetailsReturn

log = logging.getLogger('cdi_helper') 

#Python class to handle the connection to the API and to reconnect when auth fails
class SeadatanetAPI:
    def __init__(self, retry = True, retries = 3, retry_interval = 5, host = "https://seadatanet-buffer5.maris.nl/api_v5.1"):
        self.retry = retry
        self.retries = retries
        self.retry_interval = retry_interval
        self.host = host

        self.get_token()
        self.configuration = cdi_sdn_py.Configuration(host = self.host,
                                                      access_token = self.token)
        
    def get_token(self):
        '''
        Get the token from the Seadatanet API with username and password stored in env variables
        '''
        with cdi_sdn_py.ApiClient() as api_client:
            # Create an instance of the API class
            api_instance = security_api.SecurityApi(api_client)
            login = Login(
                username=os.getenv('USERNAME', ''),
                password=os.getenv('PASSWORD', ''),
            ) 
            try:
                # Normal login
                api_response = api_instance.login_post(login)
            except cdi_sdn_py.ApiException as e:
                log.warning("Exception when calling SecurityApi->login_post: %s\n" % e)
        self.token = api_response['token']
        return self.token

    def get_last_update(self,job_dict):
        '''
        Return the most recent update timestamp for a dataset query dict
        '''
        query = job_dict.get('query')
        # free_text = str(query.get('free_search'))
        # originator_edmo = str(query.get('originator_edmo'))
        
        # TODOne: add other query fields. Currently only free_text and originator_edmo are used
        # the api class doesn't seem to handle None values correctly, so the param must be 
        # completely absent.
        # query_fields=OrderQueryQueryFields(free_search=free_text,
        #                                            originator_edmo=originator_edmo))
        # from https://stackoverflow.com/questions/23484091/pass-kwargs-if-not-none


        with cdi_sdn_py.ApiClient(self.configuration) as api_client:
            # Create an instance of the API class
            api_instance = metadata_api.MetadataApi(api_client)
            metadata_query = MetadataQuery( 
                pagination_sort="last_update",
                pagination_sort_type="desc",
                pagination_page=1,
                pagination_count=0,
                query_fields=OrderQueryQueryFields(**{key:value for (key,value) in query.items() if value is not None}))   
            try:
                # Make a query and get the metadata back
                log.debug("Making query: %s" % metadata_query)
                api_response = api_instance.metadata_query_post(metadata_query,_check_return_type = False )
                log.debug('API response %s', api_response)
                if api_response['records_found'] == 0:
                    log.warning('No records found for query: %s' % query)
                else:  
                    first_row = next(iter(api_response.get('result')),None)
                    last_update = first_row.get('last_update',None)
                    log.debug('First row %s', first_row)
                    if last_update is not None:
                        last_update = datetime.datetime.strptime(last_update, '%Y-%m-%dT%H:%M:%S.%fZ')
                    return last_update
            except cdi_sdn_py.ApiException as e:
                log.warning('Issue with API query: {0}'.format(e))
                return None
           
    def get_order(self,job_dict):
        '''
        Get details on the order and the download URL if available
        '''
        order_number = job_dict.get('order_id')
        
        with cdi_sdn_py.ApiClient(self.configuration) as api_client:
            # Create an instance of the API class
            api_instance = orders_api.OrdersApi(api_client) 

            # example passing only required values which don't have defaults set
            try:
                # Find order by Ordernumber
                api_response = api_instance.order_order_number_get(order_number, _check_return_type = False )
                log.debug('API response %s', api_response)
                return api_response
            except cdi_sdn_py.ApiException as e:
                print("Exception when calling OrdersApi->order_order_number_get: %s\n" % e)
                return None

    def place_order(self, job_dict):
        query = job_dict.get('query')
        # free_text = str(query.get('free_search'))
        # originator_edmo = str(query.get('originator_edmo'))

        user_order_name = str(job_dict.get('name','auto_order'))
        motivation = str(job_dict.get('motivation','dataset update'))
        data_format_l24 = str(job_dict.get('data_format_l24','bodv'))

        with cdi_sdn_py.ApiClient(self.configuration) as api_client:
            # Create an instance of the API class
            api_instance = orders_api.OrdersApi(api_client)
            order_query = OrderQuery(
                user_order_name=user_order_name,
                motivation=motivation,
                data_format_l24=data_format_l24,
                query_fields=OrderQueryQueryFields(**{key:value for (key,value) in query.items() if value is not None}))  
            try:
                # Make an order by query
                log.debug("Placing Order: %s" % order_query)
                api_response = api_instance.order_query_post(order_query,  _check_return_type = False )
                log.debug(api_response)
                return api_response
            except cdi_sdn_py.ApiException as e:
                print("Exception when calling OrdersApi->order_query_post: %s\n" % e)
                return None

    def download_order(self, url):
        '''
        This doesn't use the OpenAPI classes but makes use of the Bearer Token
        to do a simple requests.get(). This is because the OpenAPI classes don't
        seem to work with the Bearer Token.

        response = requests.get('https://www.example.com/', auth=BearerAuth('3pVzwec1Gs1m'))

        '''
        log.debug(f'Downloading {url}...')
        headers = {"Authorization": f"Bearer {self.token}"}
        try:
            response = requests.get(url,headers=headers)
            return response
        except Exception as e:
            log.warning('Issue with order download: {0}'.format(e))
            return None
        