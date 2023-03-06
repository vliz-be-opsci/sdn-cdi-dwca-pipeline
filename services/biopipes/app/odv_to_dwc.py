# Std
import zipfile
import pathlib
import os
import re
import hashlib

# NonStd
import numpy as np
import pandas as pd
import geopy.distance
import itertools
import requests
import json
import logging
import uuid
from functools import cache
from itertools import chain

# Custom
import pyodv

log = logging.getLogger('odv_to_dwc')


# Below are the dictionaries that map the ODV terms to the DwC terms. 
# The columns in the value list are matched by order. Example:
#   'institutionCode':['Originator','institutionCode','EDMO_code']
#    The DwC column "institutionCode" is created with data pulled from "Originator", if that column
#    doesn't exist then data is pulled from "institutionCode", if that doesn't exist it matches to
#    "EDMO_Code". Some of these columns are created in the "Create new columns" function.

event_mapping = {
        'eventID':['eventID'],
        'eventDate':['yyyy-mm-ddThh:mm:ss.sss', 'YYYY-MM-DDThh:mm:ss.sss'],
        'parentEventID':['parentEventID'],
        'decimalLatitude':['Latitude'],
        'decimalLongitude':['Longitude'],
        'institutionCode':['Originator','institutionCode','EDMO_code'],
        'datasetName':['EDMED references'],
        'maximumDepthInMeters':['MaximumObservationDepth'],
        'minimumDepthInMeters':['MinimumObservationDepth'],
        'coordinateUncertaintyInMeters':['CoordinateUncertaintyInMeters'],
        'footprintWKT':['footprint_wkt'], # How to put in a single value into table? Shouldn't this be metadata?
        'type':[None],
        'parenEventID':[None],
        'dataGeneralizations':[None],
        'eventRemarks':[None],
        'samplingProtocol':['Samplingprotocol', 'SamplingProtocol'],
        'locationID':['Station'],
        'locality':['locality','Station name','Alternative station name'],
        'locationRemarks':[None],
        }

occ_mapping = {
            'eventID': ['eventID'],
            'occurrenceID': ['occurrenceID'],
            'basisOfRecord': ['basisOfRecord'],
            'occurrenceStatus': ['occurrenceStatus'], # Hardcoded? 
            'scientificName': ['ScientificName'], # Must have trailing space
            'scientificNameID': ['ScientificNameID'], 
            }   

meta_event_mapping = {
        'eventID':['eventID'],
        'eventDate':[None],
        'parentEventID':[None],
        'decimalLatitude':['Latitude 1'],
        'decimalLongitude':['Longitude 1'],
        'institutionCode':['Originator','institutionCode','EDMO_code'],
        'datasetName':['EDMED references'],
        'maximumDepthInMeters':[None],
        'minimumDepthInMeters':[None],
        'coordinateUncertaintyInMeters':[None],
        'footprintWKT':[None], # How to put in a single value into table? Shouldn't this be metadata?
        'type':[None],
        'parenEventID':[None],
        'dataGeneralizations':[None],
        'eventRemarks':[None],
        'samplingProtocol':[None],
        'locationID':['Station name'],
        'locality':['locality','Station name','Alternative station name'],
        'locationRemarks':[None],
        }

def odv_to_dwc(job_dict):
    '''
    The actual function that does the conversions from 
    the ODV zip into the DwC
    '''
    odv_zip = job_dict.get('last_data_file')
    odv_meta = job_dict.get('last_data_file')
    if (odv_zip is None) or (odv_meta is None):
        log.warning('No ODV or meta file provided...')
        return None

    log.info(f'===Converting {odv_zip} to DwC===')
    folder_dict = create_folder_structure(odv_zip)
    unzip(folder_dict)
    parsed_df, odv_list = parse_odv(folder_dict)

    # Create new IDs
    # ==================
    log.debug('   -Building WKT...')
    applied_df = parsed_df.apply(lambda row: create_wkt(row), axis='columns', result_type='expand')
    applied_df = applied_df.rename(columns={0: 'CoordinateUncertaintyInMeters', 1: 'footprint_wkt'})
    parsed_df  = pd.concat([parsed_df, applied_df], axis='columns')
    parsed_df = rename_odv_columns(parsed_df)
    parsed_df = create_new_columns(parsed_df)
    log.debug('   -Creating Event and Occurrence IDs...')
    df_id = parsed_df.apply(lambda row: create_IDs(row), axis='columns', result_type='expand')
    df_id = df_id.rename(columns={0: 'eventID', 1: 'occurrenceID', 2: 'parentEventID'})
    parsed_df  = pd.concat([parsed_df, df_id], axis='columns')

    # Create EventCore File
    dwc_event = odv_dwc_mapping(parsed_df, event_mapping) 
    dwc_meta_event = meta_event_gen(folder_dict)
    dwc_event = pd.concat([dwc_meta_event,dwc_event])

    # Create EMOF from Occ data
    params = convert_params_to_df(odv_list)
    event_dwc_emof = emof_gen(parsed_df, params)

    # Create EMOF from Event data
    meta_dwc_emof = meta_emof_gen(folder_dict)
    dwc_emof = pd.concat([meta_dwc_emof,event_dwc_emof])
    dwc_emof = emof_cleanup(dwc_emof, occ_mapping, event_mapping)
    
    # Check if there are duplicate ID's
    good_ids = check_IDs(dwc_event, ['eventID'])  
    if good_ids:
        pass
    else: 
        log.warning('Possible issues with duplicate event_ids')
    
    # Create OccCore File
    dwc_occ = odv_dwc_mapping(parsed_df, occ_mapping)
    good_ids = check_IDs(dwc_occ, ['occurrenceID'])  
    if good_ids:
        pass
    else: 
        log.warning('Possible issues with duplicate Occurrence IDs')

    # Write files:
    dwc_event.to_csv(folder_dict.get('event_path'), index = False)
    dwc_occ.to_csv(folder_dict.get('occ_path'), index = False)
    dwc_emof.to_csv(folder_dict.get('emof_path'), index = False)
    parsed_df.to_csv(folder_dict.get('all_data_path'), index = False)

    log.info(f'===Finished converting {odv_zip} to DwC===')
    return parsed_df

 
def create_new_columns(parsed_df):
    parsed_df['occurrenceStatus'] = parsed_df.apply(find_occurrenceStatus, axis=1)
    parsed_df['basisOfRecord'] = parsed_df.apply(find_basisOfRecord, axis='columns')
    if 'EDMO_code' in parsed_df.columns:
        parsed_df['institutionCode'] = 'EDMO:' + parsed_df['EDMO_code'].astype(str)
    if ('Station name' in parsed_df.columns) and ('Alternative station name' in parsed_df.columns):
        parsed_df['locality'] = parsed_df['Station name'].astype(str) + '_' + parsed_df['Alternative station name'].astype(str)
    return parsed_df

def create_folder_structure(odv_zip):
    '''
    Create a directory structure next to the odv_zip file:
    > <some-file>.zip
    > ./meta.zip
        
    > ./<some-file>/unzip
    > ./<some-file>/unzip/odv1.csv, odv2.csv ...

    > ./<some-file>/dwc
    > ./<some-file>/dwc/occ.csv
    > ./<some-file>/dwc/event.csv
    > ./<some-file>/dwc/emof.csv
    '''
    log.debug(f'Creating folder structure for {odv_zip}...')
    zipped_path = pathlib.Path(odv_zip).parent 
    meta_zipped_path = zipped_path.joinpath('meta.zip')

    unzip_folder = pathlib.Path(zipped_path).joinpath('unzip')
    unzip_folder.mkdir(parents=True, exist_ok=True)

    meta_file = pathlib.Path(odv_zip).stem + '.csv'
    meta_path = unzip_folder.joinpath(meta_file)
    
    dwc_folder = pathlib.Path(zipped_path).joinpath('dwc')
    dwc_folder.mkdir(parents=True, exist_ok=True)
    
    event_file = pathlib.Path(zipped_path).joinpath('dwc').joinpath('event.csv')
    occ_file = pathlib.Path(zipped_path).joinpath('dwc').joinpath('occ.csv')
    emof_file = pathlib.Path(zipped_path).joinpath('dwc').joinpath('emof.csv')
    all_file = pathlib.Path(zipped_path).joinpath('dwc').joinpath('all.csv')

    folder_dict = {'odv_zip': odv_zip,
                   'meta_zip': meta_zipped_path,
                   'unzip_folder': unzip_folder,
                   'dwc_path': dwc_folder,
                   'meta_path': meta_path,
                   'occ_path': occ_file,
                   'emof_path': emof_file,
                   'event_path': event_file,
                   'all_data_path': all_file}

    return folder_dict

def unzip(folder_dict):
    '''
    Unzips the ODV file into the unzip folder
    '''
    log.debug(f"Unzipping file {folder_dict.get('odv_zip')} into {folder_dict.get('unzip_folder')}...")
    with zipfile.ZipFile(folder_dict.get('odv_zip'), 'r') as zip_ref:
        zip_ref.extractall(folder_dict.get('unzip_folder'))
    
    log.debug(f"Unzipping Metadata file {folder_dict.get('meta_zip')} into {folder_dict.get('unzip_folder')}...")
    with zipfile.ZipFile(folder_dict.get('meta_zip'), 'r') as zip_ref:
        zip_ref.extractall(folder_dict.get('unzip_folder'))

    return 

def parse_odv(folder_dict):
    '''
    Parse all the ODV files in the unzipped path into 
    a single data object.
    '''
    unzipped_path = folder_dict.get('unzip_folder')
    meta_path = folder_dict.get('meta_path')

    log.debug(f'Parsing files in {unzipped_path}...')
    config = {'occurrenceStatus_hardcode': 'present'}

    odv_list  = []
    df_list = [] 
    ref_list = []
    for filename in os.listdir(unzipped_path):
        
        f = os.path.join(unzipped_path, filename)
        # checking if it is a file
        if os.path.isfile(f):
            try:
                log.debug(f'===== {f} =====')
                parsed_file = pyodv.ODV_Struct(f) 
                odv_list.append(parsed_file) 
                ref_list.append(parsed_file.refs[0])
                this_df = pd.concat([parsed_file.df_data, parsed_file.df_var],axis=1)
                this_df['scope'] = parsed_file.refs[0]['@sdn:scope'].split(':')[-1]
                this_df['defined_by'] = parsed_file.refs[0]['@xlink:href'] 
                df_list.append(this_df)
            except Exception as err:
                log.debug(err)

    metadata_path =  meta_path
    try:
        log.debug(f'Reading metadata file: {metadata_path}...')
        metadata_df = pd.read_csv(metadata_path)
        metadata_df['LOCAL_CDI_ID_split'] = metadata_df['LOCAL_CDI_ID'].str.split(pat="/").str[0]
    except:
        log.warning('Problem with reading metadata file!')
        metadata_df = pd.DataFrame()

    merged_df = pd.concat(df_list, axis=0) 
    merged_df = merged_df.join(metadata_df.set_index('LOCAL_CDI_ID_split'), on='LOCAL_CDI_ID', how='left', rsuffix = '_meta')
    merged_df.reset_index(level=None, drop=True, inplace = True)
    return merged_df, odv_list

def create_IDs(row):
    '''
    Create EventID and OccurrenceID. 
    Both are a concatenation of the other columns. They also become the columns to join on in the Occurrence Table
    and Event table. 
    '''
    # ==== Event ID ====
    eventID_columns = ['LOCAL_CDI_ID', 
                        'Station', 
                        'yyyy-mm-ddThh:mm:ss.sss',
                        'YYYY-MM-DDThh:mm:ss.sss', 
                        'Samplingprotocol', 
                        'SamplingProtocol',
                        'maximumDepthInMeters',
                        'MaximumObservationDepth',
                        'minimumDepthInMeters',
                        'MinimumObservationDepth']

    new_col = []
    for col in eventID_columns:
        if col in row.index:
            new_col.append(str(row[col]))
    pattern = re.compile(r'\s+')
    long_eventID = re.sub(pattern, '', '_'.join(new_col)) 
    # Create hash of long_eventID since the downstream tools can't handle eventID's longer than 255 chars
    eventID = hashlib.sha1(long_eventID.encode("UTF-8")).hexdigest()[:20] 
    

    # ==== Ahpia ID ====
    try:  
        scinameID_col = [i for i in row.index if i.startswith('ScientificNameID')] 
        sciname_col = [i for i in row.index if i.startswith('ScientificName')] 
        if pd.isnull(row[scinameID_col[0]]):
            aphia_id = row[sciname_col[0]].replace(' ','_')
        else:
            # Find the AphiaID from the ScinameID Column
            aphia_id = re.findall(r"\d{1,}$", row[scinameID_col[0]])[0]
    except Exception as err:
        log.error('Failed to find AphiaID...')
        print(err)

    # ==== occ ID ====
    subsamples = [i for i in row.index if i.startswith('SubsampleID')] 
    sample = [i for i in row.index if i.startswith('SampleID')] 
    # get 8 chars from the hashed sci-name col
    sciname_str = str(row[sciname_col][0])
    hash = hashlib.sha1(sciname_str.encode("UTF-8")).hexdigest()[:10]

    if len(subsamples) == 0:
        occurrenceID = str(row[sample[0]]) + '_' + str(aphia_id) + '_' + str(hash)
    else:
        occurrenceID = str(row[sample[0]]) + '_' + str(row[subsamples[0]]) + '_' + str(aphia_id) + '_' + str(hash)
    
    # ==== Parent Event ID ====
    parentEventID = row['LOCAL_CDI_ID']
    
    return eventID, occurrenceID, parentEventID

def check_IDs(dff, id_col):
    '''
    Check for duplicate ID's, if there are then figure out why and rerun with a wider event_columns list.
    Currently there are problematic duplicate event IDs but it's likely that this problem could exist in the 
    occurrence table too. 
    '''
    xx = dff[dff.duplicated(keep=False, subset=id_col)] 
    if len(xx) == 0:
        # No duplicates! Good news!
        return True
    else:
        return False

def create_wkt(row):
    '''
    Create WKT from the dataset for each row
    Uses Latitude 1,Latitude 2,Longitude 1,Longitude 2 
    from metadata. Must return uncertainty in meters. 
    '''
    
    try:
        max_lat = max(row['Latitude 1'], row['Latitude 2'])
        min_lat = min(row['Latitude 1'], row['Latitude 2'])
        max_lon = max(row['Longitude 1'], row['Longitude 2'])
        min_lon = min(row['Longitude 1'], row['Longitude 2'])
        bounding_wkt = f"POLYGON (({min_lon} {min_lat}, {min_lon} {max_lat}, {max_lon} {max_lat}, {max_lon} {min_lat}, {min_lon} {min_lat}))"
        
        lat_c = (max_lat + min_lat)/2
        lon_c = (max_lon + min_lon)/2
        
        coord_uncertainty = geopy.distance.geodesic((min_lon, min_lat), (lon_c, lat_c)).m 
          
    except Exception as err:
        print(err)
        bounding_wkt = None
        coord_uncertainty = None
    return coord_uncertainty, bounding_wkt


def find_occurrenceStatus(row):
    '''
    Do some kind of occurenceStatus thing.
    '''
    if 'PresenceOrAbsence' in row.index:
        if row['PresenceOrAbsence']:
            return 'present'
        else:
            return 'absent'
    else:
        return 'present'
            


def find_basisOfRecord(row):
    '''
    Do something to create BasisOfRecord. 
    '''
    return 'MaterialSample'

@cache
def get_units_from_nerc(measurementUnitID):
    '''
    Get the english units from the nerc vocab
    server. 
    ''' 
    log.debug('   -Downloading vocab from NERC')
    # nerc_uri = row['measurementUnitID']
    nerc_uri = measurementUnitID
    xx = requests.get(nerc_uri + '?_profile=nvs&_mediatype=application/ld+json')
    alt_labels = json.loads(xx.content)['altLabel']
    if isinstance(alt_labels, list):
        for x in alt_labels:
            if isinstance(x, str):
                alt_label = x
    elif isinstance(alt_labels, str):
        alt_label = alt_labels
    else:
        alt_label = 'Unknown'
        log.warning(f'Failure to handle Label: {alt_labels}')
    return alt_label

def convert_params_to_df(odv_list):    
    '''
    Convert the ODV Params into Nerc URI's in order to get them 
    closer towards EMOF. 
    '''
    log.debug('   -Converting ODV params to dataframe...')
    odv_params_list = []
    for odv_obj in odv_list:
        # Loop through ODV items and get the symantic params per file source.     
        xx = [x | odv_obj.refs[0] for x in odv_obj.params]
        odv_params_list.append(xx)

    params_df =  pd.DataFrame(list(chain.from_iterable(odv_params_list))).drop_duplicates()
    # Create a "scope" variable which pretty much allows all other dataframes to join based on file source. 
    params_df['scope'] = params_df['@sdn:scope'].str.split(':').str[-1]
    params_df['measurementType'] = params_df['subject'].apply(lambda x: x.split(':')[-1]) 
    params_df['measurementUnitID'] = params_df['units'].apply(lambda x: f"https://vocab.nerc.ac.uk/collection/P06/current/{x.split('::')[-1]}/")
    params_df['measurementTypeID'] = params_df['object'].apply(lambda x: f"https://vocab.nerc.ac.uk/collection/P01/current/{x.split('::')[-1]}/")
    params_df['measurementUnit'] = params_df.apply(lambda x: get_units_from_nerc(x['measurementUnitID']), axis=1)

    params_df['measurementType'] = params_df['subject'].apply(lambda x: x.split(':')[-1]) 
    params_df['measurementUnitID'] = params_df['units'].apply(lambda x: f"https://vocab.nerc.ac.uk/collection/P06/current/{x.split('::')[-1]}/")
    params_df['measurementTypeID'] = params_df['object'].apply(lambda x: f"https://vocab.nerc.ac.uk/collection/P01/current/{x.split('::')[-1]}/")
    params_df['measurementUnit'] = params_df.apply(lambda x: get_units_from_nerc(x['measurementUnitID']), axis=1)
    
    return params_df

def rename_odv_columns(df):
    '''
    Rename the ODV dataframe by
      - Strip anything between square brackets: 'Latitude [degrees_north]' >> 'Latitude'
      - Remove the ':INDEXED_TEXT' text in ODV column names. 
      - Run Mapping from ODV names to DwC names: 'Longitude [degrees_east]' >> Longitude >> decimalLongitude
    Mapping should be from a list of possible ODV terms to a single DwC term. Gonna be tough to do...
    '''
    log.debug('   -Renaming ODV column names...')
    old_columns = df.columns
    new_columns = [re.sub(r'\s\[[^>]+?\]|\b:INDEXED_TEXT\b', '', x) or x for x in df.columns]
    rename_dict = dict(zip(old_columns, new_columns))
    df = df.rename(columns = rename_dict)
    return  df

def odv_dwc_mapping(df, map_dict):
    '''
    Take mapping dict and create a new DF that has columns with <map_dict key> as names taken from 
    old_df <map_dict values>.
    
    dwc_name : [odv_colname1, odv_colname2 ...] 
    '''
    log.debug('   -Mapping column names...')
        
    dwc_col_list = []
    rename_dict = {}
    for dwc_colname, odv_colname_list in map_dict.items():
        for odv_colname in odv_colname_list:
            if odv_colname is not None:
                this_column = ''
                try: 
                    this_column = odv_colname
                    
                    dwc_col = df[this_column]
                    dwc_col.name = dwc_colname
                    rename_dict[odv_colname] = dwc_colname
                    dwc_col_list.append(dwc_col)
                    #Take the first match and move on...
                    break
                except Exception as err:
                    log.debug(f'KeyError: Changing {this_column} column to {dwc_colname} but failed.')
                    
    mapped_df = pd.concat(dwc_col_list, axis=1)
    mapped_df = mapped_df.rename(columns=rename_dict)
    mapped_df = mapped_df.drop_duplicates() 
    return mapped_df

def emof_gen(in_df, in_emof_df):
    '''
    Create EMOF table from the emof_params. 
    Loops through each 
    '''
    log.info('   -Generating EMOF file...')
    log.info('   -Size of EMOF_DF: '+str(len(in_emof_df)))
    # out_df = pd.DataFrame()
    emof_subsets = []
    for index, row in in_emof_df.iterrows():
        scope = row['scope']
        measurementType = row['measurementType']
        if len(in_df[measurementType].shape) > 1:
            df_coalesce = in_df[measurementType]
            df_coalesce["_"] = df_coalesce.bfill(axis=1).iloc[:, 0]
            in_df = in_df.rename(columns={measurementType: f"_{measurementType}"})
            in_df[measurementType] = df_coalesce["_"]
        df_subset = in_df[(in_df[measurementType].notna()) & (in_df['scope'] == scope)][['eventID','occurrenceID',measurementType]]
        if not df_subset.empty: 
            emof_subset = df_subset.copy()
            emof_subset['measurementID'] = [uuid.uuid4() for _ in range(len(emof_subset.index))]
            emof_subset['measurementValue'] = emof_subset[measurementType]
            emof_subset['measurementValueID'] = None
            emof_subset['measurementType'] = measurementType
            emof_subset['measurementTypeID'] = row['measurementTypeID']
            emof_subset['measurementUnit'] = row['measurementUnit']
            emof_subset['measurementUnitID'] = row['measurementUnitID']
            emof_subset = emof_subset[['eventID',
                                       'occurrenceID',
                                       'measurementID',
                                       'measurementValue',
                                       'measurementValueID',
                                       'measurementType',
                                       'measurementTypeID',
                                       'measurementUnit',
                                       'measurementUnitID']]
            emof_subsets.append(emof_subset)

            if 'instrument' in row.index and pd.notna(row.instrument):
                # This row has tool information           
                emof_tool_subset = df_subset.copy()
                instrument_uri = f"http://vocab.nerc.ac.uk/collection/L22/current/{row['instrument'].split('::')[-1]}/"
                emof_tool_subset['measurementID'] = None
                emof_tool_subset['measurementType'] = 'instrument'
                emof_tool_subset['measurementTypeID'] = 'http://vocab.nerc.ac.uk/collection/L19/current/SDNKG01/'
                emof_tool_subset['measurementValue'] = get_units_from_nerc(instrument_uri)
                emof_tool_subset['measurementValueID'] = instrument_uri
                emof_tool_subset['measurementUnit'] = 'Dmnless'
                emof_tool_subset['measurementUnitID'] = 'https://vocab.nerc.ac.uk/collection/P06/current/UUUU/'
                emof_tool_subset['occurrenceID'] = None
                emof_tool_subset = emof_tool_subset[['eventID',
                                                     'occurrenceID',
                                                     'measurementID',
                                                     'measurementValue',
                                                     'measurementValueID',
                                                     'measurementType',
                                                     'measurementTypeID',
                                                     'measurementUnit',
                                                     'measurementUnitID']]
                emof_subsets.append(emof_tool_subset)
    # out_df = out_df[['eventID','occurrenceID','measurementID','measurementValue','measurementValueID','measurementType','measurementTypeID','measurementUnit','measurementUnitID']]
    log.debug('     -Loop done: dropping dupes in EMOF file...')
    emod_df = pd.concat(emof_subsets)
    emod_df = emod_df.drop_duplicates()
    return emod_df

def emof_cleanup(emof_df, occ_mapping, event_mapping):
    '''
    Any measurementType that is also in the Occ or Event tables must be ignored. Also drop rows
    where the measurementValue is NaN. 
    '''
        
    log.debug('   -Cleaning EMOF file...')
    mapping_list = []
    z = {**occ_mapping, **event_mapping}
    for key, value in z.items():
        mapping_list.append(key.lower())
        for item in value:
            mapping_list.append(str(item).lower())

    # Drop where measurementValue is NaN
    emof_df = emof_df.drop(emof_df[emof_df.measurementValue.isna()].index, axis=1)
    # Drop where measurementType is already in Occ or Event
    emof_df = emof_df[~emof_df['measurementType'].str.lower().isin(mapping_list)]
    emof_df = emof_df.reset_index(drop=True)
    return emof_df

def meta_event_gen(folder_dict):
    '''
    Grab the metadata file and create an event core file from it
    '''
    log.debug('   -Converting metafile into params dataframe...')
    meta_df =  pd.read_csv(folder_dict.get('meta_path'), dtype={'Depth reference': 'object'})
    meta_df['eventID'] = meta_df['LOCAL_CDI_ID'].str.split(pat="/").str[0]
    meta_df = create_new_columns(meta_df)
    meta_events = odv_dwc_mapping(meta_df, meta_event_mapping)
    return meta_events

def meta_emof_gen(folder_dict): 
    '''
    Convert the ODV Metadata file into an EMOF starter file. 
    Much of this is hard coded since there isn't too much semantic info 
    available on WHAT the CDI meta csv file actually means. 
    '''
    log.debug('   -Converting metafile into emof dataframe...')
    

    meta_df =  pd.read_csv(folder_dict.get('meta_path'))
    # meta_params = convert_meta_params_to_df(meta_df)

    template_meta_emofs = [{'measurementType': 'Minimum instrument depth (m)',
                          'measurementTypeID': 'https://vocab.nerc.ac.uk/collection/P01/current/MINWDIST/',
                          'measurementUnit': 'm',
                          'measurementUnitID': 'http://vocab.nerc.ac.uk/collection/P06/current/ULAA/',
                          },
                         {'measurementType': 'Maximum instrument depth (m)',
                          'measurementTypeID': 'http://vocab.nerc.ac.uk/collection/P01/current/MAXWDIST/',
                          'measurementUnit': 'm',
                          'measurementUnitID': 'http://vocab.nerc.ac.uk/collection/P06/current/ULAA/',
                          },
                         {'measurementType': 'Water depth (m)',
                          'measurementTypeID': '',
                          'measurementMethod': '', # Use Depth reference here
                          'measurementUnit': 'm',
                          'measurementUnitID': 'http://vocab.nerc.ac.uk/collection/P06/current/ULAA/',
                          },
                         {'measurementType': 'Instrument / gear type',
                          'measurementTypeID': 'http://vocab.nerc.ac.uk/collection/L05/current/',
                          'measurementUnit': 'NA',
                          'measurementUnitID': 'https://vocab.nerc.ac.uk/collection/P06/current/XXXX/',
                          },
                         {'measurementType': 'Platform type',
                          'measurementTypeID': 'http://vocab.nerc.ac.uk/collection/W06/current/CLSS0001/', 
                          'measurementValueID' : 'http://vocab.nerc.ac.uk/collection/L06/current/0/',
                          'measurementUnit': 'NA',
                          'measurementUnitID': 'https://vocab.nerc.ac.uk/collection/P06/current/XXXX/',
                          }]
    template_df = pd.DataFrame(template_meta_emofs)
    in_df = meta_df

    emof_subsets = []
    for index, row in template_df.iterrows():
        measurementType = row['measurementType'] 
        df_subset = in_df[(in_df[measurementType].notna())] 

        if not df_subset.empty:
            emof_subset = df_subset.copy()
            emof_subset['eventID'] = emof_subset['LOCAL_CDI_ID'].str.split(pat="/").str[0]
            emof_subset['occurrenceID'] = None
            emof_subset['measurementID'] = None
            emof_subset['measurementTypeID'] = None
            emof_subset['measurementValue'] = emof_subset[measurementType]
            emof_subset['measurementValueID'] = None
            emof_subset['measurementMethod'] = None

            if measurementType == 'Instrument / gear type':
                gear_number = emof_subset[measurementType].str.extract(r'\((\d*?)\)')
                emof_subset['measurementTypeID'] = row['measurementTypeID'] +  gear_number

            elif measurementType == 'Platform type':
                platform_number = emof_subset[measurementType].str.extract(r'\((\d*?)\)') 
                emof_subset['measurementTypeID'] = row['measurementTypeID'] +  platform_number
                
            elif measurementType == 'Water depth (m)': 
                emof_subset['measurementMethod'] = emof_subset['Depth reference']
                
            else:
                emof_subset['measurementTypeID'] = ''

            emof_subset['measurementType'] = row['measurementType']
            emof_subset['measurementUnit'] = row['measurementUnit']
            emof_subset['measurementUnitID'] = row['measurementUnitID']
            emof_subset = emof_subset[['eventID',
                                       'occurrenceID',
                                       'measurementID',
                                       'measurementValue',
                                       'measurementValueID',
                                       'measurementType',
                                       'measurementTypeID',
                                       'measurementUnit',
                                       'measurementUnitID']]
            emof_subsets.append(emof_subset)
    meta_params_df = pd.concat(emof_subsets) 
    return meta_params_df
