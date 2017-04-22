import argparse
from argparse import RawDescriptionHelpFormatter
import os
import sys
from os.path import *
from sys import stdout
import time
import csv
import grequests
from requests import codes as status_codes
import arcpy
from datetime import datetime

# PARAMETERS FROM USER: INPUTFILE, COLUMNBATCH

parser = argparse.ArgumentParser(description=u"""
Given an input csv and an output file geodatabase, retrieve as many records from the WDPA esri feature service
as can be found.
Syntax: python retrieve_wdpa.py "C:/Users/kfisher/Documents/utilities/retrieve_wdpa/PAs_WCS_Helps_Manage/test.csv" 
""",
                                 formatter_class=RawDescriptionHelpFormatter)
parser.add_argument('inputfile',
                    help=u'Absolute path to input csv file to be parsed.')
parser.add_argument('-o', '--outgdb',
                    default='C:/Users/kfisher/Documents/utilities/retrieve_wdpa/PAs_WCS_Helps_Manage.gdb',
                    help=u'esri file gdb to which to output results')
parser.add_argument('-u', '--url',
                    default='http://ec2-54-204-216-109.compute-1.amazonaws.com:6080/arcgis/rest/services/wdpa/wdpa/'
                            'MapServer/find?layers=1&returnGeometry=true&f=pjson',
                    # Endau+Rompin
                    help=u'base url of WDPA esri feature service')
args = parser.parse_args()
# hardcoded for now
# sr = 'C:/Users/kfisher/Documents/utilities/retrieve_wdpa/7483.prj'
sr = arcpy.SpatialReference(3857)

SKIP_FIELDS = ['Shape_Length', 'Shape_Area', 'Shape', ]
CONCURRENT_REQUESTS = 2
CONTAINS = True

inputfile = args.inputfile.lstrip().rstrip(' /\\')
d, basefile = split(inputfile)
outgdb = args.outgdb.lstrip().rstrip(' /\\')
base_url = args.url.lstrip().rstrip(' /\\')
if CONTAINS:
    base_url = '%s&contains=true' % base_url
base_url = '%s&searchText=' % base_url
if not isdir(outgdb) or not os.access(outgdb, os.W_OK | os.X_OK):
    stdout.write(u'%s is not a gdb directory writeable by python. Exiting.\n' % outgdb)
    sys.exit()
timestr = time.strftime("%Y%m%d_%H%M%S")
fc = '%s_%s' % (splitext(basefile)[0], timestr)
arcpy.CreateFeatureclass_management(outgdb, fc, 'POLYGON', spatial_reference=sr)
outfc = join(outgdb, fc)
outcsv = join(d, '%s.csv' % fc)
wdpa_cursor = None
fieldnames = [u'search_term']
urls = []

stdout.write('input csv: %s\n' % inputfile)
stdout.write('output fc: %s\n' % outfc)
stdout.write('base wdpa url: %s\n' % base_url)


def create_fields(atts):
    global fieldnames, outfc
    arcpy.AddField_management(outfc, fieldnames[0], 'TEXT')
    for field in atts:
        # stdout.write('field: %s\n' % field)
        field_type = 'TEXT'

        if field not in SKIP_FIELDS:
            if 'area' in field.lower():
                field_type = 'DOUBLE'
            elif field.isdigit():
                field_type = 'LONG'
            if field == 'OBJECTID':
                field = 'wdpa_%s' % field

            arcpy.AddField_management(outfc, field, field_type)
            fieldnames.append(field)


def create_cursor(result):
    global wdpa_cursor, fieldnames
    atts = result.get('attributes', {})
    if len(atts.keys()) > 0:
        # stdout.write('attribs: %s\n' % atts)
        create_fields(atts.keys())
        stdout.write('fieldnames: %s\n' % fieldnames)

        fieldnames.insert(0, 'SHAPE@')
        wdpa_cursor = arcpy.da.InsertCursor(outfc, fieldnames)


def get_val(attribs, field):
    if field == 'wdpa_OBJECTID':
        return attribs['OBJECTID']
    for attrib in attribs:
        if attrib == field:
            return attribs[attrib]
    return None


def get_poly(result_geom):
    poly = None
    parts = []
    if len(result_geom['rings']) > 0:
        for ring in result_geom['rings']:
            part = [arcpy.Point(*coords) for coords in ring]
            parts.append(arcpy.Array(part))
        poly = arcpy.Polygon(arcpy.Array(parts))

    return poly


def add_to_fc(result, searchterm):
    global wdpa_cursor
    # stdout.write('result: %s\n' % result)
    if wdpa_cursor is None:
        create_cursor(result)
    result_attribs = result.get('attributes', {})
    result_geom = result.get('geometry', {})

    if 'rings' in result_geom:
        wdpa_record = []

        poly = get_poly(result_geom)
        for field in fieldnames:
            if field == 'SHAPE@':
                wdpa_record.append(poly)
            elif field == 'search_term':
                wdpa_record.append(searchterm)
            else:
                wdpa_record.append(get_val(result_attribs, field))

        # stdout.write('wdpa_record: %s\n' % wdpa_record)
        wdpa_cursor.insertRow(wdpa_record)


def handle_response(response, *args, **kwargs):
    if response.status_code == status_codes.ok and len(response.content) > 0:
        resp = response.json()
        if 'error' in resp:
            stdout.write('error in 200 response: %s\n' % response.request.url)
            # urls.append(response.request.url)
        else:
            searchterm = response.request.url.split('=')[-1]
            stdout.write('Response [%s]: %s\n' % (len(resp['results']), searchterm))
            # stdout.write('Response: %s\n%s\n' % (response.request.url, resp))
            writer.writerow((searchterm, len(resp['results'])))
            if 'results' in resp:
                try:
                    for r in resp['results']:
                        add_to_fc(r, searchterm)
                except:
                    pass


def exception_handler(request, exception):
    stdout.write('Exception: %s\n%s\n' % (request.url, exception))


def process_requests(urls):
    global session
    rs = [grequests.get(u, session=session, callback=handle_response) for u in urls]

    for r in grequests.imap(rs, size=CONCURRENT_REQUESTS, exception_handler=exception_handler):
        # keep trying until no more service find errors
        # if len(urls) > 0:
        #     stdout.write('%s requests came back 200 but with json errors; requeuing.' % len(urls))
        #     process_requests(urls)
        # stdout.write('Finished: %s\n' % r.request.url)
        pass


# Main script ###

start_time = datetime.now()
session = grequests.Session()
f = open(outcsv, 'wt')
writer = csv.writer(f, lineterminator='\n')
try:
    with open(inputfile, "rb") as f:
        reader = csv.reader(f)
        for i, line in enumerate(reader):
            if len(line) > 0:
                searchterm = line[0]
                url = '%s%s' % (base_url, searchterm)
                # stdout.write('[%s] Querying for %s\n' % (i, searchterm))
                urls.append(url)

    process_requests(urls)
    stdout.write('Elapsed time: %s' % (datetime.now() - start_time))
    if wdpa_cursor is not None:
        del wdpa_cursor

except:
    stdout.write('Cannot read %s. Exiting.\n' % inputfile)
    sys.exit()

f.close()
