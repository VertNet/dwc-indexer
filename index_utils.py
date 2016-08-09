#!/usr/bin/env python

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

__author__ = "John Wieczorek"
__contributors__ = "Aaron Steele, John Wieczorek"
__copyright__ = "Copyright 2016 vertnet.org"
__version__ = "index_utils.py 2016-08-09T13:10+2:00"

import json
import logging
import re
import os
from field_utils import index_fields
from datetime import datetime
from google.appengine.api import namespace_manager
from google.appengine.api import search
from mapreduce import operation as op
from mapreduce import context

IS_DEV = 'Development' in os.environ['SERVER_SOFTWARE']

# The expected header of the input stream. This will not be in the stream, but is defined
# here to define the structure of the stream.
HEADER = index_fields()

def build_search_index(readbuffer):
    """
    Construct a document from a readbuffer and index it.
    """
    # readbuffer should be a tuple from GoogleCloudLineInputReader composed of a
    # tuple of the form ((file_name, offset), line)

    # Get namespace from mapreduce job and set it.
    ctx = context.get()
    params = ctx.mapreduce_spec.mapper.params
    namespace = params['namespace']
    index_name = params['index_name']
    indexdate = datetime.now().strftime('%Y-%m-%d')

    try:
        # Get the row out of the input buffer
        row=readbuffer[1]

        # Create a dictionary from the HEADER and the row
        data = get_rec_dict(dict(zip(HEADER, row.split('\t'))))
#        s =  'Data from %s' % readbuffer[0][0]
#        s += ' offset %s: %s' % (readbuffer[0][0], data)
#        logging.warning('%s' % s)

        # Create an index document from the row dictionary
        doc = index_record(data, indexdate)

        # Store the document in the given index
        index_doc(doc, index_name, namespace)
    except Exception, e:
        logging.error('%s\n%s' % (e, readbuffer))

def get_rec_dict(rec):
    """Returns a dictionary of all fields in rec with non-printing characters removed."""
    val = {}
    for name, value in rec.iteritems():
        if value:
        	# Replace all tabs, vertical tabs, carriage returns, and line feeds 
        	# in field contents with space, then remove leading and trailing spaces.
            val[name] = re.sub('[\v\t\r\n]+', ' ', value).strip(' ')
    return val

    @classmethod
    def initalize(cls, resource):
        namespace = namespace_manager.get_namespace()
        filename = '/gs/vn-indexer/failures-%s-%s.csv' % (namespace, resource)
        log = cls.get_or_insert(key_name=filename, namespace=namespace)
        return log

def index_record(data, indexdate, issue=None):
    """
    Creates a document ready to index from the given input data. This is where the work 
    is done to construct the document.
    """
    keyname, iptrecordid, icode, collectioncode, catalognumber, \
    gbifdatasetid, gbifpublisherid, networks, \
    license, migrator, vntype, orgcountry, orgstateprovince, \
    dctype, basisofrecord, \
    continent, country, stateprov, county, municipality, \
    islandgroup, island, waterbody, locality, \
    decimallatitude, decimallongitude, coordinateuncertaintyinmeters, \
    geodeticdatum, georeferencedby, georeferenceverificationstatus, \
    kingdom, phylum, classs, order, family, \
    genus, specificepithet, infraspecificepithet, \
    scientificname, vernacularname, typestatus, \
    recordedby, recordnumber, fieldnumber, establishmentmeans, \
    bed, formation, group, member, \
    sex, lifestage, preparations, reproductivecondition, \
    year, month, day, startdayofyear, enddayofyear, eventdate, \
    haslicense, hasmedia, hastissue, hastypestatus, \
    isfossil, mappable, wascaptive, wasinvasive, \
    haslength, haslifestage, hasmass, hassex, \
    lengthinmm, lengthtype, massing, rank, hashid = map(data.get, 
        ['keyname', 'iptrecordid', 'icode', 'collectioncode', 'catalognumber', 
         'gbifdatasetid', 'gbifpublisherid', 'networks', 
         'license', 'migrator', 'vntype', 'orgcountry', 'orgstateprovince',
         'dctype', 'basisofrecord', 
         'continent', 'country', 'stateprovince', 'county', 'municipality', 
         'islandgroup', 'island', 'waterbody', 'locality',
         'decimallatitude', 'decimallongitude', 'coordinateuncertaintyinmeters', 
         'geodeticdatum', 'georeferencedby', 'georeferenceverificationstatus',
         'kingdom', 'phylum', 'class', 'order', 'family', 
         'genus', 'specificepithet', 'infraspecificepithet', 
         'scientificname', 'vernacularname', 'typestatus', 
         'recordedby', 'recordnumber', 'fieldnumber', 'establishmentmeans',
         'bed', 'formation', 'group', 'member',
         'sex', 'lifestage', 'preparations', 'reproductivecondition',
         'year', 'month', 'day', 'startdayofyear', 'enddayofyear', 'eventdate',
         'haslicense', 'hasmedia', 'hastissue', 'hastypestatus', 
         'isfossil', 'mappable', 'wascaptive', 'wasinvasive',
         'haslength', 'haslifestage', 'hasmass', 'hassex', 
         'lengthinmm', 'lengthtype', 'massing', 'rank', 'hashid'])

#    # The data type on eventdate is date, so turn the input string into a date.
#    eventdate = _w3c_eventdate(data)
    
    # The data type for location is a GeoPoint. Create one from lat and lng ignoring
    # datum. To do this correctly, the lat and lng should be transformed to WGS84 before
    # this.
    location = _location(decimallatitude, decimallongitude)

    # Do full text indexing on all the verbatim fields of the record. 
    # Index specific key fields for explicit searches on their content.

    doc = search.Document(
        doc_id=keyname,
        rank=as_int(rank),
		fields=[
                ### RECORD-LEVEL ###
                search.TextField(name='institutioncode', value=icode),
                search.TextField(name='collectioncode', value=collectioncode),
                search.TextField(name='catalognumber', value=catalognumber),
                search.AtomField(name='dctype', value=dctype),
                search.TextField(name='license', value=license),
                search.AtomField(name='basisofrecord', value=basisofrecord),

                ### OCCURRENCE ###
                search.AtomField(name='iptrecordid', value=iptrecordid),
                search.TextField(name='recordedby', value=recordedby),
                search.TextField(name='recordnumber', value=recordnumber),
                search.TextField(name='fieldnumber', value=fieldnumber),
                search.TextField(name='establishmentmeans', value=establishmentmeans),
                search.TextField(name='sex', value=sex),
                search.TextField(name='lifestage', value=lifestage),
                search.TextField(name='preparations', value=preparations),
                search.TextField( \
                    name='reproductivecondition', value=reproductivecondition),

                ### EVENT (for year, month, day, see below) ###
                search.TextField(name='eventdate', value=eventdate),

                ### LOCATION (for coordinateuncertaintyinmeters, see below) ###
                search.TextField(name='continent', value=continent),
                search.TextField(name='country', value=country),
                search.TextField(name='stateprovince', value=stateprov),
                search.TextField(name='county', value=county),
                search.TextField(name='municipality', value=municipality),
                search.TextField(name='island', value=island),
                search.TextField(name='islandgroup', value=islandgroup),
                search.TextField(name='waterbody', value=waterbody),
                search.TextField(name='locality', value=locality),
                search.TextField(name='geodeticdatum', value=geodeticdatum),
                search.TextField(name='georeferencedby', value=georeferencedby),
                search.TextField( \
                    name='georeferenceverificationstatus', \
                    value=georeferenceverificationstatus),

                ### GEOLOGICAL CONTEXT ###
                search.TextField(name='bed', value=bed),
                search.TextField(name='formation', value=formation),
                search.TextField(name='group', value=group),
                search.TextField(name='member', value=member),

                ### IDENTIFICATION ###
                search.TextField(name='typestatus', value=typestatus),

                ### TAXON ###
                search.AtomField(name='kingdom', value=kingdom),
                search.AtomField(name='phylum', value=phylum),
                search.AtomField(name='class', value=classs),
                search.AtomField(name='order', value=order),
                search.AtomField(name='family', value=family),
                search.AtomField(name='genus', value=genus),
                search.TextField(name='specificepithet', value=specificepithet),
                search.TextField(name='infraspecificepithet', value=infraspecificepithet),
                search.TextField(name='scientificname', value=scientificname),
                search.TextField(name='vernacularname', value=vernacularname),

                ### TRAIT (for lengthinmm, massing, see below) ###
                search.TextField(name='lengthtype', value=lengthtype),

                ### DATA SET ###
                search.AtomField(name='gbifdatasetid', value=gbifdatasetid),
                search.AtomField(name='gbifpublisherid', value=gbifpublisherid),
                search.TextField(name='lastindexed', value=indexdate),                
                search.TextField(name='networks', value=networks),
                search.TextField(name='migrator', value=migrator),
                search.TextField(name='orgcountry', value=orgcountry),
                search.TextField(name='orgstateprovince', value=orgstateprovince),

                ### BOOLEANS ###
                search.AtomField(name='haslicense', value=haslicense),
                search.AtomField(name='hasmedia', value=hasmedia),
                search.AtomField(name='hastissue', value=hastissue),
                search.AtomField(name='hastypestatus', value=hastypestatus),
                search.AtomField(name='isfossil', value=isfossil),
                search.AtomField(name='mappable', value=mappable),
                search.AtomField(name='wascaptive', value=wascaptive),
                search.AtomField(name='wasinvasive', value=wasinvasive),
                search.AtomField(name='haslength', value=haslength),
                search.AtomField(name='haslifestage', value=haslifestage),
                search.AtomField(name='hasmass', value=hasmass),
                search.AtomField(name='hassex', value=hassex),

                ### INDEX ###
                search.NumberField(name='rank', value=as_int(rank)),
                search.AtomField(name='vntype', value=vntype),
                search.NumberField(name='hashid', value=as_int(hashid)),
                search.TextField(name='verbatim_record', value=json.dumps(data))])

    v = as_int(year)
    if v is not None:
        doc.fields.append(search.NumberField( \
            name='year', value=v))

    v = as_int(month)
    if v is not None:
        doc.fields.append(search.NumberField( \
            name='month', value=v))

    v = as_int(day)
    if v is not None:
        doc.fields.append(search.NumberField( \
            name='day', value=v))

    v = as_int(startdayofyear)
    if v is not None:
        doc.fields.append(search.NumberField( \
            name='startdayofyear', value=v))

    v = as_int(enddayofyear)
    if v is not None:
        doc.fields.append(search.NumberField( \
            name='enddayofyear', value=v))

    v = as_float(lengthinmm)
    if v is not None:
        doc.fields.append(search.NumberField( \
            name='lengthinmm', value=v))

    v = as_float(massing)
    if v is not None:
        doc.fields.append(search.NumberField( \
            name='massing', value=v))

    if location is not None:
        doc.fields.append(search.GeoField(name='location', value=location))

    v = as_int(coordinateuncertaintyinmeters)
    if v is not None:
        doc.fields.append(search.NumberField( \
            name='coordinateuncertaintyinmeters', value=v))

    return doc

def index_doc(doc, index_name, namespace, issue=None):
    """
    Index a document in the given index and namespace.
    """
    max_retries = 2
    retry_count = 0
    while retry_count < max_retries:
        try:
            search.Index(index_name, namespace=namespace).put(doc)
#            logging.warning('Indexed doc:\n%s' % doc )
            return # Successfully indexed document.
        except Exception, e:
            logging.error('Put #%s failed for doc %s (%s)' % (retry_count, doc.doc_id, e))
            retry_count += 1
    logging.error('Failed to index: %s' % doc.doc_id)

def as_float(str): 
    """ Convert a string into a float, if possible.
    parameters:
        str - string (required)
    returns:
        a float equivalent of the string, or None
    """
    try:
        return float(str)
    except:
        return None

def as_int(str): 
    """ Convert a string into an int, if possible.
    parameters:
        str - string (required)
    returns:
        an int equivalent of the string, or None
    """
    try:
        return int(str)
    except:
        return None

def _location(lat, lon):
    """Return a GeoPoint representation of lat and long, if possible, 
       otherwise return None.
    """
    try:
        return apply(search.GeoPoint, map(float, [lat, lon]))
    except:
        return None

# def _w3c_eventdate(rec):
#     """Construct a W3C datetime from year, month, and day, if possible."""
#     if rec.has_key('day') is False:
#         return None
#     if len(rec['day']) == 0:
#         return None
#     if rec.has_key('month') is False:
#         return None
#     if len(rec['month']) == 0:
#         return None
#     if rec.has_key('year') is False:
#         return None
#     if len(rec['year']) == 0:
#         return None
#     isodate = '%s-%s-%s' % (rec['year'], rec['month'], rec['day'])
#     
#     try:
#         return datetime.strptime(isodate, '%Y-%m-%d').date()
#     except:
#         return None
