from google.appengine.api import namespace_manager
import json
import logging
from datetime import datetime
from google.appengine.api import search
from google.appengine.api.search import SortOptions, SortExpression
from mapreduce import operation as op
import re
import htmlentitydefs
import os
import codecs
import unicodedata
from google.appengine.api import files
from mapreduce import context


IS_DEV = 'Development' in os.environ['SERVER_SOFTWARE']

# TODO: Pool search api puts?
FULL_TEXT_KEYS = [
'type', 'institutionid', 'collectionid', 'institutioncode', 'collectioncode', 
'datasetname', 'basisofrecord', 'dynamicproperties', 'occurrenceid', 'catalognumber', 
'occurrenceremarks', 'recordnumber', 'recordedby', 'individualid', 'sex', 'lifestage', 
'reproductivecondition', 'behavior', 'establishmentmeans', 'preparations', 'disposition',
'othercatalognumbers', 'previousidentifications', 'materialsampleid', 
'verbatimeventdate', 'habitat', 'fieldnumber', 'fieldnotes', 'highergeography', 
'continent', 'waterbody', 'islandgroup', 'island', 'country', 'countrycode', 
'stateprovince', 'county', 'municipality', 'locality', 'verbatimlocality', 
'verbatimelevation', 'verbatimdepth', 'geodeticdatum', 'georeferencedby', 
'georeferenceprotocol', 'georeferencesources', 'georeferenceverificationstatus',
'georeferenceremarks', 'earliesteonorlowesteonothem', 'latesteonorhighesteonothem', 
'earliesteraorlowesterathem', 'latesteraorhighesterathem', 
'earliestperiodorlowestsystem', 'latestperiodorhighestsystem', 
'earliestepochorlowestseries', 'latestepochorhighestseries', 'earliestageorloweststage',
'latestageorhigheststage', 'lowestbiostratigraphiczone', 'highestbiostratigraphiczone', 
'lithostratigraphicterms', 'group', 'formation', 'member', 'bed', 'identifiedby', 
'identificationreferences', 'identificationverificationstatus', 'identificationremarks',
'identificationqualifier', 'typestatus', 'higherclassification', 'kingdom', 'phylum', 
'class', 'order', 'family', 'genus', 'subgenus', 'specificepithet', 
'infraspecificepithet', 'taxonrank', 'vernacularname']

HEADER = [
'pubdate', 'url', 'eml', 'dwca', 'title', 'icode', 'description', 'contact', 'orgname', 
'email', 'emlrights', 'count', 'citation', 'networks', 'harvestid', 
'id', 'associatedmedia', 'associatedoccurrences', 'associatedreferences', 
'associatedsequences', 'associatedtaxa', 'basisofrecord', 'bed', 'behavior', 
'catalognumber', 'collectioncode', 'collectionid', 'continent', 'coordinateprecision', 
'coordinateuncertaintyinmeters', 'country', 'countrycode', 'county', 
'datageneralizations', 'dateidentified', 'day', 'decimallatitude', 'decimallongitude', 
'disposition', 'earliestageorloweststage', 'earliesteonorlowesteonothem', 
'earliestepochorlowestseries', 'earliesteraorlowesterathem', 
'earliestperiodorlowestsystem', 'enddayofyear', 'establishmentmeans', 'eventattributes', 
'eventdate', 'eventid', 'eventremarks', 'eventtime', 'fieldnotes', 'fieldnumber', 
'footprintspatialfit', 'footprintwkt', 'formation', 'geodeticdatum', 
'geologicalcontextid', 'georeferenceprotocol', 'georeferenceremarks', 
'georeferencesources', 'georeferenceverificationstatus', 'georeferencedby', 'group', 
'habitat', 'highergeography', 'highergeographyid', 'highestbiostratigraphiczone', 
'identificationattributes', 'identificationid', 'identificationqualifier', 
'identificationreferences', 'identificationremarks', 'identifiedby', 'individualcount', 
'individualid', 'informationwithheld', 'institutioncode', 'island', 'islandgroup', 
'latestageorhigheststage', 'latesteonorhighesteonothem', 'latestepochorhighestseries', 
'latesteraorhighesterathem', 'latestperiodorhighestsystem', 'lifestage', 
'lithostratigraphicterms', 'locality', 'locationattributes', 'locationid', 
'locationremarks', 'lowestbiostratigraphiczone', 'maximumdepthinmeters', 
'maximumdistanceabovesurfaceinmeters', 'maximumelevationinmeters', 
'measurementaccuracy', 'measurementdeterminedby', 'measurementdetermineddate', 
'measurementid', 'measurementmethod', 'measurementremarks', 'measurementtype', 
'measurementunit', 'measurementvalue', 
'member', 'minimumdepthinmeters', 'minimumdistanceabovesurfaceinmeters', 
'minimumelevationinmeters', 'month', 'occurrenceattributes', 'occurrencedetails', 
'occurrenceid', 'occurrenceremarks', 'othercatalognumbers', 'pointradiusspatialfit', 
'preparations', 'previousidentifications', 'recordnumber', 'recordedby', 
'relatedresourceid', 'relationshipaccordingto', 'relationshipestablisheddate', 
'relationshipofresource', 'relationshipremarks', 
'reproductivecondition', 
'resourceid', 'resourcerelationshipid', 
'samplingprotocol', 'sex', 'startdayofyear', 'stateprovince', 'taxonattributes', 
'typestatus', 'verbatimcoordinatesystem', 'verbatimcoordinates', 'verbatimdepth', 
'verbatimelevation', 'verbatimeventdate', 'verbatimlatitude', 'verbatimlocality', 
'verbatimlongitude', 'waterbody', 'year', 'footprintsrs', 'georeferenceddate', 
'identificationverificationstatus', 'institutionid', 'locationaccordingto', 
'municipality', 'occurrencestatus', 'ownerinstitutioncode', 'samplingeffort', 
'verbatimsrs', 'locationaccordingto7', 'taxonid', 'taxonconceptid', 'datasetid', 
'datasetname', 'source', 'modified', 'accessrights', 'rights', 'rightsholder', 
'language', 'higherclassification', 'kingdom', 'phylum', 'classs', 'order', 
'family', 'genus', 'subgenus', 'specificepithet', 'infraspecificepithet', 
'scientificname', 'scientificnameid', 'vernacularname', 'taxonrank', 
'verbatimtaxonrank', 'infraspecificmarker', 'scientificnameauthorship', 
'nomenclaturalcode', 'namepublishedin', 'namepublishedinid', 'taxonomicstatus', 
'nomenclaturalstatus', 'nameaccordingto', 'nameaccordingtoid', 'parentnameusageid', 
'parentnameusage', 'originalnameusageid', 'originalnameusage', 'acceptednameusageid', 
'acceptednameusage', 'taxonremarks', 'dynamicproperties', 'namepublishedinyear']

NON_DWC_HEADER_KEYS = [
'pubdate', 'url', 'eml', 'dwca', 'title', 'icode', 'description', 'contact', 'orgname', 
'email', 'emlrights', 'count', 'citation', 'networks', 'harvestid', 
'measurementaccuracy', 'measurementdeterminedby', 'measurementdetermineddate', 
'measurementid', 'measurementmethod', 'measurementremarks', 'measurementtype', 
'measurementunit', 'measurementvalue', 'relatedresourceid', 'relationshipaccordingto', 
'relationshipestablisheddate', 'relationshipofresource', 'relationshipremarks', 
'resourceid', 'resourcerelationshipid']

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False    
 
def valid_latlng(lat,lng):
    # accepts lat and lng as strings.
    if not is_number(lat):
        return False
    if not is_number(lng):
        return False
    flat = float(lat)
    if flat < -90 or flat > 90:
        return False
    flng = float(lng)
    if flng < -180 or flng > 180:
        return False
    return True
 
def valid_georef(rec):
    if rec.has_key('decimallatitude'):
        if rec.has_key('decimallongitude'):
            return valid_latlng(rec['decimallatitude'],rec['decimallongitude'])
    return False
 
def valid_binomial(rec):
    if rec.has_key('genus'):
      if rec.has_key('specificepithet'):
        # Sufficient condition for now to have these DwC terms populated.
        # Later may want to test them against a name authority to determine validity.
        return True
    return False
 
def rank(rec):
    rank = 0
    if valid_georef(rec) is True and valid_binomial(rec) is True:
        rank = 5
        if rec.has_key('year'):
            rank = 6
            if rec.has_key('month'):
                rank = 7
                if rec.has_key('day'):
                    rank = 8
    elif valid_binomial(rec) is True:
        rank = 1
        if rec.has_key('year'):
            rank = 2
            if rec.has_key('month'):
                rank = 3
                if rec.has_key('day'):
                    rank = 4
    return rank

def has_media(rec):
    if rec.has_key('associatedmedia'):
        return 1
    if rec.has_key('type'):
        if rec['type'].lower()=='sound':
            return 1
        if 'image' in rec['type'].lower():
            return 1
        return 0
    if rec.has_key('basisofrecord'):
        if rec['basisofrecord'].lower()=='machineobservation':
            return 1
    return 0

tissuetokens = ["+t", "tiss", "tissue", "blood", "dmso", "dna", "extract", "froze", 
                "forzen", "freez", "heart", "muscle", "higado", "kidney",
                "liver", "lung", "nitrogen", "pectoral", "rinon",
                "kidney", "rnalater", "sample", "sangre", "toe", "spleen"]

def has_tissue(rec):
    if rec.has_key('preparations'):
        for token in tissuetokens:
            if token in rec['preparations'].lower():
                return 1
    return 0

def has_typestatus(rec):
    if rec.has_key('typestatus'):
        if rec['typestatus'] is not None:
            return 1
    return 0

def network(rec, network):
    if rec.has_key('networks'):
        networks = [x.lower() for x in rec['networks'].split(',')]
        if network in networks:
            return 1
    return 0

def _location(lat, lon):
    try:
        location = apply(search.GeoPoint, map(float, [lat, lon]))
    except:
        location = None
    return location

def _type(rec):
    if rec.has_key('type'):
        if rec['type'].lower() == 'physicalobject':
            return 'specimen'
        return 'observation'
    if rec.has_key('basisofrecord'):
        if 'spec' in rec['basisofrecord'].lower():
            return 'specimen'
        return 'observation'
    return 'both'

def _rec(rec):
    for x in ['pubdate','url','eml','dwca','title','icode','description',
        'contact','orgname','email','emlrights','count','citation','networks','harvestid']:
        rec.pop(x)
    return json.dumps(rec)

def _eventdate(year):
    try:
        eventdate = datetime.strptime(year, '%Y').date()
    except:
        eventdate = None
    return eventdate

def slugify(s, length=None, separator="-"):
    """Return a slugged version of supplied string."""
    s = re.sub('[^a-zA-Z\d\s:]', ' ', s)
    if length:
        words = s.split()[:length]
    else:
        words = s.split()
    s = ' '.join(words)
    ret = ''
    for c in s.lower():
        try:
            ret += htmlentitydefs.codepoint2name[ord(c)]
        except:
            ret += c
    ret = re.sub('([a-zA-Z])(uml|acute|grave|circ|tilde|cedil)', r'\1', ret)
    ret = ret.strip()
    ret = re.sub(' ', '_', ret)
    ret = re.sub('\W', '', ret)
    ret = re.sub('[ _]+', separator, ret)
    return ret.strip()

def get_rec_dict(rec):
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

def handle_failed_index_put(data, resource, did, write_path, mrid):
    logging.info('Handling failed index.put() - mrid:%s did:%s write_path:%s' % 
                (mrid, did, write_path))
    max_retries = 5
    retry_count = 0

    line = '\t'.join([data[x] for x in HEADER])

    # Write line to file:
    while retry_count < max_retries:
        try:
            with files.open(write_path, 'a') as f:
                f.write('%s\n' % line)
                f.close(finalize=False)
                logging.info('Successfully logged failure to GCS for %s' % did)
                return
        except:
            logging.error('Failure %s of %s to write line to failure log: %s' % 
                         (retry_count, max_retries, line))
            retry_count += 1

    logging.critical('Failed to index and failed to log %s' % did)
    namespace = namespace_manager.get_namespace()
    job = IndexJob.get_by_id(mrid, namespace=namespace)
    job.failed_logs.append(did)
    job.put()

# Return a record with non-full-text-indexed terms removed.
def full_text_key_trim(rec):
  for key in rec.keys():
    if key not in FULL_TEXT_KEYS:
      rec.pop(key)
  return rec

# Return a record with verbatim original Darwin Core fields plus the keyname field.
def verbatim_dwc(rec, keyname):
  # for key in rec.keys():
  #   if key in NON_DWC_HEADER_KEYS:
  #     rec.pop(key)
  rec['keyname'] = keyname
  return rec

def index_record(data, index_name, namespace, issue=None):
    icode, ccode, catnum, occid, \
    country, stateprov, county, \
    classs, order, family, genus, specep, \
    lat, lon, \
    year, collname, url = map(data.get, 
        ['icode', 'collectioncode', 'catalognumber', 'id',
         'country', 'stateprovince', 'county',
         'classs', 'order', 'family', 'genus', 'specificepithet', 
         'decimallatitude', 'decimallongitude', 
         'year', 'recordedby', 'url'])

    if data.has_key('classs'):        
        data.pop('classs')

    data['class'] = classs

    resource_slug = slugify(data['title'])
    icode_slug = slugify(icode)
    
    coll_id = ''
    if ccode is not None and len(ccode) > 0:
        coll_id = re.sub("\'",'',repr(slugify(ccode)))
    else:
        coll_id = re.sub("\'",'',repr(resource_slug))
    
    occ_id = ''
    if catnum is not None and len(catnum) > 0:        
        occ_id = re.sub("\'",'',repr(slugify(data['catalognumber'])))
    else:
        if occid is not None and len(occid) > 0:
            occ_id = 'oid-%s' % re.sub("\'",'',repr(occid))
        else:
            occ_id = 'hid-%s' % data['harvestid']

    res_id = '%s/%s' % (icode_slug, resource_slug)
    keyname = '%s/%s/%s' % (icode_slug, coll_id, occ_id)
#    oldkeyname = '%s/%s/%s' % (organization_slug, resource_slug, data['harvestid'])

    data['keyname'] = keyname
    
    doc = search.Document(
        doc_id=keyname,
        rank=rank(data),
		fields=[
                search.TextField(name='institutioncode', value=icode),
                search.TextField(name='resource', value=res_id),
                search.TextField(name='catalognumber', value=catnum),
                search.TextField(name='occurrenceid', value=occid),
                search.TextField(name='country', value=country),
                search.TextField(name='stateprovince', value=stateprov),
                search.TextField(name='county', value=county),
                search.TextField(name='class', value=classs),
                search.TextField(name='order', value=order),
                search.TextField(name='family', value=family),
                search.TextField(name='genus', value=genus),
                search.TextField(name='specificepithet', value=specep),
		        search.TextField(name='year', value=year),
                search.TextField(name='recordedby', value=collname),
                search.TextField(name='type', value=_type(data)),
                search.TextField(name='url', value=url),
                search.NumberField(name='media', value=has_media(data)),
                search.NumberField(name='tissue', value=has_tissue(data)),
                search.NumberField(name='hastypestatus', value=has_typestatus(data)),
                search.NumberField(name='rank', value=rank(data)),
                search.TextField(name='verbatim_record', 
                                 value=json.dumps(verbatim_dwc(data, keyname))),
                search.TextField(name='record', 
                                 value=json.dumps(full_text_key_trim(data)))])

    location = _location(lat, lon)
    eventdate = _eventdate(year)

    if location:
        doc.fields.append(search.GeoField(name='location', value=location))
        doc.fields.append(search.NumberField(name='mappable', value=1))
    else:
        doc.fields.append(search.NumberField(name='mappable', value=0))

    if eventdate:
        doc.fields.append(search.DateField(name='eventdate', value=eventdate))

    max_retries = 2
    retry_count = 0
    while retry_count < max_retries:
        try:
            search.Index(index_name, namespace=namespace).put(doc)
            return # Successfully indexed record.
        except Exception, e:
            logging.error('Put #%s failed for doc %s (%s)' % (retry_count, doc.doc_id, e))
            retry_count += 1

    logging.error('Failed to index: %s' % data['keyname'])

    # Failed to index record, so handle it:
    # resource = '%s-%s' % (resource_slug, data['harvestid'])
    # did = data['keyname']
    # ctx = context.get()
    # mrid = ctx.mapreduce_id
    # params = ctx.mapreduce_spec.mapper.params
    # write_path = params['write_path']
    # handle_failed_index_put(data, resource, did, write_path, mrid)

def build_search_index(entity):
    # Get namespace from mapreduce job and set it.
    ctx = context.get()
    params = ctx.mapreduce_spec.mapper.params
    namespace = params['namespace']
    index_name = params['index_name']
    #namespace_manager.set_namespace(namespace)

    try:
        data = get_rec_dict(dict(zip(HEADER, entity.split('\t'))))
        index_record(data, index_name, namespace)
    except Exception, e:
        logging.error('%s\n%s' % (e, data))

def _get_rec(doc):
    for field in doc.fields:
        if field.name == 'record':
            rec = json.loads(field.value)
            rec['rank'] = doc._rank
            return rec

def delete_entity(entity):
    yield op.db.Delete(entity)