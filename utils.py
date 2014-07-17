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

def is_float(str):
    """Return the value of str as a float if possible, otherwise return None."""
    # Accepts str as string. Returns float(str) or None
    if str is None:
        return None
    try:
        f = float(str)
        return f
    except ValueError:
        return None
 
def valid_year(year):
    """Return True if the year is since 1700 and before next year, otherwise return False."""
    # Accepts year as string.
    fyear = is_float(year)
    if fyear is None:
        return False
    if fyear < 1699:
        return False
    if fyear > datetime.now().year:
        return False
    return True
 
def valid_month(month):
    """Return True if the month is between 1 and 12 inclusive, otherwise return False."""
    # Accepts month as string.
    fmonth = is_float(month)
    if fmonth is None:
        return False
    if fmonth < 1:
        return False
    if fmonth > 12:
        return False
    return True
 
def valid_day(day):
    """Return True if the day is between 1 and 31 inclusive, otherwise return False."""
    # Accepts day as string.
    fday = is_float(day)
    if fday is None:
        return False
    if fday < 1:
        return False
    if fday > 31:
        return False
    return True
 
def valid_latlng(lat,lng):
    """Return True if lat and lng are in valid ranges, otherwise return False."""
    # Accepts lat and lng as strings.
    flat = is_float(lat)
    if flat is None:
        return False
    if flat < -90 or flat > 90:
        return False
    flng = is_float(lng)
    if flng is None:
        return False
    if flng < -180 or flng > 180:
        return False
    if flat == 0 and flng == 0:
        return False
    return True
 
def valid_coords(rec):
    """Return True if decimallatitude and decimallongitude in rec are in valid ranges, otherwise return False."""
    if rec.has_key('decimallatitude'):
        if rec.has_key('decimallongitude'):
            return valid_latlng(rec['decimallatitude'],rec['decimallongitude'])
    return False
 
def valid_georef(rec):
    """Return True if rec has valid coords, valid coordinateuncertaintyinmeters, and a geodeticdatum, otherwise return False."""
    if rec.has_key('coordinateuncertaintyinmeters') is False:
        return False
    if rec.has_key('geodeticdatum') is False:
        return False
    if _coordinateuncertaintyinmeters(rec['coordinateuncertaintyinmeters']) is None:
        return False
    if rec.has_key('decimallatitude') is False:
        return False
    if rec.has_key('decimallongitude') is False:
        return False
    return valid_latlng(rec['decimallatitude'],rec['decimallongitude'])
 
def valid_binomial(rec):
    """Return True if rec has a genus and specificepithet, otherwise return False."""
    if rec.has_key('genus') is False:
        return False
    if rec.has_key('specificepithet') is False:
        return False
    # Sufficient condition for now to have these DwC terms populated.
    # Later may want to test them against a name authority to determine validity.
    return True
def _rank(rec):
    """Return the rank to be used in document sorting based on the content priority."""
    hasbinomial = valid_binomial(rec)
    if hasbinomial is False:
        return 0
    # Must have a binomial to have a rank.
    rank = 0
    hasgeoref = valid_georef(rec)
    hascoords = False
    if hasgeoref is True:
        hascoords = True
    else:
        hascoords = valid_coords(rec) 
    hasyear = False
    if rec.has_key('year'):
        hasyear = valid_year(rec['year'])
    hasmonth = False
    if rec.has_key('month'):
        hasmonth = valid_month(rec['month'])
    hasday = False
    if rec.has_key('day'):
        hasday = valid_day(rec['day'])
    
    if hasgeoref is True:
        rank = 9
        if hasyear is True:
            rank = 10
            if hasmonth is True:
                rank = 11
                if hasday is True:
                    rank = 12
    elif hascoords is True:
        rank = 5
        if hasyear is True:
            rank = 6
            if hasmonth is True:
                rank = 7
                if hasday is True:
                    rank = 8
    else:
        rank = 1
        if hasyear is True:
            rank = 2
            if hasmonth is True:
                rank = 2
                if hasday is True:
                    rank = 4
    return rank

def has_media(rec):
    """Return 1 if the rec represents a media record or has associated media, otherwise return 0."""
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
                "forzen", "freez", "freeze", "heart", "muscle", "higado", "kidney",
                "liver", "lung", "nitrogen", "pectoral", "rinon",
                "kidney", "rnalater", "sample", "sangre", "toe", "spleen"]

def has_tissue(rec):
    """Return 1 if the rec represents a record that has preparations that might be viable tissues, otherwise return 0."""
    if rec.has_key('preparations'):
        preps = rec['preparations'].lower()
        for token in tissuetokens:
            if token in preps:
                return 1
    return 0

def has_typestatus(rec):
    """Return 1 if the rec has the typestatus field populated, otherwise return 0."""
    if rec.has_key('typestatus'):
        return 1
    return 0

# This function may no longer be in use.
def network(rec, network):
    if rec.has_key('networks'):
        networks = [x.lower() for x in rec['networks'].split(',')]
        if network in networks:
            return 1
    return 0

def _coordinateuncertaintyinmeters(unc):
    """Return the value of unc as a rounded up integer if it is a number greater than zero, otherwise return None."""
    uncertaintyinmeters = is_float(unc)
    if uncertaintyinmeters is None:
        return None
    # Check to see if uncertaintyinmeters is less than one. Zero is not a legal 
    # value. Less than one is an error in concept.
    if uncertaintyinmeters < 1:
        return None
    # Return the nearest rounded up meter.
    return int( round(uncertaintyinmeters + 0.5) )

def _location(lat, lon):
    """Return a GeoPoint representation of lat and long if possible, otherwise return None."""
    try:
        location = apply(search.GeoPoint, map(float, [lat, lon]))
    except:
        location = None
    return location

def _type(rec):
    """Return one of 'specimen', 'observation', or 'both' based on content of the type and basisofrecord fields."""
    if rec.has_key('type'):
        if rec['type'].lower() == 'physicalobject':
            return 'specimen'
        return 'observation'
    if rec.has_key('basisofrecord'):
        if 'spec' in rec['basisofrecord'].lower():
            return 'specimen'
        return 'observation'
    return 'both'

# This function may no longer be in use.
def _rec(rec):
    """Remove the listed field from rec."""
    for x in ['pubdate','url','eml','dwca','title','icode','description',
        'contact','orgname','email','emlrights','count','citation','networks','harvestid']:
        rec.pop(x)
    return json.dumps(rec)

def eventdate_from_ymd(y,m,d):
    """Return the eventdate as ISO8601 based on year y, month m, and day d as strings."""
    if valid_year(y) is False:
        return None
    hasmonth = valid_month(m)
    hasday = valid_day(d)
    eventdate = y
    if hasmonth is True:
        month = is_float(m)
        if month is not None:
            if month > 9:
                eventdate += '-'+m
            else:
                eventdate += '-0'+m
            if hasday is True:
                day = is_float(d)
                if day is not None:
                    if day > 9:
                        eventdate += '-'+d
                    else:
                        eventdate += '-0'+d
    return eventdate
    
def _eventdate(rec):
    """Return the eventdate as a datetime.date based on a eventdate if it is a datetime, otherwise on year, month, and day converted to ISO8601."""
    isodate = None
    if rec.has_key('eventdate'):
        isodate = rec['eventdate']
    else:
        if rec.has_key('year') is False or rec.has_key('month') is False or rec.has_key('day') is False:
            return None
            y = rec['year']
            m = rec['month']
            d = rec['day']
            isodate = eventdate_from_ymd(y,m,d)
    
    try:
        eventdate = datetime.strptime(isodate, '%Y-%m-%d').date()
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
    """Returns a dictionary of all fields in the rec list with non-printing characters removed."""
    val = {}
    for name, value in rec.iteritems():
        if value:
        	# Replace all tabs, vertical tabs, carriage returns, and line feeds 
        	# in field contents with space, then remove leading and trailing spaces.
            val[name] = re.sub('[\v\t\r\n]+', ' ', value).strip(' ')
#            if val[name]=='':
#                rec.pop(name)
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

def full_text_key_trim(rec):
    """Returns a record rec with non-full-text-indexed terms removed."""
    for key in rec.keys():
        if key not in FULL_TEXT_KEYS:
            rec.pop(key)
    return rec

def verbatim_dwc(rec, keyname):
    """Returns a record with verbatim original Darwin Core fields plus the keyname field, minus any empty fields."""
    for key in rec.keys():
        if rec[key] == '':
            rec.pop(key)
    rec['keyname'] = keyname
    return rec

def index_record(data, issue=None):
    """Creates a document ready to index from the given input data. This is where the work is to construct the document."""
    icode, collcode, catnum, occid, \
    continent, country, stateprov, county, islandgroup, island, \
    classs, order, family, genus, specep, \
    lat, lon, uncertainty, datum, \
    year, collname, url, pubdate = map(data.get, 
        ['icode', 'collectioncode', 'catalognumber', 'id',
         'continent', 'country', 'stateprovince', 'county', 'islandgroup', 'island',
         'classs', 'order', 'family', 'genus', 'specificepithet', 
         'decimallatitude', 'decimallongitude', 'coordinateuncertaintyinmeters', 'geodeticdatum',
         'year', 'recordedby', 'url', 'pubdate'])

    # Trim any empty data fields from the verbatim record
    
    # Translate the field 'classs' to field 'class'
    if data.has_key('classs'):        
        data.pop('classs')
    data['class'] = classs

    # Create a slugged version of the resource title as a resource identifier
    resource_slug = slugify(data['title'])
    
    icode_slug = re.sub(" ","", icode.lower())
    
    # Make a coll_id as slugged ascii of the collection for use in document id.
    coll_id = ''
    if collcode is not None and collcode != '':
        coll_id = re.sub(' ','', re.sub("\'",'',repr(collcode)).lower())
    else:
        coll_id = re.sub("\'",'',repr(resource_slug))
    
    # Make a occ_id as slugged ascii of the record identifier for use in document id.
    occ_id = ''
    if catnum is not None and len(catnum) > 0:        
        occ_id = re.sub("\'",'',repr(slugify(data['catalognumber'])))
    else:
        if occid is not None and len(occid) > 0:
            occ_id = 'oid-%s' % re.sub("\'",'',repr(occid))
        else:
            occ_id = 'hid-%s' % data['harvestid']

    # Make a potentially persistent resource identifier
    res_id = '%s/%s' % (icode_slug, resource_slug)

    # Make a unique, potentially persistent document id
    keyname = '%s/%s/%s' % (icode_slug, coll_id, occ_id)
#    oldkeyname = '%s/%s/%s' % (organization_slug, resource_slug, data['harvestid'])

    data['keyname'] = keyname

    # Determine any values for indexing that must be calculated before creating doc
    # because full_text_key_trim(data) affects the contents of data.
    recrank = _rank(data)
    location = _location(lat, lon)
    unc = _coordinateuncertaintyinmeters(uncertainty)
    eventdate = _eventdate(data)
    fyear = is_float(year)

#    if location is not None:
#        logging.info('%s %s location: %s unc:%s datum: %s georefed: %s rank: %s\n%s' %
#                    (icode, catnum, location, unc, datum, valid_georef(data), recrank, data ) )

    # Of the fields to index, the following are essential, while all others are probably
    # sufficiently covered by full text indexing on the record field:
    # institutioncode, resource, catalognumber, year, type, media, tissue, hastypestatus, 
    # rank, record.
    # pubdate would be useful for being able to reindex a single resource - because 
    # search.Index().put(doc) replaces docs with matching doc_ids. Then one would only
    # need to remove any records from that resource with an older pubdate as opposed to 
    # deleting all records from the resource.
    # The problem is that pubdate is currently populated with the date the 
    # resource was first published, not the date it was last published.
    # hashid is a hash of the rec as a means to evenly distribute records among bins
    # for parallel processing on bins having 10k or less records.
    
    doc = search.Document(
        doc_id=keyname,
        rank=recrank,
		fields=[
                search.TextField(name='institutioncode', value=icode),
                search.TextField(name='resource', value=res_id),
                search.TextField(name='catalognumber', value=catnum),
                search.TextField(name='occurrenceid', value=occid),
                search.TextField(name='continent', value=continent),
                search.TextField(name='country', value=country),
                search.TextField(name='stateprovince', value=stateprov),
                search.TextField(name='county', value=county),
                search.TextField(name='island', value=island),
                search.TextField(name='islandgroup', value=islandgroup),
                search.TextField(name='class', value=classs),
                search.TextField(name='order', value=order),
                search.TextField(name='family', value=family),
                search.TextField(name='genus', value=genus),
                search.TextField(name='specificepithet', value=specep),
                search.TextField(name='recordedby', value=collname),
                search.TextField(name='type', value=_type(data)),
#                search.TextField(name='url', value=url),
                search.TextField(name='pubdate', value=pubdate),
                search.NumberField(name='media', value=has_media(data)),
                search.NumberField(name='tissue', value=has_tissue(data)),
                search.NumberField(name='hastypestatus', value=has_typestatus(data)),
                search.NumberField(name='rank', value=recrank),
                search.NumberField(name='hashid', value=hash(keyname)%1999),
                search.TextField(name='verbatim_record', 
                                 value=json.dumps(verbatim_dwc(data, keyname))),
    # Note that full_text_key_trim pops values from data, making them unavailable to
    # for use hereafter in this function.
                search.TextField(name='record', 
                                 value=json.dumps(full_text_key_trim(data)))])

    mappable = 0
    if location is not None:
        doc.fields.append(search.GeoField(name='location', value=location))
        mappable = 1
    doc.fields.append(search.NumberField(name='mappable', value=mappable))

    if unc is not None:
        doc.fields.append(search.NumberField(name='coordinateuncertaintyinmeters', value=unc))

    if fyear is not None:
        doc.fields.append(search.NumberField(name='year', value=fyear))

    if eventdate is not None:
        doc.fields.append(search.DateField(name='eventdate', value=eventdate))
    return doc

def index_doc(doc, index_name, namespace, issue=None):
    max_retries = 2
    retry_count = 0
    while retry_count < max_retries:
        try:
            search.Index(index_name, namespace=namespace).put(doc)
            return # Successfully indexed document.
        except Exception, e:
            logging.error('Put #%s failed for doc %s (%s)' % (retry_count, doc.doc_id, e))
            retry_count += 1
    logging.error('Failed to index: %s' % doc.doc_id)

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
#        logging.info('Did get_rec_dict() OK! data: %s' % data)
        doc = index_record(data)
#        logging.info('Did index_record(data) OK!! doc: %s' % doc)
        index_doc(doc, index_name, namespace)
#        logging.info('Did index_doc() OK!!!')
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