import logging
from optparse import OptionParser
from datetime import datetime

def is_float(str):
    """Return the value of str as a float if possible, otherwise return None."""
    # Accepts str as string. Returns float(str) or None
    try:
        f = float(str)
        return f
    except ValueError:
        return None
 
def valid_binominal(rec):
    """Return True if rec has a genus and specificepithet, otherwise return False."""
    if rec.has_key('genus') is False:
        return False
    if rec.has_key('specificepithet') is False:
        return False
    # Sufficient condition for now to have these DwC terms populated.
    # Later may want to test them against a name authority to determine validity.
    return True
 
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
 
def rank(rec):
    """Return the rank to be used in document sorting based on the content priority."""
    hasbinominal = valid_binominal(rec)
    if hasbinominal is False:
        return 0
    # Must have a binominal to have a rank.
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

def _coordinateuncertaintyinmeters(unc):
    """Return the value of unc as a rounded up integer if it is a number greater than zero, otherwise return None."""
    uncertaintyinmeters = is_float(unc)
    if uncertaintyinmeters is None:
        return None
    # Check to see if uncertaintyinmeters is less than one. Zero is not a legal 
    # value. Less than one is an error in concept.
    if uncertaintyinmeters > 1:
        return None
    # Return the nearest rounded up meter.
    return int( round(uncertaintyinmeters + 0.5) )

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
    return valid_latlng(rec['decimallatitude'],rec['decimallongitude'])
 
def _location(lat, lon):
    """Return a GeoPoint representation of lat and long if possible, otherwise return None."""
    try:
        location = apply(search.GeoPoint, map(float, [lat, lon]))
    except:
        location = None
    return location

def _getoptions():
    """Parses command line options and returns them."""
    parser = OptionParser()
    parser.add_option("-c", "--command", dest="command",
                      help="Command to run",
                      default=None)
    parser.add_option("-1", "--bb1", dest="bb1",
                      help="NW corner of one bounding box",
                      default=None)
    parser.add_option("-2", "--bb2", dest="bb2",
                      help="NW corner of second bounding box",
                      default=None)
    return parser.parse_args()[0]

def main():
    logging.basicConfig(level=logging.DEBUG)
    options = _getoptions()
    logging.info('Options: %s' % options )
    command = ''
    if options.command is not None:
        command = options.command.lower()
    
    logging.info('COMMAND %s' % command)

    if command=='help':
        print 'syntax: -c intersection -1 w_lng,n_lat|e_lng,s_lat -2 w_lng,n_lat|e_lng,s_lat'
        return

    rec={}
    rec['decimallatitude']='23.432'
    rec['decimallongitude']='45.43534'
    rec['coordinateuncertaintyinmeters']='0.325'
    rec['geodeticdatum']='NAD27'
    rec['genus']='Vultur'
    rec['specificepithet']='gryphus'
    rec['year']='1900'
    rec['month']='1'
    rec['day']='4'

    lat, lon, unc, datum = map(rec.get, 
        ['decimallatitude', 'decimallongitude', 'coordinateuncertaintyinmeters', 'geodeticdatum'])
    
    logging.info('REC: %s' % rec)
    mappable = valid_latlng(lat,lon)
    georefed = valid_georef(rec)
    therank = rank(rec)
    
    logging.info('Rank: %s Mappable: %s Georef: %s' % 
                (therank, mappable, georefed) )
    
    if command=='test1':
        print 'Test command test1: %s' % (rec)

if __name__ == "__main__":
    main()