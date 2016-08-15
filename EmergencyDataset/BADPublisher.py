#!/bin/env python3
import sys
import simplejson as json
import time
import datetime
import requests

# URL = 'http://169.234.50.93:19002'
URL = 'http://169.234.26.75:19002'

def feedRecord(filename):
    jsonfile = filename + '.json'
    lastOffset = 0
    # currentTime = datetime.datetime.utcnow()

    with open(jsonfile) as f:
        for line in f.readlines():
            record = json.loads(line)

            timestamp = None
            location = None
            impactZone = None

            if 'timeoffset' in record:
                timeOffset = record['timeoffset']
                timeOffset /= 100
                wait = timeOffset - lastOffset 
                time.sleep(wait+10)

                # timestamp = currentTime + datetime.timedelta(seconds=(timeOffset))
                timestamp = datetime.datetime.utcnow()
                timestamp = timestamp.strftime('%Y-%m-%dT%H:%M:%SZ')
                lastOffset = timeOffset

            if 'impactZone' in record:
                iz = record['impactZone']
                impactZone = 'circle(\"%f,%f %f\")' % (iz['center']['x'], iz['center']['y'], iz['radius'])
                del record['impactZone']

            if 'location' in record:
                loc = record['location']
                location = 'point(\"{0}, {1}\")'.format(loc['x'], loc['y'])
                del record['location']


            recordString = json.dumps(record)
            recordString = recordString.replace('}', '')

            if impactZone:
                recordString = '{0}, \"impactZone\":{1}'.format(recordString, impactZone)

            if location:
                recordString = '{0}, \"location\":{1}'.format(recordString, location)

            if timestamp:
                recordString = '{0}, \"timestamp\": datetime(\"{1}\")'.format(recordString, timestamp)

            recordString = recordString + '}'

            stmt = 'use dataverse channels; insert into dataset %s [%s]' % (filename, recordString)

            print(stmt)

            r = requests.get(URL + '/update', params={'statements': stmt})

            if r.status_code != 200:
                print('Insertation failed, %s' % str(r.text))



if __name__ == "__main__":
    feedRecord(sys.argv[1])
