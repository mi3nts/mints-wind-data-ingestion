import os
import json
import datetime
import math
import requests
import psycopg2
import pypsqlcon
import sys

try:
    import urllib2
except ModuleNotFoundError:
    import urllib.request as urllib2

# Future reference: Assuming that data is successfully stored into database,
# we should not have to worry about keeping the data in these files and replace
# the information with the new data.

# Grab data from current directory up until after 6 hours
# If 6 hours passed, then check to see if the new folder is up
# If not, use the next hour in the current directory, check if new directory is up
# Do this if the directory is not up
# Once the folder is up, get the equivalent timestamp for the new directory to get updated predictions

API_ENDPOINT = "https://api.sharedairdfw.com/wind_data/"
NOAA_ENDPOINT = 'https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod'

current_datetime = datetime.datetime.utcnow()
latLon = '&leftlon=0&rightlon=360&toplat=90&bottomlat=-90'
fdir = os.path.abspath(os.path.dirname(__file__))

# Maximum number of files to check in history
# NOAA gfs data only goes back about 10 days (4 files are available per 
#   day based on different reference hours available)
max_attempts = 40

def convertData(year, month, day, refHour, recorded_hour, needUpdate):
    goToGrib2JSON = './grib2json/target/grib2json-0.8.0-SNAPSHOT/bin'
    gribPath = os.path.join(fdir, goToGrib2JSON)
    os.chdir(gribPath)
    print("[DEBUG] goToGrib2JSON path: {}".format(gribPath))

    # # ** will need to group the U and V data together by time or name of file **
    # (--fp) parameterNumber: 2 (U-component_of_wind)
    # 				         3 (V-component_of_wind)
    # (--fs) Height level above ground => surface1Type: 103
    # (--fv) surface1Value: 10.0

    convertForUComponent = 'sh grib2json --names --data --fp 2 --fs 103 --fv 10.0 --output ../../../../data/u_comp.json ../../../../data/data.grb2'
    os.system(convertForUComponent)

    convertForVComponent = 'sh grib2json --names --data --fp 3 --fs 103 --fv 10.0 --output ../../../../data/v_comp.json ../../../../data/data.grb2'
    os.system(convertForVComponent)

    print('[INFO] Converting from grib2 to json: SUCCESS!')

    goToData = '../../../../data'
    os.chdir(goToData)

    with open("u_comp.json") as fo:
        data1 = json.load(fo)

    if refHour == 18:
        storeRecordedDay = datetime.datetime.utcnow().day
        storeRecordedMonth = datetime.datetime.utcnow().month
        storeRecordedYear = datetime.datetime.utcnow().year
        data1[0]['recordedTime'] = str(storeRecordedYear) + '-' + "{:02d}".format(storeRecordedMonth) + '-' + "{:02d}".format(storeRecordedDay) + ' ' + "{:02d}".format(recorded_hour) + ':00:00+00'
    else:
        data1[0]['recordedTime'] = str(year) + '-' + "{:02d}".format(month) + '-' + "{:02d}".format(day) + ' ' + "{:02d}".format(recorded_hour) + ':00:00+00'

    with open("u_comp.json", "w") as fo:
        json.dump(data1, fo)

    with open("v_comp.json") as fo:
        data2 = json.load(fo)

    if refHour == 18:
        storeRecordedDay = datetime.datetime.utcnow().day
        storeRecordedMonth = datetime.datetime.utcnow().month
        storeRecordedYear = datetime.datetime.utcnow().year
        data2[0]['recordedTime'] = str(storeRecordedYear) + '-' + "{:02d}".format(storeRecordedMonth) + '-' + "{:02d}".format(storeRecordedDay) + ' ' + "{:02d}".format(recorded_hour) + ':00:00+00'
    else:
        data2[0]['recordedTime'] = str(year) + '-' + "{:02d}".format(month) + '-' + "{:02d}".format(day) + ' ' + "{:02d}".format(recorded_hour) + ':00:00+00'

    with open("v_comp.json", "w") as fo:
        json.dump(data2, fo)

    data1.append(data2[0])

    with open("wind_data.json", "w") as fo:
        json.dump(data1, fo)

    print('[INFO] Storing data onto files: SUCCESS!')

    connection, cursor = pypsqlcon.createConnection()
    if needUpdate == True:
        try:
            cursor.execute("DELETE from wind_data WHERE recorded_time = (%s)", (data1[0]['recordedTime'],))
            connection.commit()
        except (Exception, psycopg2.Error) as error:
           print("[ERROR] Error while performing query", error)
        insertWindData(data1, connection, cursor)
    else:
        insertWindData(data1, connection, cursor)

    pypsqlcon.closeConnection()

def insertWindData(data, connection, cursor):
    for i in range(len(data)):
        header = json.dumps(data[i]['header'])
        dataArray = json.dumps(data[i]['data'])
        recordedTime = data[i]['recordedTime']
        try:
            cursor.execute("INSERT INTO wind_data(recorded_time, header, data) VALUES (%s, %s, %s);", (recordedTime, header, dataArray))
            connection.commit()
        except (Exception, psycopg2.Error) as error:
            print("[ERROR] Error while performing query", error)

def getData():
    # date format: <year><month><day>
    year = current_datetime.year
    month = current_datetime.month
    day = current_datetime.day

    # refHour can be 00, 06, 12, or 18
    refHour = math.floor(current_datetime.hour / 6) * 6

    # recorded_hour ranges from 000 to 384 (0 to 16 days, every 3 hours)
    recorded_hour = math.floor(current_datetime.hour / 3) * 3

    # number of hours since last update
    hourWithinRef = abs(recorded_hour - refHour)

    for attempt in range(0, max_attempts):
        # file name format: gfs.t<hour>z.pgrb2.1p00.f<hourWithinRef>
        fileName = 'gfs.t' + "{:02d}".format(refHour) + 'z.pgrb2.1p00.f' + "{:03d}".format(hourWithinRef)
        url = "{}/gfs.{}{:02d}{:02d}/{:02d}/atmos/{}".format(NOAA_ENDPOINT, year, month, day, refHour, fileName)

        print("[INFO] Attempt {}, downloading {}".format(attempt, url))

        try:
            fetched_data = urllib2.urlopen(url)

            # Ref hour directory available
            # Convert to UTC time, since refHour 18 in local time (CST) is already a new day in UTC
            if refHour == 18:
                storeRecordedDay = datetime.datetime.utcnow().day
                storeRecordedMonth = datetime.datetime.utcnow().month
                storeRecordedYear = datetime.datetime.utcnow().year
                datetimeFormat = str(storeRecordedYear) + '-' + "{:02d}".format(storeRecordedMonth) + '-' + "{:02d}".format(storeRecordedDay) + 'T' + "{:02d}".format(recorded_hour) + ':00:00.000Z'
            else:
                datetimeFormat = str(year) + '-' + "{:02d}".format(month) + '-' + "{:02d}".format(day) + 'T' + "{:02d}".format(recorded_hour) + ':00:00.000Z'
            
            print("[INFO] Stored date: {}".format(datetimeFormat))

            # Check data existance
            check_url = API_ENDPOINT + datetimeFormat
            check_data = urllib2.urlopen(check_url)
            check_data_json = json.load(check_data)
            needUpdate = False
            if len(check_data_json) != 0:
                print("[INFO] Wind data already exists")
                storedRefTime = check_data_json[0]['header']['refTime']
                date_time_obj = datetime.datetime.strptime(storedRefTime, '%Y-%m-%dT%H:%M:%S.%fZ')
                storedRefTimeHour = date_time_obj.time().hour
                if (storedRefTimeHour < refHour) or (storedRefTimeHour == 18 and refHour == 0):
                    needUpdate = True
                else:
                    check_data.close()
                    sys.exit()
            check_data.close()
            local = './data/data.grb2'
            dataPath = os.path.join(fdir, local)
            data_file = open(dataPath, "wb")
            content = fetched_data.read()
            data_file.write(content)
            data_file.close()

            print("[INFO] Data downloaded and processed successfully! Now storing data in database...")
            convertData(year, month, day, refHour, recorded_hour, needUpdate)
        
        # The ref hour directory is not available (URL error occured)
        # So access the previous ref hour file and/or directory
        except urllib2.URLError as e:
            print("[WARN]: {}".format(e))
            print("[INFO] Checking previous file now...")
            # If reference hour is midnight
            if refHour == 0:
                # If first day of January
                if month == 1 and day == 1:
                    year -= 1           # Go back a year
                    month = 12          # Reset month to December
                    day = 31            # Go back a day to December 31st (12/31)
                    
                # If previous month has 30 days and current day is the first day of the current month
                elif (month == 5 or month == 7 or month == 8 or month == 10 or month == 12) and day == 1:
                    month -= 1          # Go back a month
                    day = 30            # Go back a day to the 30th
                
                # If current month is March and first day of the month
                elif month == 3 and day == 1:
                    
                    if year % 4 == 0:   # If leap year
                        month -= 1      # Go back a month
                        day = 29        # Go back a day to the 29th

                    else:               # If not leap year
                        month -= 1      # Go back a month
                        day = 28        # Go back a day to the 28th

                # If previous month has 31 days and it's the first day of the current month
                elif (month == 2 or month == 4 or month == 6 or month == 9 or month == 11) and day == 1:
                    month -= 1          # Go back a month
                    day = 31            # Go back a day to the 31st
                
                # If not beginning of a month
                else:
                    day -= 1            # Go back a day
                
                # Always reset refHour because we went back a day
                refHour = 18
            
            # If not midnight
            else:
                # Set ref hour back 6 hours
                refHour -= 6

# Start
getData()
