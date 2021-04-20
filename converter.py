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

current_datetime = datetime.datetime.utcnow()
noaa = 'https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/'
latLon = '&leftlon=0&rightlon=360&toplat=90&bottomlat=-90'
fdir = os.path.abspath(os.path.dirname(__file__))

# date format: <year><month><day>
year = current_datetime.year
# month = current_datetime.month
month = 3
day = current_datetime.day

# refHour can be 00, 06, 12, or 18
refHour = math.floor(current_datetime.hour / 6) * 6

# recorded_hour ranges from 000 to 384 (0 to 16 days, every 3 hours)
recorded_hour = math.floor(current_datetime.hour / 3) * 3

# number of hours since last update
hourWithinRef = abs(recorded_hour - refHour)

# used to avoid infinite loop when searching back in time for data
max_recursions = 6


def convertData(year, month, day, refHour, needUpdate):
    goToGrib2JSON = './grib2json/target/grib2json-0.8.0-SNAPSHOT/bin'
    gribPath = os.path.join(fdir, goToGrib2JSON)
    os.chdir(gribPath)
    print("goToGrib2JSON path")

    # # ** will need to group the U and V data together by time or name of file **
    # (--fp) parameterNumber: 2 (U-component_of_wind)
    # 				         3 (V-component_of_wind)
    # (--fs) Height level above ground => surface1Type: 103
    # (--fv) surface1Value: 10.0

    convertForUComponent = 'sh grib2json --names --data --fp 2 --fs 103 --fv 10.0 --output ../../../../data/u_comp.json ../../../../data/data.grb2'
    os.system(convertForUComponent)

    convertForVComponent = 'sh grib2json --names --data --fp 3 --fs 103 --fv 10.0 --output ../../../../data/v_comp.json ../../../../data/data.grb2'
    os.system(convertForVComponent)

    print('Converting from grib2 to json: SUCCESS!')

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

    print('Storing data onto files: SUCCESS!')

    connection, cursor = pypsqlcon.createConnection()
    if needUpdate == True:
        try:
            cursor.execute("DELETE from wind_data WHERE recorded_time = (%s)", (data1[0]['recordedTime'],))
            connection.commit()
        except (Exception, psycopg2.Error) as error:
           print("Error while performing query", error)
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
            print("Error while performing query", error)

def getData(year, month, day, refHour, sentinel):
    # End recursion if max_recursions is hit
    if sentinel <= 0:
        print("Data file not found. Searched back {} hours".format(max_recursions * 6))
        return

    # file name format: gfs.t<hour>z.pgrb2.1p00.f<hourWithinRef>
    fileName = 'gfs.t' + "{:02d}".format(refHour) + 'z.pgrb2.1p00.f' + "{:03d}".format(hourWithinRef)
    print("Attempt to download: " + fileName)
    url = "{}/gfs.{}{:02d}{:02d}/{:02d}/atmos/{}".format(noaa, datetime.datetime.utcnow().year, datetime.datetime.utcnow().month, datetime.datetime.utcnow().day, refHour, fileName)

    try:
        u = urllib2.urlopen(url)
    # The ref hour directory not available, access the previous ref hour directory
    except urllib2.URLError as e:
        print (e)
        # If reference hour is midnight
        if refHour == 0:
            # If first day of January
            if month == 1 and day == 1:
                # Go back a year
                # Go back a day to December 31st (12/31)
                getData(year - 1, 12, 31, 18, 5, sentinel - 1)
            # If previous month has 30 days and current day is the first day of the current month
            elif (month == 5 or month == 7 or month == 8 or month == 10 or month == 12) and day == 1:
                # Go back a month
                # Go back a day to the 30th
                getData(year, month - 1, 30, 18, sentinel - 1)
            # If current month is March and first day of the month
            elif month == 3 and day == 1:
                # If leap year
                if year % 4 == 0:
                    # Go back a month
                    # Go back a day to the 29th
                    getData(year, month - 1, 29, 18, sentinel - 1)
                # If not leap year
                else:
                    # Go back a month
                    # Go back a day to the 28th
                    getData(year, month - 1, 28, 18, sentinel - 1)
            # If previous month has 31 days and it's the first day of the current month
            elif (month == 2 or month == 4 or month == 6 or month == 9 or month == 11) and day == 1:
                # Go back a month
                # Go back a day to the 31st
                getData(year, month - 1, 31, 18, sentinel - 1)
            # If not beginning of a month
            else:
                # Go back a day
                getData(year, month, day - 1, 18, sentinel - 1)
        # If not midnight
        else:
            # Set ref hour back 6 hours
            getData(year, month, day, refHour - 6, sentinel - 1)
    # Ref hour directory available
    else:
        if refHour == 18:
            storeRecordedDay = datetime.datetime.utcnow().day
            storeRecordedMonth = datetime.datetime.utcnow().month
            storeRecordedYear = datetime.datetime.utcnow().year
            datetimeFormat = str(storeRecordedYear) + '-' + "{:02d}".format(storeRecordedMonth) + '-' + "{:02d}".format(storeRecordedDay) + 'T' + "{:02d}".format(recorded_hour) + ':00:00.000Z'
        else:
            datetimeFormat = str(year) + '-' + "{:02d}".format(month) + '-' + "{:02d}".format(day) + 'T' + "{:02d}".format(recorded_hour) + ':00:00.000Z'
        print(datetimeFormat)
        API_ENDPOINT = "https://api.sharedairdfw.com/wind_data/" + datetimeFormat
        r = urllib2.urlopen(API_ENDPOINT)
        x = json.load(r)
        needUpdate = False
        if len(x) != 0:
            print("Data already exists")
            storedRefTime = x[0]['header']['refTime']
            date_time_obj = datetime.datetime.strptime(storedRefTime, '%Y-%m-%dT%H:%M:%S.%fZ')
            storedRefTimeHour = date_time_obj.time().hour
            if (storedRefTimeHour < refHour) or (storedRefTimeHour == 18 and refHour == 0):
                needUpdate = True
            else:
                r.close()
                sys.exit()
        r.close()
        local = './data/data.grb2'
        dataPath = os.path.join(fdir, local)
        f = open(dataPath, "wb")
        content = u.read()
        f.write(content)
        f.close()
        print("Downloading data: SUCCESS!")
        convertData(year, month, day, refHour, needUpdate)

getData(year, month, day, refHour, max_recursions)
