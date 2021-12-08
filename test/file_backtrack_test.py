# ** NOT A UNIT TEST **
# Used to test file backtracking algorithm
# Changes need to be made if necessary if wanting to use or develop from it in the future

# Script is simply runnable w/o additional setup to check what files will be checked from today
#   for debugging purposes

import datetime
import math

current_datetime = datetime.datetime.utcnow()
NOAA_ENDPOINT = 'https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod'

def test():
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
    for attempt in range(0, 40):
        # file name format: gfs.t<hour>z.pgrb2.1p00.f<hourWithinRef>
        fileName = 'gfs.t' + "{:02d}".format(refHour) + 'z.pgrb2.1p00.f' + "{:03d}".format(hourWithinRef)
        url = "{}/gfs.{}{:02d}{:02d}/{:02d}/atmos/{}".format(NOAA_ENDPOINT, year, month, day, refHour, fileName)

        print("Attempt {}, downloading {}".format(attempt, url))

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

test()