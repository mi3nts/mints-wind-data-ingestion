# mints-wind-data-ingestion
This was separated from mints-noaa-api on August 12th, 2020. 
This python script gets wind data from NOAA and converts it into data that can be used for display on the sharedairdfw.com website.

### Usage
* Python 2, PostgreSQL 10+ is required
  * Python modules (install with pip):
    * requests
    * psycopg2
* Check pypsqlcon-template.py to make sure the connection information is accurate and rename it to "pypsqlcon.py" afterwards.
* Both converter.py and deleteOld.py should be run on a cronjob that is executed every 6 hours.
