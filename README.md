## Google Geocoder

This Python script takes an input CSV of addresses and iteratively outputs a CSV of geocoded lat / long values. Fields needed in the input CSV: street address and one of either county name or standard city / state / ZIP fields.

For testing, the repo already includes a sample CSV (test_data.csv) with fields for address, city, state, zip, and county name. The first python script, 1-prep.py, is the necessary first step to prep the input file. This script will create a 'url' field that will be run through a headless Selenium browser in 2-geocoded.py. Only run this second script after having run the first script without error.

The second python script will iterate through the input CSV and will produce an incremental output file. That is, each successfully geocoded address will be added, one at a time, to the output CSV instead of outputting all at once at the end of the script.

In the event the script is unable to geolocate an address, it will use the 'fallback.csv' to instead get the lat / long values of the city in which the address is located.

See 'requirements.txt' for required python libraries.
