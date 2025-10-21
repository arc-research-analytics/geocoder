import csv
import pandas as pd
import re
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from time import sleep
import os
import traceback
import random
import sys

# Define constants
INPUT_CSV = 'test_data_ready.csv'
OUTPUT_CSV = 'test_data_geocoded.csv'
UNIQUE_ID = 'id'  # Column in input CSV that contains unique identifiers
MAX_FAILURE_RATE = 15.0  # Maximum allowed failure rate 
MIN_ADDRESSES_FOR_FAILURE_CHECK = 35  # Only check after processing this many addresses

#############################################
# HELPER FUNCTIONS
#############################################

# This function introduces randomized delays between requests to avoid detection
# and rate limiting when making multiple calls to Google Maps.
# The delay time varies between 3.8 and 8 seconds to appear more human-like.
def get_random_delay():
    """Return a random delay between 3.8 and 8 seconds"""
    return random.uniform(3.8, 8.0)

# This function extracts geographic coordinates from a Google Maps URL.
# It uses regex to find the latitude and longitude values embedded in the URL.
# Returns the coordinates and a success flag to indicate if extraction was successful.
def parse_coordinates(url):
    """Extract latitude and longitude from a Google Maps URL"""
    try:
        found = re.search('/@(.+?),17z', url).group(1)
        lat = found.split(',')[0]
        lng = found.split(',')[1]
        return lat, lng, True
    except:
        return 'error', 'error', False

# This is the core geocoding function that handles both primary and fallback geocoding.
# It first attempts to geocode the full address URL, and if that fails,
# it tries to match the city and state to the fallback CSV lookup dictionary.
# The function handles errors gracefully and returns a structured result with
# coordinates and status information.
def geocode_address(address_row, driver, fallback_dict, skip_address_print=False):
    """Geocode a single address and return the results"""
    address = address_row.get('Address', 'Unknown Address')
    unique_id = address_row.get(UNIQUE_ID, 'Unknown ID')
    
    # Skip printing the address since we already printed it in the main function
    if not skip_address_print:
        print(f"🔍 Attempting to geocode {address}...")
    
    try:
        # Try primary URL first (full address)
        driver.get(address_row['url'])
        delay = get_random_delay()
        sleep(delay)
        current_url = driver.current_url
        
        # Parse coordinates from primary URL
        lat, lng, success = parse_coordinates(current_url)
        source_url = address_row['url']
        geocode_level = 'rooftop'

        # If primary URL failed, try fallback using city/state from CSV
        if not success:
            print(f"⚠️ Primary geocoding failed. Trying city-level fallback from CSV...")

            # Get city and state from address_row
            city = address_row.get('city', '').strip().upper()
            state = address_row.get('state', '').strip().upper()

            # Check if city/state exists in fallback dictionary
            if (city, state) in fallback_dict:
                lat = str(fallback_dict[(city, state)]['lat'])
                lng = str(fallback_dict[(city, state)]['lng'])
                geocode_level = 'city'
                print(f"✅ Successfully geocoded using city-level fallback ({city}, {state})!")
            else:
                geocode_level = 'none'
                print(f"❌ City/state combination ({city}, {state}) not found in fallback CSV.")
        else:
            print(f"✅ Successfully geocoded at rooftop level!")
        
        # Create result dictionary with original data plus coordinates
        geocoded_info = {
            UNIQUE_ID: unique_id,
            'Address': address,
            'url': source_url,
            'returned_url': current_url,
            'lat': lat,
            'long': lng,
            'geocode_level': geocode_level,
            'status': 'success' if (success or geocode_level == 'city') else 'failed'
        }
        
        return geocoded_info
        
    except Exception as e:
        error_details = traceback.format_exc()
        print(f"❌ Error geocoding {address}: {str(e)}")
        print(f"📋 Error details: {error_details.splitlines()[-1]}")
        
        # Return error result
        geocoded_info = {
            UNIQUE_ID: unique_id,
            'Address': address,
            'url': address_row['url'],
            'returned_url': 'error',
            'lat': 'error',
            'long': 'error',
            'geocode_level': 'none',
            'status': f'error: {str(e)}'
        }
        return geocoded_info

# This function saves a successfully geocoded address to the output CSV file.
# It handles creating the file with headers if it doesn't exist,
# and appends new results to existing files.
# The function removes internal status fields before writing to CSV.
def save_geocoded_address(geocoded_info):
    """Save a single geocoded address to CSV without the status field"""
    file_exists = os.path.exists(OUTPUT_CSV)
    write_header = not file_exists or os.path.getsize(OUTPUT_CSV) == 0

    # Create a copy of geocoded_info without the status field
    output_info = {k: v for k, v in geocoded_info.items() if k not in ['status']}

    try:
        with open(OUTPUT_CSV, 'a', newline='', encoding='utf-8') as csvfile:
            fieldnames = [UNIQUE_ID, 'Address', 'url', 'returned_url', 'lat', 'long', 'geocode_level']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if write_header:
                writer.writeheader()
            writer.writerow(output_info)
            csvfile.flush()
            os.fsync(csvfile.fileno())
    except Exception as e:
        print(f"❌ Error writing to CSV: {str(e)}")

# This function monitors the geocoding process for excessive failures.
# If the failure rate exceeds the defined threshold, it will trigger
# an auto-shutoff to prevent wasting resources on likely systemic issues.
# Only activates after processing a minimum number of addresses.
def check_failure_rate(success_count, failure_count):
    """Check if failure rate exceeds threshold and return True if process should stop"""
    total = success_count + failure_count
    
    # Only check if we have processed enough addresses
    if total < MIN_ADDRESSES_FOR_FAILURE_CHECK:
        return False
    
    failure_rate = (failure_count / total) * 100
    
    if failure_rate > MAX_FAILURE_RATE:
        print(f"\n🚨 CRITICAL: Failure rate of {failure_rate:.1f}% exceeds maximum allowed {MAX_FAILURE_RATE}%")
        print("🛑 Auto-shutoff triggered. Terminating geocoding process.")
        return True
    
    return False

# This function ensures that the unique ID column specified in the constants
# actually exists in the input data. If not, it displays available columns
# and exits the program with helpful information.
def validate_unique_id_field(df):
    """Validate that the UNIQUE_ID column exists in the dataframe"""
    if UNIQUE_ID not in df.columns:
        available_columns = ", ".join(df.columns)
        print(f"❌ Error: UNIQUE_ID column '{UNIQUE_ID}' not found in input CSV.")
        print(f"Available columns: {available_columns}")
        print("Please set the UNIQUE_ID constant at the top of the script to one of these columns.")
        sys.exit(1)

# main function
def main():
    # Load all addresses to geocode
    universe = pd.read_csv(INPUT_CSV)
    validate_unique_id_field(universe)

    print(f"📊 {len(universe):,} total addresses to geocode in this batch!")

    # Load fallback CSV and create lookup dictionary
    try:
        fallback_df = pd.read_csv('fallback.csv')
        fallback_dict = {}
        for _, row in fallback_df.iterrows():
            city = str(row['city']).strip().upper()
            state = str(row['state']).strip().upper()
            fallback_dict[(city, state)] = {
                'lat': row['latitude'],
                'lng': row['longitude']
            }
        print(f"✅ Loaded {len(fallback_dict):,} city/state combinations from fallback.csv")
    except Exception as e:
        print(f"⚠️ Warning: Could not load fallback.csv: {str(e)}")
        print("Continuing without fallback option...")
        fallback_dict = {}

    # Check what's already been geocoded
    if os.path.exists(OUTPUT_CSV):
        try:
            geocoded_addresses = pd.read_csv(OUTPUT_CSV)
            
            # Validate that the UNIQUE_ID column exists in the output CSV
            if UNIQUE_ID not in geocoded_addresses.columns:
                print(f"⚠️ Warning: UNIQUE_ID column '{UNIQUE_ID}' not found in output CSV.")
                print("Starting geocoding from scratch to include this column.")
                geocoded_addresses = pd.DataFrame()
            else:
                print(f"📝 {len(geocoded_addresses):,} addresses already geocoded")
                
                # Find addresses that haven't been geocoded yet using the unique ID
                geocoded_ids = set(geocoded_addresses[UNIQUE_ID].astype(str).tolist())
                ungeocoded_addresses = universe[~universe[UNIQUE_ID].astype(str).isin(geocoded_ids)]
                print(f"🔍 Found {len(ungeocoded_addresses):,} addresses that need geocoding")
        except Exception as e:
            print(f"⚠️ Error reading output CSV: {str(e)}")
            print("Starting geocoding from scratch.")
            geocoded_addresses = pd.DataFrame()
            ungeocoded_addresses = universe
    else:
        # Starting from scratch
        ungeocoded_addresses = universe
        geocoded_addresses = pd.DataFrame()  # Empty dataframe
        print("🚀 Starting geocoding from scratch")

    if len(ungeocoded_addresses) == 0:
        print("✅ All addresses have already been geocoded!")
        return

    # Set up Chrome driver
    options = Options()
    options.add_argument("--headless=new")
    # Make headless Chrome look more like a real browser
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    
    # Execute CDP commands to prevent detection
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": """
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
        """
    })

    total_addresses = len(universe)
    ungeocoded_count = len(ungeocoded_addresses)
    geocoded_so_far = len(geocoded_addresses) if not geocoded_addresses.empty else 0
    address_count = 0
    success_count = 0
    failure_count = 0
    fallback_success_count = 0
    consecutive_failures = 0
    max_consecutive_failures = 5

    print(f"🚀 Starting geocoding for {ungeocoded_count:,} addresses")
    
    try:
        # Process each ungeocoded address
        for _, address_row in ungeocoded_addresses.iterrows():
            address_count += 1
            current_count = geocoded_so_far + address_count
            print("------------------------------------------------")
            
            # Get the address and ID for the combined print statement
            address = address_row.get('Address', 'Unknown Address')
            unique_id = address_row.get(UNIQUE_ID, 'Unknown ID')
            print(f"🔍 Processing {address} (ID: {unique_id}, {address_count:,}/{ungeocoded_count:,} - {(address_count/ungeocoded_count)*100:.1f}% complete)")
            
            geocoded_info = geocode_address(address_row, driver, fallback_dict, skip_address_print=True)
            save_geocoded_address(geocoded_info)
            sleep(0.5)  # Ensure CSV is written properly before next address
            
            if geocoded_info['status'] == 'success':
                success_count += 1
                consecutive_failures = 0

                # Track city-level geocoding successes
                if geocoded_info.get('geocode_level') == 'city':
                    fallback_success_count += 1
            else:
                failure_count += 1
                consecutive_failures += 1
            
            # Calculate the current failure rate
            total_processed = success_count + failure_count
            failure_rate = (failure_count / total_processed) * 100 if total_processed > 0 else 0
            
            print(f"📊 Current session: {success_count} successful, {failure_count} failed")
            if fallback_success_count > 0:
                print(f"🔄 City-level fallback used successfully: {fallback_success_count} times")
            print(f"📉 Failure rate: {failure_rate:.1f}%")
            
            # Check if we should stop due to high failure rate
            if check_failure_rate(success_count, failure_count):
                break
            
            # If we have too many consecutive failures, take a longer break
            if consecutive_failures >= max_consecutive_failures:
                cooldown = random.uniform(20, 30)
                print(f"⚠️ {consecutive_failures} consecutive failures detected. Taking a longer break ({cooldown:.1f} seconds)...")
                sleep(cooldown)
                consecutive_failures = 0
                
                # Restart the browser session to get a fresh state
                print("🔄 Restarting browser session...")
                driver.quit()
                driver = webdriver.Chrome(service=service, options=options)
                driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
                    "source": """
                        Object.defineProperty(navigator, 'webdriver', {
                            get: () => undefined
                        });
                    """
                })
            
    except KeyboardInterrupt:
        print("\n⚠️ Process interrupted by user. Saving progress...")
    except Exception as e:
        print(f"\n❌ Unexpected error: {str(e)}")
        print(traceback.format_exc())
    finally:
        print("\n🔒 Closing Chrome driver...")
        driver.quit()

    # Print summary statistics
    try:
        final_results = pd.read_csv(OUTPUT_CSV)
        # Determine success/failure based on lat column instead of status
        successful = len(final_results[final_results['lat'] != 'error'])
        failed = len(final_results[final_results['lat'] == 'error'])
        total = len(final_results)
        failure_rate = (failed / total) * 100 if total > 0 else 0

        print(f"\n📊 Geocoding Summary:")
        print(f"✅ Successfully geocoded: {successful:,}")
        if fallback_success_count > 0:
            print(f"🔄 Used city-level fallback successfully: {fallback_success_count:,}")
        print(f"❌ Failed to geocode: {failed:,}")
        print(f"📈 Success rate: {(successful/total)*100:.1f}%")
        print(f"📉 Failure rate: {failed}/{total} ({failure_rate:.1f}%)")
        print(f"🔧 Addresses needing manual geocoding: {failed:,}")
        
        # Check if all addresses were processed
        if len(final_results) == len(universe):
            print("✅ All addresses from the input file were processed!")
        else:
            print(f"⚠️ Note: {len(universe) - len(final_results):,} addresses from the input file were not processed.")
    except Exception as e:
        print(f"❌ Error reading final results: {str(e)}")

if __name__ == '__main__':
    main()
    print("✅ Geocoding complete!")