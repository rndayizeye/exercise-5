from sodapy import Socrata
import datetime
import json # Still useful if you want to inspect raw data, but not strictly for CSV saving
import os
import csv # Import the csv module
import pandas as pd

# Configuration
DATASET_ID = "h9gi-nx95"
BASE_URL = "data.cityofnewyork.us"
LAST_UPDATE_FILE = "last_crash_update.txt"
OUTPUT_DIR = "nyc_crash_data" # Directory to save CSV files

def get_last_update_date():
    """Reads the last update date from a file."""
    if os.path.exists(LAST_UPDATE_FILE):
        with open(LAST_UPDATE_FILE, 'r') as f:
            date_str = f.readline().strip()
            if date_str:
                return datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
    return None

def save_last_update_date(date_obj):
    """Saves the last update date to a file."""
    with open(LAST_UPDATE_FILE, 'w') as f:
        f.write(date_obj.strftime('%Y-%m-%d'))

def fetch_new_crashes():
    """Fetches new crash data since the last update and saves it to a CSV file."""
    client = Socrata(BASE_URL, None, timeout = 1000) # Consider adding an app token for better rate limits

    last_update = get_last_update_date()
    today = datetime.date.today()

    if last_update is None:
        print("No previous update found. Fetching data from the beginning of yesterday.")
        start_date = datetime.timedelta(input("start data yyyy-mm-dd:")) # Fetch yesterday's data on first run
    else:
        # Fetch data starting from the day *after* the last recorded update
        print(f"Last update was on {last_update}. Fetching data since {last_update + datetime.timedelta(days=1)}.")
        start_date = last_update + datetime.timedelta(days=1)

    # Ensure the output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Format the start date for the API query
    # Socrata expects ISO 8601 format for date queries
    # To get data from a specific date forward, use >=
    query = f"crash_date >= '{start_date.isoformat()}T00:00:00.000'"

    try:
        # Fetch all results matching the query
        # Socrata's API has a default limit. For large datasets, you might need to paginate.
        # However, for daily updates, the number of new records daily is usually manageable.
        # If you hit the limit (e.g., 50,000 or 100,000 records), you'd need to loop and increment offset.
        # For simplicity here, we assume daily new records are below a reasonable limit.
        results = client.get(DATASET_ID, where=query,limit=5_000_000) # Increased limit for daily fetch

        num_results = len(results)
        print(f"Fetched {num_results} new crash records since {start_date}.")

        if results:
            # Define the output CSV file name
            output_csv_filename = os.path.join(OUTPUT_DIR, f"nyc_crashes_{today.strftime('%Y-%m-%d')}.csv")

            df = pd.DataFrame(results)
            df.to_csv(output_csv_filename)

            '''# Determine fieldnames (CSV header) from the first record
            # Socrata APIs can have varying fields, so dynamically getting them is robust.
            fieldnames = list(results[0].keys())

            with open(output_csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

                writer.writeheader() # Write the header row
                for row in results:
                    writer.writerow(row) # Write each row of data'''

            print(f"New crash data saved to: {output_csv_filename}")

            # Update the last updated date only if data was fetched
            save_last_update_date(today)
            print(f"Last update date saved as {today.strftime('%Y-%m-%d')}.")
        else:
            print("No new crash records found to save.")

    except Exception as e:
        print(f"An error occurred while fetching or saving data: {e}")
        # It's good practice to NOT update last_update_file if an error occurred

if __name__ == "__main__":
    fetch_new_crashes()