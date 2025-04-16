# ----- collect_web3career.py (Updated Version - With PostgreSQL Insertion) -----
import requests
import os
import json
import time
from datetime import datetime
import psycopg2 # Import PostgreSQL driver
import sys      # To cleanly exit on major errors

print("--- Starting Web3.Career Collection Script ---")

# --- Database Connection Setup ---
db_conn = None
db_cursor = None
try:
    print("Reading POSTGRES_URI from Replit Secrets...")
    db_uri = os.environ.get('POSTGRES_URI')
    if not db_uri:
        print(">>> Error: POSTGRES_URI secret not found or is empty!")
        print(">>> Please add the connection string from Neon (or your provider) to Replit Secrets.")
        sys.exit(1) # Exit if DB URI is missing

    print("Connecting to external PostgreSQL database (Neon)...")
    db_conn = psycopg2.connect(db_uri)
    db_cursor = db_conn.cursor()
    print("Database connection successful!")

    # Verify table exists (optional check)
    db_cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'job_postings');")
    table_exists = db_cursor.fetchone()[0]
    if not table_exists:
         print(">>> Error: 'job_postings' table does not exist in the database!")
         print(">>> Please run the CREATE TABLE script in your Neon SQL Editor first.")
         sys.exit(1)
    else:
        print("'job_postings' table found.")

except Exception as db_err:
    print(f">>> Database connection error: {db_err}")
    # Ensure connection/cursor are closed if partially opened
    if db_cursor: db_cursor.close()
    if db_conn: db_conn.close()
    sys.exit(1) # Exit if DB connection fails

# --- API Connection Setup ---
api_key = None
try:
    print("\nReading WEB3_CAREER_API_KEY from Replit Secrets...")
    api_key = os.environ.get('WEB3_CAREER_API_KEY')
    if not api_key:
        print(">>> Error: WEB3_CAREER_API_KEY secret not found.")
        sys.exit(1)
    else:
         print(f"API Key loaded successfully (starts with: {api_key[:4]}...).")
except Exception as key_err:
    print(f">>> Error reading API key: {key_err}")
    sys.exit(1)


# --- API Request Configuration ---
api_endpoint = "https://web3.career/api/v1"
params = {
    'token': api_key,
    'limit': 100,
    'show_description': 'true'
    # Add other filters here if needed, e.g.: 'remote': 'true',
}
printable_params = {k: v for k, v in params.items() if k != 'token'}
print(f"\nRequesting data from: {api_endpoint}")
print(f"With parameters: {printable_params}")


# --- Fetch and Process ---
inserted_count = 0
skipped_count = 0
api_error = False

try:
    print("\nSending GET request to the API...")
    response = requests.get(api_endpoint, params=params, timeout=25)
    print(f"API request status: {response.status_code}")
    response.raise_for_status() # Check for HTTP errors

    print("Attempting to parse JSON response...")
    raw_data = response.json()
    print("JSON parsing successful.")

    jobs_list = []
    if isinstance(raw_data, list) and len(raw_data) > 2 and isinstance(raw_data[2], list):
        jobs_list = raw_data[2]
    else:
        print(">>> Warning: API response structure not as expected (list[2] not found or not a list). Check API documentation or raw response.")
        # Optional: print raw_data for deep debugging
        # print(json.dumps(raw_data, indent=2))

    print(f"\nProcessing {len(jobs_list)} potential job entries from API...")

    for job_entry in jobs_list:
        if not isinstance(job_entry, dict):
            print(f"Warning: Skipping item, not a dictionary: {job_entry}")
            skipped_count += 1
            continue

        # Extract data (use .get with default=None for safety)
        external_id = str(job_entry.get('id')) if job_entry.get('id') is not None else None
        title = job_entry.get('title')
        company = job_entry.get('company')
        location = job_entry.get('location') # Contains city/country often
        country = job_entry.get('country')
        city = job_entry.get('city')
        apply_url = job_entry.get('apply_url')
        tags_list = job_entry.get('tags', []) # Ensure it's a list
        description = job_entry.get('description')
        date_epoch = job_entry.get('date_epoch')
        # Infer remote status based on tags or location info if possible
        is_remote = 'remote' in [tag.lower() for tag in tags_list if isinstance(tag, str)] if tags_list else None
        # Add a placeholder for salary if the API provides it later
        salary = job_entry.get('salary_range') # Check actual key name

        # Prepare data for insertion
        # Only insert if we have a title and a unique URL
        if title and apply_url:
            sql_insert_query = """
                INSERT INTO job_postings (
                    title, company_name, location, salary_range, tags, source,
                    job_url, description, external_id, is_remote, date_posted_epoch,
                    raw_api_response
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (job_url) DO NOTHING;
            """
            # Use json.dumps for the raw response if storing it
            raw_json_str = json.dumps(job_entry) if job_entry else None
            data_to_insert = (
                title, company, location, salary, tags_list, 'Web3.Career',
                apply_url, description, external_id, is_remote, date_epoch,
                raw_json_str # Insert raw JSON here
            )

            try:
                db_cursor.execute(sql_insert_query, data_to_insert)
                # Check if a row was actually inserted (0 means conflict/duplicate)
                if db_cursor.rowcount > 0:
                    inserted_count += 1
                else:
                    skipped_count += 1
            except Exception as insert_err:
                print(f"  > DB insert error for job ID {external_id} ({title}): {insert_err}")
                db_conn.rollback() # Rollback failed transaction for this job
                skipped_count += 1
        else:
             print(f"Skipping job entry due to missing title or apply_url: {external_id}")
             skipped_count += 1

    # Commit all successful insertions after the loop
    db_conn.commit()
    print(f"\nDatabase commit successful.")


# --- Error Handling for API Request/Parsing ---
except requests.exceptions.HTTPError as http_err:
    print(f"\n>>> HTTP error occurred: {http_err}")
    if response is not None:
         print(f"Status Code: {response.status_code}, Response: {response.text[:500]}")
    api_error = True
except requests.exceptions.RequestException as req_err:
    print(f"\n>>> Request error occurred: {req_err}")
    api_error = True
except json.JSONDecodeError:
    print("\n>>> Error: Failed to decode JSON response from API.")
    if response is not None: print(f"Response text snippet: {response.text[:500]}")
    api_error = True
except Exception as proc_err:
    print(f"\n>>> An unexpected error occurred during processing: {proc_err}")
    api_error = True # Treat as API/Processing error
    import traceback
    traceback.print_exc()


# --- Cleanup ---
finally:
    print("\n--- Final Summary ---")
    print(f"Jobs Inserted: {inserted_count}")
    print(f"Jobs Skipped (Duplicate/Error/Incomplete): {skipped_count}")
    if api_error:
         print(">>> There was an error fetching or processing data from the API.")

    print("Closing database connection...")
    if db_cursor: db_cursor.close()
    if db_conn: db_conn.close()
    print("Database connection closed.")
    print("\n--- Web3.Career Collection Script Finished ---")# ----- collect_web3career.py (Updated Version - With PostgreSQL Insertion) -----
import requests
import os
import json
import time
from datetime import datetime
import psycopg2 # Import PostgreSQL driver
import sys      # To cleanly exit on major errors

print("--- Starting Web3.Career Collection Script ---")

# --- Database Connection Setup ---
db_conn = None
db_cursor = None
try:
    print("Reading POSTGRES_URI from Replit Secrets...")
    db_uri = os.environ.get('POSTGRES_URI')
    if not db_uri:
        print(">>> Error: POSTGRES_URI secret not found or is empty!")
        print(">>> Please add the connection string from Neon (or your provider) to Replit Secrets.")
        sys.exit(1) # Exit if DB URI is missing

    print("Connecting to external PostgreSQL database (Neon)...")
    db_conn = psycopg2.connect(db_uri)
    db_cursor = db_conn.cursor()
    print("Database connection successful!")

    # Verify table exists (optional check)
    db_cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'job_postings');")
    table_exists = db_cursor.fetchone()[0]
    if not table_exists:
         print(">>> Error: 'job_postings' table does not exist in the database!")
         print(">>> Please run the CREATE TABLE script in your Neon SQL Editor first.")
         sys.exit(1)
    else:
        print("'job_postings' table found.")

except Exception as db_err:
    print(f">>> Database connection error: {db_err}")
    # Ensure connection/cursor are closed if partially opened
    if db_cursor: db_cursor.close()
    if db_conn: db_conn.close()
    sys.exit(1) # Exit if DB connection fails

# --- API Connection Setup ---
api_key = None
try:
    print("\nReading WEB3_CAREER_API_KEY from Replit Secrets...")
    api_key = os.environ.get('WEB3_CAREER_API_KEY')
    if not api_key:
        print(">>> Error: WEB3_CAREER_API_KEY secret not found.")
        sys.exit(1)
    else:
         print(f"API Key loaded successfully (starts with: {api_key[:4]}...).")
except Exception as key_err:
    print(f">>> Error reading API key: {key_err}")
    sys.exit(1)


# --- API Request Configuration ---
api_endpoint = "https://web3.career/api/v1"
params = {
    'token': api_key,
    'limit': 100,
    'show_description': 'true'
    # Add other filters here if needed, e.g.: 'remote': 'true',
}
printable_params = {k: v for k, v in params.items() if k != 'token'}
print(f"\nRequesting data from: {api_endpoint}")
print(f"With parameters: {printable_params}")


# --- Fetch and Process ---
inserted_count = 0
skipped_count = 0
api_error = False

try:
    print("\nSending GET request to the API...")
    response = requests.get(api_endpoint, params=params, timeout=25)
    print(f"API request status: {response.status_code}")
    response.raise_for_status() # Check for HTTP errors

    print("Attempting to parse JSON response...")
    raw_data = response.json()
    print("JSON parsing successful.")

    jobs_list = []
    if isinstance(raw_data, list) and len(raw_data) > 2 and isinstance(raw_data[2], list):
        jobs_list = raw_data[2]
    else:
        print(">>> Warning: API response structure not as expected (list[2] not found or not a list). Check API documentation or raw response.")
        # Optional: print raw_data for deep debugging
        # print(json.dumps(raw_data, indent=2))

    print(f"\nProcessing {len(jobs_list)} potential job entries from API...")

    for job_entry in jobs_list:
        if not isinstance(job_entry, dict):
            print(f"Warning: Skipping item, not a dictionary: {job_entry}")
            skipped_count += 1
            continue

        # Extract data (use .get with default=None for safety)
        external_id = str(job_entry.get('id')) if job_entry.get('id') is not None else None
        title = job_entry.get('title')
        company = job_entry.get('company')
        location = job_entry.get('location') # Contains city/country often
        country = job_entry.get('country')
        city = job_entry.get('city')
        apply_url = job_entry.get('apply_url')
        tags_list = job_entry.get('tags', []) # Ensure it's a list
        description = job_entry.get('description')
        date_epoch = job_entry.get('date_epoch')
        # Infer remote status based on tags or location info if possible
        is_remote = 'remote' in [tag.lower() for tag in tags_list if isinstance(tag, str)] if tags_list else None
        # Add a placeholder for salary if the API provides it later
        salary = job_entry.get('salary_range') # Check actual key name

        # Prepare data for insertion
        # Only insert if we have a title and a unique URL
        if title and apply_url:
            sql_insert_query = """
                INSERT INTO job_postings (
                    title, company_name, location, salary_range, tags, source,
                    job_url, description, external_id, is_remote, date_posted_epoch,
                    raw_api_response
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (job_url) DO NOTHING;
            """
            # Use json.dumps for the raw response if storing it
            raw_json_str = json.dumps(job_entry) if job_entry else None
            data_to_insert = (
                title, company, location, salary, tags_list, 'Web3.Career',
                apply_url, description, external_id, is_remote, date_epoch,
                raw_json_str # Insert raw JSON here
            )

            try:
                db_cursor.execute(sql_insert_query, data_to_insert)
                # Check if a row was actually inserted (0 means conflict/duplicate)
                if db_cursor.rowcount > 0:
                    inserted_count += 1
                else:
                    skipped_count += 1
            except Exception as insert_err:
                print(f"  > DB insert error for job ID {external_id} ({title}): {insert_err}")
                db_conn.rollback() # Rollback failed transaction for this job
                skipped_count += 1
        else:
             print(f"Skipping job entry due to missing title or apply_url: {external_id}")
             skipped_count += 1

    # Commit all successful insertions after the loop
    db_conn.commit()
    print(f"\nDatabase commit successful.")


# --- Error Handling for API Request/Parsing ---
except requests.exceptions.HTTPError as http_err:
    print(f"\n>>> HTTP error occurred: {http_err}")
    if response is not None:
         print(f"Status Code: {response.status_code}, Response: {response.text[:500]}")
    api_error = True
except requests.exceptions.RequestException as req_err:
    print(f"\n>>> Request error occurred: {req_err}")
    api_error = True
except json.JSONDecodeError:
    print("\n>>> Error: Failed to decode JSON response from API.")
    if response is not None: print(f"Response text snippet: {response.text[:500]}")
    api_error = True
except Exception as proc_err:
    print(f"\n>>> An unexpected error occurred during processing: {proc_err}")
    api_error = True # Treat as API/Processing error
    import traceback
    traceback.print_exc()


# --- Cleanup ---
finally:
    print("\n--- Final Summary ---")
    print(f"Jobs Inserted: {inserted_count}")
    print(f"Jobs Skipped (Duplicate/Error/Incomplete): {skipped_count}")
    if api_error:
         print(">>> There was an error fetching or processing data from the API.")

    print("Closing database connection...")
    if db_cursor: db_cursor.close()
    if db_conn: db_conn.close()
    print("Database connection closed.")
    print("\n--- Web3.Career Collection Script Finished ---")