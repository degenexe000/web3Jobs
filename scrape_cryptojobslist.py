# ----- scrape_cryptojobslist.py (Updated Version 5 - With PostgreSQL Insertion) -----
import requests
from bs4 import BeautifulSoup
import time
import json
from urllib.parse import urljoin
import re
import psycopg2 # Import PostgreSQL driver
import os
import sys      # To cleanly exit on major errors
from datetime import datetime # For timestamp

print("--- Starting CryptoJobsList Scraper ---")

# --- Database Connection Setup ---
db_conn = None
db_cursor = None
try:
    print("Reading POSTGRES_URI from Replit Secrets...")
    db_uri = os.environ.get('POSTGRES_URI')
    if not db_uri:
        print(">>> Error: POSTGRES_URI secret not found or is empty!")
        sys.exit(1)

    print("Connecting to external PostgreSQL database (Neon)...")
    db_conn = psycopg2.connect(db_uri)
    db_cursor = db_conn.cursor()
    print("Database connection successful!")

    # Verify table exists
    db_cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'job_postings');")
    table_exists = db_cursor.fetchone()[0]
    if not table_exists:
         print(">>> Error: 'job_postings' table does not exist! Run CREATE TABLE script first.")
         sys.exit(1)
    else:
        print("'job_postings' table found.")

except Exception as db_err:
    print(f">>> Database connection error: {db_err}")
    if db_cursor: db_cursor.close()
    if db_conn: db_conn.close()
    sys.exit(1)


# --- Scraper Configuration ---
BASE_URL = 'https://cryptojobslist.com'
target_url = 'https://cryptojobslist.com/' # Scraping homepage
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
}
REQUEST_TIMEOUT = 25
POLITENESS_DELAY = 2

# --- Scrape and Insert ---
inserted_count = 0
skipped_count = 0
api_error = False # Reusing variable name, here means scraping error

try:
    # Step 1: Fetch HTML
    print(f"\nAttempting to scrape: {target_url}")
    response = requests.get(target_url, headers=headers, timeout=REQUEST_TIMEOUT)
    print(f"Request sent. Status Code: {response.status_code}")
    response.raise_for_status()
    print("Successfully fetched page.")

    # Step 2: Parse HTML
    soup = BeautifulSoup(response.text, 'lxml')

    # Step 3: Find Job Rows
    table_body_selector = 'table.job-preview-inline-table tbody'
    table_body = soup.select_one(table_body_selector)

    if not table_body:
        print(f"\n>>> ERROR: Could not find table body using selector: '{table_body_selector}'")
        raise Exception("Table body not found, cannot proceed.") # Raise exception to trigger finally block

    job_row_selector = 'tr[role="button"]'
    job_rows = table_body.select(job_row_selector)
    print(f"\nFound {len(job_rows)} potential job rows using selector '{job_row_selector}'.")

    if not job_rows:
        print("\n>>> Warning: No job rows found.")
    else:
        print(f"\nProcessing {len(job_rows)} potential job rows...")

    # Step 4: Loop through rows, extract data, and insert into DB
    for row_index, row in enumerate(job_rows):
        if row.has_attr('class') and 'notAJobAd' in row['class']:
            continue # Skip ads

        # Extract data using previously validated logic
        title_element = row.select_one('a.job-title-text')
        company_element = row.select_one('a.job-company-name-text')
        link_element = title_element
        tag_elements = row.select('td.job-tags span.category')

        title = title_element.get_text(strip=True) if title_element else 'N/A'
        company = company_element.get_text(strip=True) if company_element else 'N/A'
        tags_list = [tag.get_text(strip=True) for tag in tag_elements] if tag_elements else []
        relative_link = link_element['href'] if link_element and link_element.has_attr('href') else None
        job_url = urljoin(BASE_URL, relative_link) if relative_link else 'N/A'

        salary = 'N/A'
        salary_span = row.select_one('td span.align-middle')
        if salary_span:
             parent_div = salary_span.find_parent('div')
             if parent_div and parent_div.select_one('svg[stroke="currentColor"]'):
                  salary = salary_span.get_text(strip=True)

        location = 'N/A'
        potential_loc_td = None
        tags_td = row.select_one('td.job-tags')
        location_tds = row.select('td')
        if tags_td:
            potential_loc_td = tags_td.find_previous_sibling('td')
        elif len(location_tds) >= 5:
            potential_loc_td = location_tds[4]
        if potential_loc_td:
             location_span = potential_loc_td.select_one('span.text-sm')
             if location_span:
                  raw_location_text = location_span.get_text(strip=True)
                  if salary == 'N/A' or salary != raw_location_text:
                       location = re.sub(r'^\s*📍\s*', '', raw_location_text).strip()
        if location == 'N/A' and 'Remote' in tags_list:
             location = 'Remote'
        is_remote = location == 'Remote' or 'Remote' in tags_list


        # Insert data into PostgreSQL
        if title != 'N/A' and job_url != 'N/A':
            sql_insert_query = """
                INSERT INTO job_postings (
                    title, company_name, location, salary_range, tags, source,
                    job_url, is_remote, collected_at
                    -- external_id, description could be added if scraped from detail page later
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (job_url) DO NOTHING;
            """
            # Get current timestamp for collected_at
            collected_timestamp = datetime.utcnow()

            data_to_insert = (
                title, company, location, salary if salary != 'N/A' else None, tags_list, 'CryptoJobsList',
                job_url, is_remote, collected_timestamp
            )

            try:
                db_cursor.execute(sql_insert_query, data_to_insert)
                if db_cursor.rowcount > 0:
                    inserted_count += 1
                else:
                    skipped_count += 1 # Likely duplicate based on job_url
            except Exception as insert_err:
                print(f"  > DB insert error for job URL {job_url}: {insert_err}")
                db_conn.rollback() # Rollback failed transaction
                skipped_count += 1
        else:
            print(f"Skipping row - Missing title or URL. Title: {title}, URL: {job_url}")
            skipped_count += 1

    # Commit all successful insertions after the loop
    if inserted_count > 0:
        print(f"\nAttempting to commit {inserted_count} insertions...")
        db_conn.commit()
        print("Database commit successful.")
    else:
        print("\nNo new jobs were inserted (they might be duplicates or had errors).")


# --- Error Handling ---
except requests.exceptions.HTTPError as http_err:
    print(f"\n>>> HTTP error occurred: {http_err}")
    print(f"Verify the target URL is correct: {target_url}")
    api_error = True
except requests.exceptions.RequestException as req_err:
    print(f"\n>>> Request error occurred: {req_err}")
    api_error = True
except Exception as proc_err:
    print(f"\n>>> An unexpected error occurred during scraping/processing: {proc_err}")
    api_error = True
    import traceback
    traceback.print_exc()

# --- Cleanup ---
finally:
    print("\n--- Final Summary ---")
    print(f"Jobs Inserted: {inserted_count}")
    print(f"Jobs Skipped (Duplicate/Error/Incomplete): {skipped_count}")
    if api_error:
         print(">>> There was an error fetching or processing data from the website.")

    print("Closing database connection...")
    if db_cursor: db_cursor.close()
    if db_conn: db_conn.close()
    print("Database connection closed.")
    print("\n--- CryptoJobsList Scraper Finished ---")