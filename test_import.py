# ----- test_imports.py -----
import sys
import importlib
import traceback # Include traceback for more detail on unexpected errors

print(f"--- Running Import Test ---")
print(f"Using Python version: {sys.version}")
errors_found = 0

# List the *package* or *top-level module* names as typically imported
# These should align with what you installed via requirements.txt
libs_to_test = [
    'requests',
    'bs4',          # Installs beautifulsoup4, import as bs4
    'lxml',
    'psycopg2',     # psycopg2-binary installs this module name
    'pymongo',
    'praw',
    'tweepy',
    'vaderSentiment.vaderSentiment' # Specific path needed for class import later
]

print('\nChecking library imports...')
print('----------------------------')

for lib_path in libs_to_test:
    try:
        module_name_to_import = lib_path.split('.')[0] # Get the base name to import first

        print(f"- Checking {lib_path}... ", end="") # Print without newline initially

        # Attempt to import the base module
        module = importlib.import_module(module_name_to_import)

        # Add specific checks for known classes if needed for extra validation
        if module_name_to_import == 'bs4':
            from bs4 import BeautifulSoup
            print(f"✓ ({module_name_to_import} - BeautifulSoup OK)")
        elif module_name_to_import == 'vaderSentiment':
            from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
            print(f"✓ ({module_name_to_import} - SIA OK)")
        elif module_name_to_import == 'psycopg2':
             # Can optionally check db connection details here if needed, but usually just import is fine
             print(f"✓ ({module_name_to_import})") # psycopg2 doesn't have an obvious class to import simply
        else:
             # For others, just the base import is usually sufficient
             print(f"✓ ({module_name_to_import})")

    except ImportError as e:
        print(f"\n✗ FAIL: Error importing {lib_path}: {e}")
        errors_found += 1
    except Exception as e_gen:
        print(f"\n✗ FAIL: Unexpected error testing import {lib_path}: {e_gen}")
        print("--- Traceback ---")
        traceback.print_exc() # Print full traceback for unexpected errors
        print("-----------------")
        errors_found += 1

# --- Summary and Exit ---
print('----------------------------')
if errors_found > 0:
    print(f"\n>>> {errors_found} critical import errors detected. Workflow step will fail.")
    sys.exit(1) # Exit with failure code
else:
    print("\nAll required library imports were successful.")
    sys.exit(0) # Exit with success code
