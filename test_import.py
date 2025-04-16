import sys
import importlib
import traceback

print(f"--- Running Import Test ---")
print(f'Using Python version: {sys.version}')
errors_found = 0

# List the *package* or *top-level module* names as installed by pip/uv
libs_to_test = [
    'requests',
    'bs4', # Installs beautifulsoup4, import as bs4
    'lxml',
    'psycopg2', # psycopg2-binary installs this module name
    'pymongo',
    'praw',
    'tweepy',
    'vaderSentiment' # Installs vaderSentiment, main class is in vaderSentiment.vaderSentiment
]

print('\nChecking imports:')
for lib_name in libs_to_test:
    print(f"- Checking {lib_name}...")
    try:
        # Use importlib to try importing the module by name
        importlib.import_module(lib_name)
        print(f'  ✓ Import successful: {lib_name}')

        # Add specific class/function import tests for confirmation if desired
        if lib_name == 'vaderSentiment':
            from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
            print(f'    ✓ Verified SentimentIntensityAnalyzer class import')
        elif lib_name == 'bs4':
            from bs4 import BeautifulSoup
            print(f'    ✓ Verified BeautifulSoup class import')
        # Add others if needed

    except ImportError as e:
        print(f'  ✗ ERROR importing {lib_name}: {e}')
        errors_found += 1
    except Exception as e_gen: # Catch other potential errors during import test
        print(f'  ✗ UNEXPECTED ERROR testing import {lib_name}: {e_gen}')
        traceback.print_exc() # Print full traceback for unexpected errors
        errors_found += 1

# --- Final Check and Exit ---
print("\n--- Import Test Summary ---")
if errors_found > 0:
    print(f'>>> {errors_found} critical import errors detected. Failing workflow step.')
    sys.exit(1) # Exit with a non-zero code to fail the GitHub Actions step
else:
    print('All required library imports successful.')
    sys.exit(0) # Exit with success code
