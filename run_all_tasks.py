# ----- run_all_tasks.py -----
import subprocess
import time
import sys
from datetime import datetime

# List of scripts to run in order
scripts_to_run = [
    'collect_web3career.py',
    'scrape_cryptojobslist.py',
    'collect_reddit.py',
    'collect_twitter.py',
    'process_sentiment.py'
]

print(f"--- Starting Task Runner at {datetime.utcnow().isoformat()} ---")

for script_name in scripts_to_run:
    print(f"\n>>> Running script: {script_name} <<<")
    start_time = time.time()
    try:
        # Use subprocess to run each script using python3
        # capture_output=True gets stdout/stderr, text=True decodes it
        # check=True raises CalledProcessError if script exits with non-zero code
        process = subprocess.run(
            [sys.executable, script_name], # Use sys.executable to ensure correct python version
            capture_output=True,
            text=True,
            check=True,
            timeout=900 # Set a timeout (e.g., 15 minutes) per script
        )
        # Print the output from the script
        print(f"--- Output from {script_name} ---")
        print(process.stdout)
        if process.stderr: # Print errors if any occurred
             print(f"--- Errors from {script_name} ---")
             print(process.stderr)
        print(f"--- Finished {script_name} ---")

    except subprocess.CalledProcessError as e:
        # Script exited with an error code
        print(f">>> Error running {script_name}: Exited with code {e.returncode}")
        print(f"--- STDOUT ---:\n{e.stdout}")
        print(f"--- STDERR ---:\n{e.stderr}")
        # Decide if you want to stop the whole process or continue
        # continue
        break # Stop if one script fails catastrophically
    except subprocess.TimeoutExpired as e:
         print(f">>> Timeout running {script_name} after {e.timeout} seconds.")
         print(f"--- STDOUT ---:\n{e.stdout}")
         print(f"--- STDERR ---:\n{e.stderr}")
         break # Stop if one script times out
    except Exception as e:
        # Catch other potential errors during subprocess run
         print(f">>> Unexpected error trying to run {script_name}: {e}")
         break # Stop on unexpected errors

    end_time = time.time()
    print(f"Script {script_name} took {end_time - start_time:.2f} seconds.")
    # Optional short pause between scripts
    time.sleep(5)

print(f"\n--- Task Runner Finished at {datetime.utcnow().isoformat()} ---")