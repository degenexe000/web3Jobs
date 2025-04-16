# ----- process_sentiment.py (Corrected Syntax) -----
import os
import json
from datetime import datetime
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer # Import VADER
import sys
import time

print("--- Starting Sentiment Analysis Script ---")

# --- Database Connection Setup ---
mongo_client = None
db = None
posts_collection = None
analyzer = None
db_connection_ok = False
vader_init_ok = False

try:
    # Try connecting to DB
    print("Reading MONGO_URI from Replit Secrets...")
    mongo_uri = os.environ.get('MONGO_URI')
    if not mongo_uri:
        print(">>> Error: MONGO_URI secret not found or is empty!")
        sys.exit(1)

    print("Connecting to MongoDB Atlas...")
    mongo_client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
    mongo_client.admin.command('ismaster')
    db = mongo_client['web3_data']
    posts_collection = db['social_media_posts']
    print("MongoDB connection successful!")
    db_connection_ok = True

    # Try initializing VADER
    print("\nInitializing VADER Sentiment Analyzer...")
    analyzer = SentimentIntensityAnalyzer()
    print("VADER Analyzer initialized.")
    vader_init_ok = True

    # --- Proceed only if DB and VADER are ready ---
    if db_connection_ok and vader_init_ok:

        # --- Processing Logic ---
        query = {"sentiment": {"$exists": False}}
        process_limit = 50
        print(f"\nQuerying MongoDB for up to {process_limit} documents needing sentiment analysis...")

        documents_to_analyze = list(posts_collection.find(query).limit(process_limit))
        print(f"Found {len(documents_to_analyze)} documents to analyze.")

        if not documents_to_analyze:
            print("No documents found requiring sentiment analysis at this time.")
        else:
            print("Starting sentiment analysis...")
            updated_count = 0
            error_count = 0

            for doc in documents_to_analyze:
                doc_id = doc.get("_id")
                text_to_analyze = doc.get("text", "")

                if not text_to_analyze or not isinstance(text_to_analyze, str) or len(text_to_analyze.strip()) < 5:
                    continue

                try:
                    vs = analyzer.polarity_scores(text_to_analyze)
                    update_result = posts_collection.update_one(
                        {"_id": doc_id},
                        {"$set": {"sentiment": vs, "sentiment_analyzed_at": datetime.utcnow()}}
                    )
                    if update_result.modified_count == 1:
                        updated_count += 1
                    else:
                         print(f"  Warning: Document {doc_id} might not have been updated (modified_count=0).")

                except Exception as analysis_err:
                    print(f"  > Error analyzing/updating document {doc_id}: {analysis_err}")
                    error_count += 1

            # --- Analysis Summary --- (MOVED INSIDE the main try block's successful path)
            print("\n--- Analysis Summary ---")
            print(f"Documents Considered in this run: {len(documents_to_analyze)}")
            print(f"Documents Successfully Updated: {updated_count}")
            print(f"Errors Encountered During Analysis/Update: {error_count}")


# --- Handle Initial Connection/Setup Errors ---
except ConnectionFailure as conn_err:
     print(f">>> MongoDB Atlas Connection Failure during setup: {conn_err}")
     # Client might be None or partially initialized, closing handled in finally
except Exception as setup_err:
    print(f">>> Error during initial setup (DB or VADER): {setup_err}")
    # Ensure cleanup happens in finally

# --- Cleanup ---
finally: # This now correctly follows the outer try/except block
    print("\nClosing MongoDB connection (if active)...")
    if mongo_client:
        try:
            mongo_client.close()
            print("MongoDB connection closed.")
        except Exception as close_err:
            print(f">>> Error closing MongoDB connection: {close_err}")
    else:
         print("No MongoDB connection was active to close.")

    print("\n--- Sentiment Analysis Script Finished ---")