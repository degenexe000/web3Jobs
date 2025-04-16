import tweepy
import os
import json
import time
from datetime import datetime
from pymongo import MongoClient # Import MongoDB Driver
from pymongo.errors import ConnectionFailure # Import specific error type
import sys

print("--- Starting Twitter Collection Script ---")

# --- Database Connection Setup ---
mongo_client = None
db = None
posts_collection = None
try:
    print("Reading MONGO_URI from Replit Secrets...")
    mongo_uri = os.environ.get('MONGO_URI')
    if not mongo_uri:
        print(">>> Error: MONGO_URI secret not found or is empty!")
        print(">>> Please add the connection string from MongoDB Atlas to Replit Secrets.")
        sys.exit(1)

    print("Connecting to MongoDB Atlas...")
    # Set serverSelectionTimeoutMS to handle connection issues better
    mongo_client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
    # The ismaster command is cheap and does not require auth.
    mongo_client.admin.command('ismaster') # Force connection check
    # Choose/create your database (e.g., 'web3_data')
    db = mongo_client['web3_data']
    # Choose/create your collection (e.g., 'social_media_posts')
    posts_collection = db['social_media_posts']
    print("MongoDB connection successful!")
    # Optional: Create an index on tweet_id for faster duplicate checks if needed
    # posts_collection.create_index("source_specific_id", unique=True) # If making ID unique
    posts_collection.create_index("source_specific_id") # Index for faster searching
    posts_collection.create_index("source")
    print("Index on 'source_specific_id' ensured.")

except ConnectionFailure as conn_err:
     print(f">>> MongoDB Atlas Connection Failure: {conn_err}")
     print(">>> Check your MONGO_URI string, network access rules in Atlas, and if the cluster is active.")
     if mongo_client: mongo_client.close()
     sys.exit(1)
except Exception as db_err:
    print(f">>> MongoDB connection/setup error: {db_err}")
    if mongo_client: mongo_client.close()
    sys.exit(1)


# --- Twitter API Setup ---
bearer_token = None
client = None
try:
    print("\nReading Twitter credentials (Bearer Token) from Replit Secrets...")
    bearer_token = os.environ.get('TWITTER_BEARER_TOKEN')
    if not bearer_token:
        print(">>> Error: TWITTER_BEARER_TOKEN secret not found.")
        sys.exit(1)
    print("Bearer Token loaded successfully.")

    print("\nInitializing Tweepy v2 Client...")
    client = tweepy.Client(bearer_token=bearer_token, wait_on_rate_limit=True)
    print("Tweepy v2 Client initialized successfully.")
except Exception as api_err:
    print(f">>> Twitter API setup error: {api_err}")
    if mongo_client: mongo_client.close() # Close DB connection on early exit
    sys.exit(1)


# --- Search Configuration ---
search_queries = [
    '(#Web3Jobs OR #CryptoHiring OR #BlockchainCareers) -is:retweet lang:en',
    '("web3 developer salary" OR "blockchain developer pay") -is:retweet lang:en',
    '(from:Coinbase OR from:binance OR from:ethereum) (hiring OR jobs OR career)',
    '#DeFiJobs -is:retweet lang:en'
]
collection_limit_per_query = 10


# --- Execute Searches and Insert into DB ---
print("\nExecuting search queries for recent tweets (last 7 days)...")
inserted_count = 0
skipped_count = 0
total_processed = 0

try:
    for query in search_queries:
        print(f" Searching for: {query}")
        try:
            response = client.search_recent_tweets(
                query,
                max_results=collection_limit_per_query,
                tweet_fields=["created_at", "public_metrics", "author_id", "lang", "geo"]
            )

            if response.data:
                print(f"  > Received {len(response.data)} tweets.")
                documents_to_insert = [] # Batch insert for efficiency
                for tweet in response.data:
                    total_processed += 1
                    # Create document structure for MongoDB
                    tweet_doc = {
                        'source': 'twitter',
                        'source_query': query,
                        'source_method': 'search_recent',
                        'source_specific_id': str(tweet.id), # Use a consistent ID field name
                        'text': tweet.text,
                        'author_id': str(tweet.author_id) if tweet.author_id else None,
                        'language': tweet.lang,
                        'created_at': tweet.created_at, # Store as ISODate
                        'public_metrics': tweet.public_metrics,
                        'geo': tweet.geo,
                        'collected_at': datetime.utcnow() # Store as ISODate
                        # Consider adding original full JSON object if needed: 'raw_response': tweet.data
                    }
                    # Basic check for duplicates before adding to batch
                    # Only add if no doc with this source_specific_id and source='twitter' exists
                    # More efficient might be insert_many with ordered=False or using ON CONFLICT later
                    existing_doc = posts_collection.find_one({
                         "source": "twitter",
                         "source_specific_id": tweet_doc['source_specific_id']
                    })
                    if not existing_doc:
                        documents_to_insert.append(tweet_doc)
                    else:
                         skipped_count += 1

                # Insert the batch of new documents
                if documents_to_insert:
                    try:
                         insert_result = posts_collection.insert_many(documents_to_insert, ordered=False) # ordered=False continues on error
                         inserted_count += len(insert_result.inserted_ids)
                         print(f"  Inserted {len(insert_result.inserted_ids)} new tweets into MongoDB.")
                    except Exception as bulk_err:
                         print(f"  > Error during bulk insert: {bulk_err}")
                         # Handle potential individual errors if needed, though ordered=False helps
                else:
                    if len(response.data) > skipped_count: # Check if we skipped docs or simply had none to insert
                        print(f"  No new unique tweets to insert from this batch (Skipped {skipped_count} duplicates).")


            elif response.errors:
                 print(f"  > API returned errors for this query: {response.errors}")
            else:
                print("  No tweets found matching this query in the recent period.")

        except tweepy.errors.TweepyException as e:
            print(f"  > Tweepy Error processing query '{query}': {e}")
            if isinstance(e, tweepy.errors.TooManyRequests):
                 print("  >> Rate limit hit, Tweepy is pausing automatically...")
            # Other Tweepy error handling here if needed
        except Exception as e_inner:
            print(f"  > Unexpected error during query '{query}': {e_inner}")

        time.sleep(1) # Small polite pause between distinct queries

except Exception as e_outer:
    print(f"\n>>> Major error occurred during Twitter search loop: {e_outer}")
    import traceback
    traceback.print_exc()

# --- Cleanup ---
finally:
    print("\n--- Final Summary ---")
    print(f"Total Tweets Processed: {total_processed}")
    print(f"New Tweets Inserted: {inserted_count}")
    print(f"Tweets Skipped (Duplicate/Error): {skipped_count}")

    print("Closing MongoDB connection...")
    if mongo_client:
        mongo_client.close()
        print("MongoDB connection closed.")
    else:
         print("No MongoDB connection was active.")

    print("\n--- Twitter Collection Script Finished ---")