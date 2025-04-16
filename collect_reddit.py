# ----- collect_reddit.py (Updated - With MongoDB Insertion) -----
import praw          # Reddit API wrapper
import os            # Access environment variables (Replit Secrets)
import json          # To print output nicely
import time          # For delays
from datetime import datetime # Timestamps
from pymongo import MongoClient # MongoDB Driver
from pymongo.errors import ConnectionFailure, DuplicateKeyError # Error types
import sys

print("--- Starting Reddit Collection Script ---")

# --- Database Connection Setup ---
mongo_client = None
db = None
posts_collection = None
try:
    print("Reading MONGO_URI from Replit Secrets...")
    mongo_uri = os.environ.get('MONGO_URI')
    if not mongo_uri:
        print(">>> Error: MONGO_URI secret not found!")
        sys.exit(1)

    print("Connecting to MongoDB Atlas...")
    mongo_client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
    mongo_client.admin.command('ismaster') # Force connection check
    db = mongo_client['web3_data'] # Use same DB name as Twitter script
    posts_collection = db['social_media_posts'] # Use same collection
    print("MongoDB connection successful!")
    # Ensure index exists for duplicate checking
    # Using a compound index on source and source_specific_id for uniqueness across platforms
    posts_collection.create_index([("source", 1), ("source_specific_id", 1)], unique=True)
    print("Compound unique index on ('source', 'source_specific_id') ensured.")

except ConnectionFailure as conn_err:
     print(f">>> MongoDB Atlas Connection Failure: {conn_err}")
     if mongo_client: mongo_client.close()
     sys.exit(1)
except Exception as db_err:
    print(f">>> MongoDB connection/setup error: {db_err}")
    if mongo_client: mongo_client.close()
    sys.exit(1)

# --- Reddit API Setup ---
reddit = None
try:
    print("\nReading Reddit credentials from Replit Secrets...")
    client_id = os.environ.get('REDDIT_CLIENT_ID')
    client_secret = os.environ.get('REDDIT_CLIENT_SECRET')
    user_agent = os.environ.get('REDDIT_USER_AGENT')
    if not all([client_id, client_secret, user_agent]):
        print(">>> Error: Missing Reddit credentials in Replit Secrets.")
        sys.exit(1)
    print("Reddit credentials loaded.")
    print(f"User Agent: {user_agent}")

    print("\nAttempting to authenticate with Reddit (read-only)...")
    reddit = praw.Reddit(
        client_id=client_id,
        client_secret=client_secret,
        user_agent=user_agent,
        read_only=True
    )
    print(f"Authenticated successfully via PRAW. Read Only Mode: {reddit.read_only}")
except Exception as praw_err:
    print(f">>> Error initializing PRAW or authenticating: {praw_err}")
    if mongo_client: mongo_client.close() # Close DB if PRAW fails
    sys.exit(1)

# --- Collection Configuration ---
target_subreddits = ['ethereum', 'CryptoCurrency', 'web3'] # Removed non-existent ones
search_keywords = ['web3 developer salary', 'Coinbase hiring', 'blockchain skill demand', 'remote web3 role']
collection_limit_per_source = 15 # Increase slightly if desired

# --- Collect and Insert ---
inserted_count = 0
skipped_count = 0
total_processed = 0

# Function to create document structure consistently
def create_reddit_doc(submission, source_method, source_query):
    return {
        'source': 'reddit',
        'source_method': source_method,
        'source_query': source_query,
        'source_specific_id': submission.id, # Use Reddit's submission ID
        'title': submission.title,
        'text': submission.selftext,
        'author': submission.author.name if submission.author else '[deleted]',
        'subreddit': submission.subreddit.display_name,
        'url': f"https://www.reddit.com{submission.permalink}",
        'score': submission.score,
        'upvote_ratio': submission.upvote_ratio,
        'num_comments': submission.num_comments,
        'created_utc': datetime.utcfromtimestamp(submission.created_utc), # Store as datetime
        'collected_at': datetime.utcnow(), # Store as datetime
        # Optional: Store original PRAW object data if needed later
        # 'raw_data': vars(submission) # Be careful, can be large/complex
    }

# --- Collect from Subreddits (New Posts) ---
print(f"\nFetching {collection_limit_per_source} new posts from subreddits: {target_subreddits}...")
try:
    for sub_name in target_subreddits:
        print(f" Accessing r/{sub_name}...")
        subreddit = reddit.subreddit(sub_name)
        posts_to_insert = []
        processed_in_batch = 0
        try:
            for submission in subreddit.new(limit=collection_limit_per_source):
                processed_in_batch += 1
                reddit_doc = create_reddit_doc(submission, 'subreddit_new', sub_name)
                posts_to_insert.append(reddit_doc)
            total_processed += processed_in_batch

            # Attempt to insert batch, ignoring duplicates
            if posts_to_insert:
                 try:
                      # Use insert_many with ordered=False to continue on duplicate errors
                      insert_result = posts_collection.insert_many(posts_to_insert, ordered=False)
                      inserted_count += len(insert_result.inserted_ids)
                      skipped_in_batch = processed_in_batch - len(insert_result.inserted_ids)
                      skipped_count += skipped_in_batch
                      print(f"  Processed: {processed_in_batch}, Inserted: {len(insert_result.inserted_ids)}, Skipped (duplicates): {skipped_in_batch}")
                 except DuplicateKeyError:
                     # This might still catch if the *whole batch* only contains duplicates (less likely)
                     skipped_count += len(posts_to_insert)
                     print(f"  Processed: {processed_in_batch}, Inserted: 0, Skipped (all duplicates).")
                 except Exception as batch_err:
                      print(f"  > Error during bulk insert for r/{sub_name}: {batch_err}")
                      # Consider incrementing skipped_count for all attempted in failed batch
                      skipped_count += len(posts_to_insert)

        except Exception as sub_err:
            print(f"  > Error processing subreddit r/{sub_name}: {sub_err}")
        time.sleep(1)

except Exception as e:
    print(f">>> Error during subreddit collection phase: {e}")

# --- Collect using Search Keywords ---
print(f"\nSearching top {collection_limit_per_source} posts (sorted by 'new') using keywords...")
search_scope = '+'.join(target_subreddits)
print(f"Search Scope: r/{search_scope}")
try:
    for keyword in search_keywords:
        print(f" Searching for '{keyword}'...")
        posts_to_insert = []
        processed_in_batch = 0
        try:
            search_results = reddit.subreddit(search_scope).search(
                keyword, limit=collection_limit_per_source, sort='new'
            )
            unique_ids_in_batch = set() # Track IDs within this search batch

            for submission in search_results:
                processed_in_batch += 1
                # Basic check within batch - full check happens on insert
                if submission.id not in unique_ids_in_batch:
                    reddit_doc = create_reddit_doc(submission, 'search', keyword)
                    posts_to_insert.append(reddit_doc)
                    unique_ids_in_batch.add(submission.id)
                # Else: likely duplicate within search results, don't even add to batch
            total_processed += processed_in_batch

            if posts_to_insert:
                try:
                     insert_result = posts_collection.insert_many(posts_to_insert, ordered=False)
                     inserted_count += len(insert_result.inserted_ids)
                     skipped_in_batch = len(posts_to_insert) - len(insert_result.inserted_ids)
                     skipped_count += skipped_in_batch
                     print(f"  Processed: {processed_in_batch}, Inserted: {len(insert_result.inserted_ids)}, Skipped (duplicates): {skipped_in_batch}")
                except DuplicateKeyError:
                    skipped_count += len(posts_to_insert)
                    print(f"  Processed: {processed_in_batch}, Inserted: 0, Skipped (all duplicates).")
                except Exception as batch_err:
                     print(f"  > Error during bulk insert for keyword '{keyword}': {batch_err}")
                     skipped_count += len(posts_to_insert)
            else:
                print(f"  Processed: {processed_in_batch}, No unique items found to insert.")

        except Exception as search_err:
            print(f"  > Error processing search for '{keyword}': {search_err}")
        time.sleep(2)

except Exception as e:
    print(f">>> Error during search collection phase: {e}")


# --- Cleanup ---
finally:
    print("\n--- Final Summary ---")
    print(f"Total Reddit Items Processed (approx): {total_processed}")
    print(f"New Items Inserted: {inserted_count}")
    print(f"Items Skipped (Duplicate/Error): {skipped_count}")

    print("Closing MongoDB connection...")
    if mongo_client:
        mongo_client.close()
        print("MongoDB connection closed.")
    else:
        print("No MongoDB connection was active.")
    print("\n--- Reddit Collection Script Finished ---")