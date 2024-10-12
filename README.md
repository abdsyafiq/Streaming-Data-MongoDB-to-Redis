# Streaming Data MongoDB to Redis

## Purpose:
   This script monitors MongoDB collection(s) and sends data to Redis. The following steps outline its functionality:

   1. Set Parameters:
      - Define MongoDB connection parameters, Redis connection parameters, log file path, and Telegram notification settings.

   2. Define Helper Functions:
      - logger: Logs messages with a timestamp to a specified log file.
      - send_telegram_notification: Sends a notification to a specified Telegram chat with the provided message.
      - mongodb_connection: Attempts to connect to the MongoDB database with retry logic. If the connection fails, it waits 5 seconds before retrying.
      - redis_connection: Attempts to connect to the Redis server with retry logic. Similar to the MongoDB connection function.

   3. Monitor MongoDB Collection:
      - watch_single_collection_and_send_to_redis: 
          - Monitors a specified MongoDB collection for insert operations using a Change Stream.
          - Collects the captured changes into a local dictionary called local_latest_data.
          - Defines a nested function send_data_to_redis to send the collected data to Redis. It implements retry logic for failed attempts.
          - Defines another nested function change_stream_listener to listen for changes in the collection.
          - Utilizes asyncio.gather to run both the change stream listener and the data sending function concurrently.

   4. Periodic Data Sending:
      - send_data_to_redis_loop: This function runs the send_data_to_redis function every 10 minutes (600 seconds) to send any collected data to Redis.

   5. Make Documents Serializable:
      - make_serializable: Converts BSON types from MongoDB documents to JSON serializable types (e.g., converts ObjectId to string, datetime to ISO format).

   6. Execution:
      - The main function initializes a connection to MongoDB, retrieves the names of all collections, and starts monitoring each collection by creating asynchronous tasks for each collection's watcher.
      - Uses asyncio.run to execute the main function in an asynchronous event loop.
