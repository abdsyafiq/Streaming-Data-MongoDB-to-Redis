
import redis.asyncio as aioredis
import motor.motor_asyncio
import aiofiles
import aiohttp
import asyncio

import datetime
import bson
import json

import warnings
warnings.filterwarnings("ignore")

##########################
####### PARAMETERS #######
##########################
MONGO_HOST = "server_1:port_1,server_2:port_2,server_3:port_3"
MONGO_USER = "user"
MONGO_PASS = "password"
MONGO_REPL = "replica_name"
MONGO_AUTH = "db_authentication"
MONGO_DB = "database"
MONGO_URI = (
    f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}/"
    f"?replicaSet={MONGO_REPL}&authSource={MONGO_AUTH}&"
    "authMechanism=SCRAM-SHA-256"
)

REDIS_HOST = "server"
REDIS_PORT = 0000
REDIS_DB = 0

LOG_PATH = "/path/to/streamsMongoRedis.log"
TELE_TOKEN = "token_telegram"
TELE_CHAT_ID = "chat_id_telegram"

#########################
####### FUNCTIONS #######
#########################

async def logger(mssg: str) -> None:
    now = datetime.datetime.now().strftime("%d-%h-%Y %H:%M:%S")
    async with aiofiles.open(LOG_PATH, "a") as f:
        await f.write(f"[{now}] {mssg}\n")

async def send_telegram_notification(mssg: str) -> None:
    message = f"[Streaming Data MongoDB to Redis]\n\n{mssg}"
    url = f"https://api.telegram.org/bot{TELE_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELE_CHAT_ID,
        "text": message,
    }
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(url, json=payload) as response:
                response_data = await response.json()
                if response.status != 200:
                    await logger(f"Telegram notification failed: {response_data}")
        except Exception as e:
            await logger(f"Failed to send Telegram notification: {e}")

async def mongodb_connection(uri: str = MONGO_URI, db: str = MONGO_DB):
    while True:
        try:
            client = motor.motor_asyncio.AsyncIOMotorClient(uri, readPreference="nearest")
            return client[db]
        except Exception as e:
            mssg = f"Failed to connect to MongoDB: {e}. Retrying in 5 seconds..."
            await send_telegram_notification(mssg)
            await logger(mssg)
            await asyncio.sleep(5)

async def redis_connection(host: str = REDIS_HOST, port: int = REDIS_PORT, db: int = REDIS_DB):
    while True:
        try:
            return await aioredis.from_url(f"redis://{host}:{port}/{db}", decode_responses=True)
        except Exception as e:
            mssg = f"Failed to connect to Redis: {e}. Retrying in 5 seconds..."
            await send_telegram_notification(mssg)
            await logger(mssg)
            await asyncio.sleep(5)

async def watch_single_collection_and_send_to_redis(mongodb_conn, collection_name: str) -> None:
    mongodb_collection = mongodb_conn[collection_name]
    local_latest_data = {}

    pipeline = [
        {"$match": {"operationType": "insert"}}
    ]

    await logger(f"Monitoring the collection: {collection_name}.")

    async def send_data_to_redis():
        if not local_latest_data:
            await logger(f"No data to send to Redis for collection: {collection_name}.")
            return

        for attempt in range(5):
            try:
                redis_conn = await redis_connection(REDIS_HOST, REDIS_PORT, REDIS_DB)
                ttl_in_seconds = 3 * 24 * 60 * 60  # 3 days in seconds

                pipeline = redis_conn.pipeline()
                for key, value in local_latest_data.items():
                    pipeline.execute_command("JSON.MSET", key, "$", json.dumps(value))
                    pipeline.expire(key, ttl_in_seconds)

                await pipeline.execute()
                await logger(f"Sent {len(local_latest_data)} documents to Redis for collection '{collection_name}'.")
                local_latest_data.clear()
                await redis_conn.close()
                return

            except Exception as e:
                mssg = f"Error sending data to Redis for collection {collection_name}: {e}. Retrying in {5 ** (attempt + 1)} seconds..."
                await send_telegram_notification(mssg)
                await logger(mssg)
                await asyncio.sleep(5 ** (attempt + 1))

        await logger(f"All retries failed for sending data to Redis for collection {collection_name}. Aborting.")

    async def change_stream_listener():
        while True:
            try:
                async with mongodb_collection.watch(pipeline) as stream:
                    async for change in stream:
                        doc = make_serializable(change["fullDocument"])
                        key = f"{collection_name}:{doc.get('ID_1', doc.get('ID_2', doc.get('ID_3')))}"
                        local_latest_data[key] = doc
            except Exception as e:
                mssg = f"Error watching collection {collection_name}: {e}. Reconnecting in 5 seconds..."
                await send_telegram_notification(mssg)
                await logger(mssg)
                await asyncio.sleep(5)

    await asyncio.gather(
        change_stream_listener(),
        send_data_to_redis_loop(send_data_to_redis)
    )

async def send_data_to_redis_loop(send_data_to_redis):
    while True:
        await send_data_to_redis()
        await asyncio.sleep(600)

def make_serializable(doc: dict) -> dict:
    for key, value in doc.items():
        if isinstance(value, bson.ObjectId):
            doc[key] = str(value)
        elif isinstance(value, datetime.datetime):
            doc[key] = value.isoformat()
        elif isinstance(value, bson.Decimal128):
            doc[key] = float(value.to_decimal())
        elif isinstance(value, list):
            doc[key] = [make_serializable(item) if isinstance(item, dict) else item for item in value]
        elif isinstance(value, dict):
            doc[key] = make_serializable(value)
    return doc

########################
###### EXECUTIONS ######
########################
async def main():
    mongodb_conn = await mongodb_connection(MONGO_URI, MONGO_DB)
    collections = await mongodb_conn.list_collection_names()

    tasks = []
    for collection_name in collections:
        task = watch_single_collection_and_send_to_redis(mongodb_conn, collection_name)
        tasks.append(asyncio.create_task(task))

    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
