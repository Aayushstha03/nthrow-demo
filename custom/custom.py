# import argparse
import asyncio
import os
from nthrow.utils import create_db_connection, create_store
from extractor import Extractor


# Set up your DB credentials and table name
table = "nthrows"
creds = {
    "user": os.environ["DB_USER"],
    "password": os.environ["DB_PASSWORD"],
    "database": os.environ["DB"],
    "host": os.environ["DB_HOST"],
    "port": os.environ["DB_PORT"],
}


conn = create_db_connection(**creds)
create_store(conn, table)


async def main():
    # args = parse_args()
    # magnitude = args.magnitude
    extractor = Extractor(conn, table)

    while True:
        extractor.set_list_info("https://f1/")
        # extractor.query_args = {
        #     "magnitude": magnitude,
        # }
        extractor.settings = {
            "remote": {
                "refresh_interval": 2,
                "run_period": "18-2",
                "timezone": "Asia/Kathmandu",
            }
        }
        async with await extractor.create_session() as session:
            extractor.session = session
            result = await extractor.collect_rows(extractor.get_list_row())
            print(result)
        # Sleep for the refresh interval before next run
        interval = extractor.settings["remote"]["refresh_interval"]
        print(f"Sleeping for {interval} minutes before next run...")
        await asyncio.sleep(interval * 60)


# def parse_args():
#     parser = argparse.ArgumentParser(
#         description="Scrape data for given magnitude threshold"
#     )
#     parser.add_argument(
#         "-m",
#         "--magnitude",
#         type=int,
#         help="Minimum magnitude threshold for earthquakes (e.g., 1, 2, 3, etc.)",
#         default=0,
#     )
#     return parser.parse_args()


if __name__ == "__main__":
    asyncio.run(main())
