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
    extractor = Extractor(conn, table)
    extractor.set_list_info("https://www.scrapethissite.com/pages/forms/")
    extractor.query_args = {
        "teamname": "boston",
    }
    extractor.settings = {
        "remote": {
            "refresh_interval": 15,
            "run_period": "18-2",
            "timezone": "Asia/Kathmandu",
        }
    }
    async with await extractor.create_session() as session:
        extractor.session = session
        result = await extractor.collect_rows(extractor.get_list_row())
        print(result)

        while (
            extractor.get_list_row()["state"]["pagination"].get("to") <= 5
            and extractor.should_run_again() is True
        ):
            result = await extractor.collect_rows(extractor.get_list_row())
            print(result)


if __name__ == "__main__":
    asyncio.run(main())
