import os
import time
import asyncio

from datetime import datetime
from nthrow.utils import create_db_connection, create_store, utcnow
from nthrow.utils import uri_clean, uri_row_count

from tests.extractors.daterange.extractor import Extractor

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


def test_daterange_scraper():
    extractor = Extractor(conn, table)

    extractor.query_args.update({"q": "honda", "limit": 10})

    extractor.set_list_info("http://community.cartalk.com/")
    uri_clean(extractor.uri, conn, table)

    async def call():
        async with await extractor.create_session() as session:
            extractor.session = session
            assert await extractor.collect_rows(extractor.get_list_row()) == 0

            row = extractor.get_list_row()
            assert uri_row_count(extractor.uri, conn, table, partial=False) == 10
            row["state"]["pagination"]["to"]["cursor"] == 2
            row["state"]["pagination"]["from"]["date"] is not None

            time.sleep(1)
            extractor.settings = {
                "remote": {"refresh_interval": 0.01}
            }  # so it doesn't skip collect_new_rows

            assert await extractor.collect_new_rows(row) == 0
            assert uri_row_count(extractor.uri, conn, table, partial=False) == 10

            r = extractor.get_list_row()
            assert r["updated_at"] > row["updated_at"]
            r["state"]["pagination"]["to"]["cursor"] == 2
            r["state"]["pagination"]["from"]["date"] is not None

            extractor.session = None  # simulate error

            assert await extractor.collect_rows(extractor.get_list_row()) == 1
            row = extractor.get_list_row()

            assert row["state"]["error"]["primary"]["times"] == 1
            assert type(row["next_update_at"]) is datetime
            assert row["next_update_at"] <= utcnow()

            extractor.session = session
            assert await extractor.collect_rows(row) == 0
            assert uri_row_count(extractor.uri, conn, table, partial=False) == 20

            row = extractor.get_list_row()
            row["state"]["pagination"]["to"]["cursor"] == 3
            row["state"]["pagination"]["from"]["date"] is not None
            assert set(row["state"].keys()) == {"pagination", "last_run"}

    asyncio.run(call())


if __name__ == "__main__":
    pass
    test_daterange_scraper()
