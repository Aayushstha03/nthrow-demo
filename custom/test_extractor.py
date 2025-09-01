import os
import asyncio

from datetime import datetime
from nthrow.utils import create_db_connection, create_store, utcnow
from nthrow.utils import uri_clean, uri_row_count

from tests.extractors.simple.extractor import Extractor

# from nthrow.source.StorageHelper import Storage

# table name and postgres credentials
table = "nthrows"
creds = {
    "user": os.environ["DB_USER"],
    "password": os.environ["DB_PASSWORD"],
    "database": os.environ["DB"],
    "host": os.environ["DB_HOST"],
    "port": os.environ["DB_PORT"],
}

conn = create_db_connection(**creds)
create_store(conn, table)  # creates table


def test_simple_extractor():
    extractor = Extractor(conn, table)

    # url of your dataset, this effectively becomes id of this dataset
    # use l.get_list_row() to return this record from database later
    extractor.set_list_info("https://www.scrapethissite.com/pages/forms/")

    # sets the CustomStorage if redis can't be found
    # extractor.storage = Storage(conn, table)

    # truncate table to remove previous run's rows
    uri_clean(extractor.uri, conn, table)
    extractor.settings = {
        "remote": { 
            "refresh_interval": 15,
            "run_period": "18-2",  # 24 hrs format
            "timezone": "Asis/Kathmandu",
        }
    }

    """
        extractor.settings = {
            'remote': {
                # how often to refresh this dataset (in miutes)
                # you can leave it None but this extractor will only run once
                # (will still run until pagination ends)
                'refresh_interval': None,

                # number if remote url accepts a limit parameter
                'limit': None
            }
        }
    """

    async def call():
        async with await extractor.create_session() as session:
            extractor.session = session

            # collect_rows calls fetch_rows on your extractor class and
            # puts the returned rows in postgres table
            _ = await extractor.collect_rows(extractor.get_list_row())
            row = extractor.get_list_row()

            assert type(row["next_update_at"]) is datetime
            assert row["next_update_at"] <= utcnow()
            to = row["state"]["pagination"]["to"]
            assert row["state"]["pagination"]["to"]
            assert not row["state"]["pagination"]["from"]

            row_count = uri_row_count(extractor.uri, conn, table, partial=False)
            assert row_count >= 10

            # if the pagination has next page info, should_run_again() will
            # return true so you can run the extractor again
            assert extractor.should_run_again() is True

            extractor.session = None  # simulate error

            _ = await extractor.collect_rows(row)
            row = extractor.get_list_row()

            assert type(row["next_update_at"]) is datetime
            assert row["next_update_at"] <= utcnow()
            assert row["state"]["pagination"]["to"] == to
            assert "error" in row["state"]
            assert row["state"]["error"]["primary"]["times"] == 1
            assert uri_row_count(extractor.uri, conn, table, partial=False) == row_count

            extractor.session = session
            # this method resets class property used by should_run_again()
            extractor._reset_run_times()

            _ = await extractor.collect_rows(row)
            row = extractor.get_list_row()

            assert type(row["next_update_at"]) is datetime
            assert row["next_update_at"] <= utcnow()
            assert row["state"]["pagination"]["to"]
            assert row["state"]["pagination"]["to"] != to
            assert not row["state"]["pagination"]["from"]
            assert set(row["state"].keys()) == {"pagination", "last_run"}
            assert uri_row_count(extractor.uri, conn, table, partial=False) > row_count
            assert extractor.should_run_again() is True
            # l.del_garbage()

    asyncio.run(call())


if __name__ == "__main__":
    pass
    test_simple_extractor()
