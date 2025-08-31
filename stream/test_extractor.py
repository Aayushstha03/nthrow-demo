import os
import time
import asyncio

from datetime import datetime
from nthrow.utils import create_db_connection, create_store, utcnow
from nthrow.utils import uri_clean, uri_row_count

from tests.extractors.stream.extractor import Extractor

# from nthrow.source.StorageHelper import Storage

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


def test_stream_extractor():
    extractor = Extractor(conn, table)
    extractor.set_list_info("http://reddit.com/")
    # extractor.storage = Storage(conn, table)
    uri_clean(extractor.uri, conn, table)

    async def call():
        async with await extractor.create_session() as session:
            extractor.session = session
            _ = await extractor.collect_rows(extractor.get_list_row())
            row = extractor.get_list_row()

            assert type(row["next_update_at"]) == datetime
            assert row["next_update_at"] <= utcnow()
            to = row["state"]["pagination"]["to"]
            assert row["state"]["pagination"]["to"]
            assert not row["state"]["pagination"]["from"]
            assert uri_row_count(extractor.uri, conn, table, partial=False) == 25
            assert extractor.should_run_again() is True

            extractor.session = None  # simulate error

            _ = await extractor.collect_rows(row)
            row = extractor.get_list_row()

            assert type(row["next_update_at"]) == datetime
            assert row["next_update_at"] <= utcnow()
            assert row["state"]["pagination"]["to"] == to
            assert "error" in row["state"]
            assert row["state"]["error"]["primary"]["times"] == 1
            assert uri_row_count(extractor.uri, conn, table, partial=False) == 25

            await asyncio.sleep(3)
            extractor.session = session

            time.sleep(1)
            extractor.settings = {
                "remote": {"refresh_interval": 0.01}
            }  # so it doesn't skip collect_new_rows

            _ = await extractor.collect_new_rows(row)
            row = extractor.get_list_row()

            assert type(row["next_update_at"]) == datetime
            assert row["next_update_at"] <= utcnow()
            assert row["state"]["pagination"]["to"]
            assert not row["state"]["pagination"]["from"]
            assert set(row["state"].keys()) == {"error", "pagination", "last_run"}
            assert uri_row_count(extractor.uri, conn, table, partial=False) > 25

            extractor._reset_run_times()
            _ = await extractor.collect_rows(row)
            row = extractor.get_list_row()

            assert type(row["next_update_at"]) == datetime
            assert row["next_update_at"] <= utcnow()
            assert row["state"]["pagination"]["to"]
            assert not row["state"]["pagination"]["from"]
            assert set(row["state"].keys()) == {"pagination", "last_run"}
            assert extractor.should_run_again() is True

    asyncio.run(call())


if __name__ == "__main__":
    pass
    test_stream_extractor()
