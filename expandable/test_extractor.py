import os
import asyncio

from nthrow.utils import create_db_connection, create_store
from nthrow.utils import uri_clean, uri_row_count

from tests.extractors.expandable.extractor import Extractor

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


def test_expandable_scraper():
    extractor = Extractor(conn, table)
    extractor.set_list_info("https://www.brainyquote.com/topics/beauty-quotes/")
    # extractor.storage = Storage()
    uri_clean(extractor.uri, conn, table)

    async def call():
        async with await extractor.create_session() as session:
            extractor.session = session
            extractor.query_args["limit"] = 3

            _ = await extractor.collect_rows(extractor.get_list_row())
            assert uri_row_count(extractor.uri, conn, table, partial=False) == 0
            assert uri_row_count(extractor.uri, conn, table, partial=True) == 3
            assert extractor.should_run_again() is True

            extractor.session = None  # simulate error

            extractor._reset_run_times()

            # expand_rows will call expand_partial_rows in your extractor class with
            # a list of partial rows as argument and
            # store returned results in postgres table
            _ = await extractor.expand_rows()
            assert uri_row_count(extractor.uri, conn, table, partial=False) == 0
            assert uri_row_count(extractor.uri, conn, table, partial=True) == 3
            assert extractor.should_run_again() is True

            extractor.session = session
            extractor._reset_run_times()
            _ = await extractor.expand_rows()
            assert uri_row_count(extractor.uri, conn, table, partial=False) == 3
            assert uri_row_count(extractor.uri, conn, table, partial=True) == 0
            assert extractor.should_run_again() is False
            # extractor.del_garbage()

    asyncio.run(call())


if __name__ == "__main__":
    pass
    test_expandable_scraper()
