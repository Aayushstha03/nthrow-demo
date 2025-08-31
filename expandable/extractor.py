import asyncio

from urllib.parse import urljoin
from bs4 import BeautifulSoup

from nthrow.source.utils import canonicalize_url
from nthrow.source import SimpleSource

"""
	You scrape item links from search or list pages
	Now you have to visit those links individually to get their detailed info
	You'll also need to track the links you've already visited
	--
	Use this extractor for such cases
"""


class Extractor(SimpleSource):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def make_url(self, row, _type):
        args = self.prepare_request_args(row, _type)
        page = args["cursor"] or 1
        return f"https://www.brainyquote.com/topics/beauty-quotes_{page}", page

    async def fetch_rows(self, row, _type="to"):
        try:
            url, page = self.make_url(row, _type)
            res = await self.http_get(url)

            if res.status_code == 200:
                rows = []
                content = res.text
                soup = BeautifulSoup(content, "html.parser")
                for el in soup.find(id="quotesList").find_all(class_="b-qt"):
                    rows.append(
                        {
                            "url": canonicalize_url(
                                urljoin("https://www.brainyquote.com/", el.get("href")),
                                parameterlist=("src",),
                            )
                        }
                    )

                rows = self.clamp_rows_length(rows)
                return {
                    # pass partial=True to tell it's a partial row and
                    # needs to be visited to get more details
                    "rows": [
                        self.make_a_row(
                            row["uri"],
                            self.mini_uri(r["url"], parameterlist=("src",)),
                            r,
                            partial=True,
                        )
                        for r in rows
                    ],
                    "state": {"pagination": {_type: page + 1}},
                }
            else:
                self.logger.error(
                    "Non-200 HTTP response: %s : %s" % (res.status_code, url)
                )
                return self.make_error("HTTP", res.status_code, url)
        except Exception as e:
            self.logger.exception(e)
            return self.make_error("Exception", type(e), str(e))

    async def expand_partial_rows(self, rows):
        tasks = []
        for r in rows:
            task = asyncio.create_task(self.http_get(r["data"]["url"]))
            task.row = r
            tasks.append(task)

        res = []
        done, pending = await asyncio.wait(tasks)

        for task in done:
            res.append(task.row)
            if task.exception():
                exc = task.exception()
                self.logger.exception(exc)
                # call merge_error with row and a new error object when
                # expanding a partial rows fails this will track retries
                self.merge_error(
                    task.row,
                    self.make_error(type(exc), str(exc), task.row["data"]["url"]),
                )
                continue
            resp = task.result()
            if resp.status_code == 200:
                content = resp.text
                soup = BeautifulSoup(content, "html.parser")
                text = soup.find(class_="b-qt").get_text()

                # always set partial=False if details for
                # this row have been successfully fetched
                task.row["partial"] = False
                task.row["data"]["text"] = text
            else:
                self.merge_error(
                    task.row,
                    self.make_error("HTTP", resp.status_code, task.row["data"]["url"]),
                )
        return res
