from urllib.parse import urlencode, quote_plus
from nthrow.source import DateRangeSource
from nthrow.source.http import create_session


class Extractor(DateRangeSource):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    async def create_session(self, session=None):
        headers = {
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:98.0) Gecko/20100101 Firefox/98.0",  # noqa:E501
        }
        return await create_session(headers=headers)

    def make_url(self, row, _type):
        args = self.prepare_request_args(row, _type)

        q = args["q"]["q"]
        page = args["cursor"] or 1
        limit = args["limit"]  # noqa:F841
        after_s = args["after"].strftime("%Y-%m-%d")
        before_s = args["before"].strftime("%Y-%m-%d")

        params = {"q": f"{q} after:{after_s} before:{before_s}"}
        params = urlencode(params, quote_via=quote_plus)
        url = f"https://community.cartalk.com/search?q={params}&page={page}"  # noqa:E501
        return url, args, page

    async def fetch_rows(self, row, _type="to"):
        try:
            pagi = row["state"]["pagination"]  # noqa: F841
            url, args, cursor = self.make_url(row, _type)
            res = await self.http_get(url)

            if res.status_code == 200:
                rows = []
                jsn = res.json()
                has_more = jsn["grouped_search_result"]["more_full_page_results"]

                for r in self.clamp_rows_length(jsn.get("topics", [])):
                    rows.append(
                        self.make_a_row(
                            row["uri"],
                            self.mini_uri(f"http://community.cartalk.com/{r['id']}"),
                            r,
                        )
                    )

                return {
                    "rows": rows,
                    "state": {
                        "pagination": self.construct_pagination(
                            row, _type, cursor + 1 if has_more else None, args
                        )
                    },
                }
            else:
                self.logger.error(
                    "Non-200 HTTP response: %s : %s" % (res.status_code, url)
                )
                return self.make_error("HTTP", res.status_code, url)
        except Exception as e:
            self.logger.exception(e)
            return self.make_error("Exception", type(e), str(e))
