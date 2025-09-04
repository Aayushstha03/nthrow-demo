from bs4 import BeautifulSoup
from nthrow.utils import sha1
from nthrow.source import SimpleSource

"""
extractor.make_a_row method
	make_a_row takes 3 parameters here
	1.) url of the dataset that you put in extractor.set_list_info
	2.) url of a row,
			always pass it through self.mini_uri method,
			this replaces https with http and
			removes www. from urls to reduce duplicate rows
	- hash of urls from 1 & 2 becomes id of the row
	3.) the row data, it's stored in a JSONB column

extractor.make_error method
	make_error takes 3 parameters
	1.) _type = HTTP, Exception etc.
	2.) code = 404, 403 etc.
	3.) message = None (Any text message)
"""


class Extractor(SimpleSource):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def make_url(self, row, _type):
        # args is dict that contains current page cursor, limit and
        # other variables from extractor.query_args, extractor.settings
        args = self.prepare_request_args(row, _type)
        page = args["cursor"] or 1
        magnitude = self.query_args.get("magnitude")
        return (
            f"https://earthquake.usgs.gov/earthquakes/map/?magnitude={magnitude}",
            page,
        )

    async def fetch_rows(self, row, _type="to"):
        # row is info about this dataset
        # it is what was returned with extractor.get_list_row method
        # it holds pagination, errors, retry count, next update time etc.
        try:
            url, page = self.make_url(row, _type)
            res = await self.http_get(url)  # wrapper around aiohttp session's get

            if res.status_code == 200:
                rows = []
                content = res.text
                soup = BeautifulSoup(content, "html.parser")
                for i, e in enumerate(soup.find_all("usgs-event-item")):
                    magnitude = e.find("span", class_="ng-star-inserted").get_text(
                        strip=True
                    )
                    location = e.find("h6", class_="header").get_text(strip=True)
                    time = e.find("span", class_="time").get_text(strip=True)
                    depth = e.find("aside", class_="aside").get_text(strip=True)
                    rows.append(
                        {
                            "uri": f"https://earthquake.usgs.gov/earthquakes/map/#{sha1(magnitude + time + depth)}",
                            "magnitude": magnitude,
                            "location": location,
                            "time": time,
                            "depth": depth,
                        }
                    )
                    # print(rows)

                # Removing duplicate rows by 'uri' because some entries are duplicated on the website?
                print(f"Original rows count: {len(rows)}")
                unique_rows = {row["uri"]: row for row in rows}.values()
                print(f"Deduped rows count: {len(unique_rows)}")
                rows = list(unique_rows)

                # slice rows length to limit from extractor.query_args or
                # extractor.settings[remote]
                rows = self.clamp_rows_length(rows)
                return {
                    "rows": [
                        self.make_a_row(
                            row["uri"], self.mini_uri(r["uri"], keep_fragments=True), r
                        )
                        for r in rows
                    ],
                    "state": {
                        "pagination": {
                            # value for next page, return None when pagination ends
                            _type: page + 1
                        }
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
