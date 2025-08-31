from nthrow.source import SimpleSource


class Extractor(SimpleSource):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.use_cache = False  # is always False in prod

    def make_url(self, row, _type):
        args = self.prepare_request_args(row, _type)
        return f"https://old.reddit.com/r/all/new.json?after={args['cursor']}"

    async def fetch_rows(self, row, _type="to"):
        try:
            url = self.make_url(row, _type)
            res = await self.http_get(url)

            if res.status_code == 200:
                rows = []
                content = res.json()
                for r in self.clamp_rows_length(content["data"]["children"]):
                    rows.append(
                        self.make_a_row(
                            row["uri"],
                            self.mini_uri(f"http://reddit.com/{r['data']['id']}"),
                            r,
                        )
                    )

                return {
                    "rows": rows,
                    "state": {"pagination": {_type: content["data"]["after"]}},
                }
            else:
                self.logger.error(
                    "Non-200 HTTP response: %s : %s" % (res.status_code, url)
                )
                return self.make_error("HTTP", res.status_code, url)
        except Exception as e:
            self.logger.exception(e)
            return self.make_error("Exception", type(e), str(e))
