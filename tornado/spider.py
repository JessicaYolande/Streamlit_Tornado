import asyncio
import time
from datetime import timedelta
from html.parser import HTMLParser
from urllib.parse import urljoin, urldefrag

from tornado import gen, httpclient, queues

base_url = "http://www.tornadoweb.org/en/stable/"
concurrency = 10


async def get_links_from_url(url):
    response = await httpclient.AsyncHTTPClient().fetch(url)
    html = response.body.decode(errors="ignore")
    return get_links(url, html)


def remove_fragment(url):
    pure_url, fragment = urldefrag(url)
    return pure_url


def get_links(base_url, html):
    class URLSeeker(HTMLParser):
        def __init__(self):
            HTMLParser.__init__(self)
            self.urls = set()

        def handle_starttag(self, tag, attrs):
            href = dict(attrs).get("href")
            if href is not None:
                self.urls.add(urljoin(base_url, href))

    url_seeker = URLSeeker()
    url_seeker.feed(html)
    return [remove_fragment(url) for url in url_seeker.urls]


async def main():
    q = queues.Queue()
    start = time.time()
    fetching = set()
    fetched = set()
    dead = set()

    async def fetch_url(url):
        if url in fetching or url in fetched or url in dead:
            return

        fetching.add(url)

        try:
            urls = await get_links_from_url(url)
            fetched.add(url)

            for new_url in urls:
                await q.put(new_url)

        except Exception:
            dead.add(url)

        finally:
            fetching.remove(url)

    async def worker():
        while True:
            url = await q.get()

            if url is None:
                q.task_done()
                break

            try:
                await fetch_url(url)
            finally:
                q.task_done()

    await q.put(base_url)

    workers = gen.multi([worker() for _ in range(concurrency)])

    await q.join(timeout=timedelta(seconds=300))

    print("fetched:", len(fetched))
    print("dead:", len(dead))
    print("fetching:", len(fetching))
    print("elapsed:", time.time() - start)

    for _ in range(concurrency):
        await q.put(None)

    await workers


if __name__ == "__main__":
    asyncio.run(main())