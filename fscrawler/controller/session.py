import asyncio
import sys
import logging
import time
import threading

import httpx as requests

FAMILYSEARCH_LOGIN = "https://www.familysearch.org/auth/familysearch/login"
AUTHORIZATION = "https://ident.familysearch.org/cis-web/oauth2/v3/authorization"
BASE_URL = 'https://www.familysearch.org:443'
CURRENT_USER = "/platform/users/current.json"
FSSESSIONID = "fssessionid"
CONTINUE = object()
RETRY = object()

logger = logging.getLogger(__name__)


class Session:
    """ Create a FamilySearch session
        :param username and password: valid FamilySearch credentials
        :param verbose: True to active verbose mode
        :param timeout: time before retry a request
    """

    def __init__(self, username, password, verbose=False, timeout=15,
                 requests_per_second: float = 0.0,
                 max_retries: int = 5,
                 backoff_base_seconds: float = 1.0,
                 backoff_multiplier: float = 2.0,
                 backoff_max_seconds: float = 60.0):
        self.username = username
        self.password = password
        self.verbose = verbose
        self.timeout = timeout
        self.max_retries = max(0, max_retries)
        self.backoff_base_seconds = max(backoff_base_seconds, 0.0)
        self.backoff_multiplier = max(backoff_multiplier, 1.0)
        self.backoff_max_seconds = max(backoff_max_seconds, 0.0)
        self.rate_limiter = RateLimiter(requests_per_second) if requests_per_second > 0 else None
        self.fid = self.lang = self.display_name = None
        self.counter = 0
        self.client = None
        self.fssessionid = None
        self.logged = self.login()

    def write_log(self, text):
        """ write text in the log file """
        log = "[%s]: %s\n" % (time.strftime("%Y-%m-%d %H:%M:%S"), text)
        if self.verbose:
            sys.stderr.write(log)

    def login(self):
        """ retrieve FamilySearch session ID
            (https://familysearch.org/developers/docs/guides/oauth2)
        """
        while True:
            url = FAMILYSEARCH_LOGIN
            try:
                self.write_log("Downloading: " + url)
                r = requests.get(url, params={"ldsauth": False}, follow_redirects=False)
                url = r.headers["Location"]
                self.write_log("Downloading: " + url)
                r = requests.get(url, follow_redirects=False)
                idx = r.text.index('name="params" value="')
                span = r.text[idx + 21:].index('"')
                params = r.text[idx + 21: idx + 21 + span]

                url = AUTHORIZATION
                self.write_log("Downloading: " + url)
                r = requests.post(
                    url,
                    data={"params": params, "userName": self.username, "password": self.password},
                    follow_redirects=False,
                )

                if "The username or password was incorrect" in r.text:
                    self.write_log("The username or password was incorrect")
                    return False

                if "Invalid Oauth2 Request" in r.text:
                    self.write_log("Invalid Oauth2 Request")
                    time.sleep(self.timeout)
                    continue

                url = r.headers["Location"]
                self.write_log("Downloading: " + url)
                r = requests.get(url, follow_redirects=False)
                self.fssessionid = r.cookies[FSSESSIONID]
                self.client = requests.Client(base_url=BASE_URL,
                                              cookies={FSSESSIONID: self.fssessionid},
                                              timeout=self.timeout)
            except requests.ReadTimeout:
                logger.warning(f"Read timed out: {url}")
                continue
            except requests.HTTPError as e:
                logger.warning(f"Error, url: {url}, HTTPError: {e}")
                time.sleep(self.timeout)
                continue
            except KeyError:
                logger.warning(f"KeyError with url: {url}")
                time.sleep(self.timeout)
                continue
            except ValueError:
                logger.warning(f"ValueError with url: {url}")
                time.sleep(self.timeout)
                continue
            self.write_log("FamilySearch session id: " + self.fssessionid)
            self.set_current()
            return True

    def set_current(self):
        """ retrieve FamilySearch current user ID, name and language """
        url = CURRENT_USER
        data = self.get_url(url)
        if data:
            self.fid = data["users"][0]["personId"]
            self.lang = data["users"][0]["preferredLanguage"]
            self.display_name = data["users"][0]["displayName"]

    def get_url(self, url):
        """ retrieve JSON structure from a FamilySearch URL """
        self.counter += 1
        attempt = 0
        while True:
            if self.rate_limiter:
                self.rate_limiter.wait()
            self.write_log("Getting: " + url)
            r = self.client.get(url)
            signal, payload = self._process_response(r)
            if signal is CONTINUE:
                attempt = 0
                continue
            if signal is RETRY:
                if attempt >= self.max_retries:
                    logger.warning(f"Exceeded retry attempts for {url}")
                    return {'error': r}
                delay = self._compute_backoff(attempt)
                attempt += 1
                time.sleep(delay)
                continue
            return payload

    async def get_urla(self, url):
        """ asynchronously retrieve JSON structure from a FamilySearch URL """
        self.counter += 1
        attempt = 0
        async with requests.AsyncClient(base_url=BASE_URL,
                                        cookies={FSSESSIONID: self.fssessionid},
                                        timeout=self.timeout) as client:
            while True:
                if self.rate_limiter:
                    await self.rate_limiter.wait_async()
                self.write_log("Getting: " + url)
                r = await client.get(url)
                signal, payload = self._process_response(r)
                if signal is CONTINUE:
                    attempt = 0
                    continue
                if signal is RETRY:
                    if attempt >= self.max_retries:
                        logger.warning(f"Exceeded retry attempts for {url}")
                        return {'error': r}
                    delay = self._compute_backoff(attempt)
                    attempt += 1
                    await asyncio.sleep(delay)
                    continue
                return payload

    def _process_response(self, r):
        if r.status_code == 204:
            return None, None
        if r.status_code in {404, 405, 410}:
            logger.warning(f"WARNING: status: {r.status_code}, url: {r.url}")
            return None, {'error': r}
        if r.status_code == 401:
            self.login()
            return CONTINUE, None
        if r.status_code == 429 or r.status_code >= 500:
            logger.warning(f"Throttled or server error: status {r.status_code}, url: {r.url}")
            return RETRY, None
        try:
            r.raise_for_status()
        except requests.HTTPError:
            logger.warning(f"HTTPError: status: {r.status_code}, url: {r.url}")
            return None, {'error': r}
        try:
            return None, r.json()
        except Exception as e:
            logger.warning(f"WARNING: corrupted file from {r.url}, error: {e}")
            return None, {'error': r}

    def _compute_backoff(self, attempt: int) -> float:
        delay = self.backoff_base_seconds * (self.backoff_multiplier ** max(attempt, 0))
        if self.backoff_max_seconds > 0:
            delay = min(delay, self.backoff_max_seconds)
        return delay
class RateLimiter:
    def __init__(self, requests_per_second: float):
        self.requests_per_second = max(requests_per_second, 0.0)
        self._interval = 1.0 / self.requests_per_second if self.requests_per_second > 0 else 0.0
        self._lock = threading.Lock()
        self._next_allowed = time.monotonic()

    def _compute_delay(self) -> float:
        with self._lock:
            now = time.monotonic()
            if now >= self._next_allowed:
                self._next_allowed = now + self._interval
                return 0.0
            delay = self._next_allowed - now
            self._next_allowed += self._interval
            return delay

    async def wait_async(self):
        if self._interval == 0:
            return
        while True:
            delay = self._compute_delay()
            if delay <= 0:
                return
            await asyncio.sleep(delay)

    def wait(self):
        if self._interval == 0:
            return
        while True:
            delay = self._compute_delay()
            if delay <= 0:
                return
            time.sleep(delay)
