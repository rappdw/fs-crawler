import sys
import logging
import time

import httpx as requests

FAMILYSEARCH_LOGIN = "https://www.familysearch.org/auth/familysearch/login"
AUTHORIZATION = "https://ident.familysearch.org/cis-web/oauth2/v3/authorization"
BASE_URL = 'https://www.familysearch.org:443'
CURRENT_USER = "/platform/users/current.json"
FSSESSIONID = "fssessionid"
CONTINUE = object()

logger = logging.getLogger(__name__)


class Session:
    """ Create a FamilySearch session
        :param username and password: valid FamilySearch credentials
        :param verbose: True to active verbose mode
        :param timeout: time before retry a request
    """

    def __init__(self, username, password, verbose=False, timeout=15):
        self.username = username
        self.password = password
        self.verbose = verbose
        self.timeout = timeout
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
        while True:
            self.write_log("Getting: " + url)
            r = self.client.get(url)
            result = self._process_response(r)
            if result == CONTINUE:
                continue
            return result

    async def get_urla(self, url):
        """ asynchronously retrieve JSON structure from a FamilySearch URL """
        self.counter += 1
        async with requests.AsyncClient(base_url=BASE_URL,
                                        cookies={FSSESSIONID: self.fssessionid},
                                        timeout=self.timeout) as client:
            while True:
                self.write_log("Getting: " + url)
                r = await client.get(url)
                result = self._process_response(r)
                if result == CONTINUE:
                    continue
                return result

    def _process_response(self, r):
        if r.status_code == 204:
            return None
        if r.status_code in {404, 405, 410, 500}:
            logger.warning(f"WARNING: status: {r.status_code}, url: {r.url}")
            return {'error': r}
        if r.status_code == 401:
            self.login()
            return CONTINUE
        try:
            r.raise_for_status()
        except requests.HTTPError:
            logger.warning(f"HTTPError: status: {r.status_code}, url: {r.url}")
            return {'error': r}
        try:
            return r.json()
        except Exception as e:
            logger.warning(f"WARNING: corrupted file from {r.url}, error: {e}")
            return {'error': r}
