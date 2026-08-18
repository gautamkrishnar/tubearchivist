"""
functionality:
- base class to make all calls to yt-dlp
- handle yt-dlp errors
"""

import os
import time
from datetime import datetime
from http import cookiejar
from io import StringIO
from os import path

import requests
import yt_dlp
from appsettings.src.config import AppConfig
from common.src.env_settings import EnvironmentSettings
from common.src.helper import deep_merge, rand_sleep
from common.src.ta_redis import RedisArchivist
from django.conf import settings


class YtWrap:
    """wrap calls to yt"""

    BOT_MESSAGES = [
        "not a bot",
    ]
    BOT_ERROR_LOG = "YouTube bot detection, abort!"
    RELOAD_MESSAGE = "The page needs to be reloaded"
    COOKIE_REFRESH_WAIT = 15
    # cached UA fetched once from kasm Chrome via cookie-refresher sidecar
    _chrome_ua: str | None = None

    OBS_BASE = {
        "default_search": "ytsearch",
        "quiet": True,
        "socket_timeout": 10,
        "extractor_retries": 3,
        "retries": 10,
        "cachedir": path.abspath(
            path.join(EnvironmentSettings.CACHE_DIR, "ytdlp")
        ),
        "plugin_dirs": [],
    }

    def __init__(self, obs_request, config=False):
        self.obs_request = obs_request
        self.config = config
        self.build_obs()

    @classmethod
    def _get_chrome_ua(cls) -> str | None:
        """fetch Chrome UA from kasm sidecar once, cache at class level"""
        if cls._chrome_ua:
            return cls._chrome_ua
        trigger_url = os.environ.get("COOKIE_REFRESH_URL", "").rstrip("/")
        if not trigger_url:
            return None
        try:
            ua = requests.get(f"{trigger_url}/useragent", timeout=5).text.strip()
            if ua:
                cls._chrome_ua = ua
                print(f"[chrome-ua] using kasm Chrome UA: {ua[:80]}")
            return cls._chrome_ua
        except Exception as err:
            print(f"[chrome-ua] failed to fetch: {err}")
            return None

    def build_obs(self):
        """build yt-dlp obs"""
        self.obs = self.OBS_BASE.copy()
        deep_merge(self.obs, self.obs_request)
        if self.config:
            self._add_cookie()
            self._add_potoken_url()

        ua = self._get_chrome_ua()
        if ua:
            self.obs.setdefault("http_headers", {})["User-Agent"] = ua

        if getattr(settings, "DEBUG", False):
            del self.obs["quiet"]
            print(self.obs)

    def _add_cookie(self):
        """add cookie if enabled"""
        if self.config["downloads"]["cookie_import"]:
            cookie_io = CookieHandler(self.config).get()
        else:
            cookie_io = CookieHandler(self.config).get("cookie_temp")

        self.obs["cookiefile"] = cookie_io

    def _add_potoken_url(self):
        """add bgutils token url"""
        if pot_provider_url := self.config["downloads"].get(
            "pot_provider_url"
        ):
            deep_merge(
                self.obs,
                {
                    "extractor_args": {
                        # web avoids web_safari SABR default when cookies present.
                        # bgutil provides the PO token the web client requires.
                        # android is NOT listed here — yt-dlp skips it when cookies
                        # are present ("doesn't support cookies"). The SABR retry in
                        # download() handles android without cookies.
                        "youtube": {"player_client": ["web"]},
                        "youtubepot-bgutilhttp": {
                            "base_url": [pot_provider_url]
                        },
                    }
                },
            )
            return

        # from fork: https://github.com/bbilly1/bgutil-ytdlp-pot-provider
        deep_merge(
            self.obs,
            {
                "extractor_args": {
                    "youtubepot-bgutilhttp": {"disable": ["True"]}
                }
            },
        )

    def _trigger_cookie_refresh(self) -> bool:
        """
        Call the cookie-refresher sidecar's /refresh endpoint.
        The sidecar navigates YouTube in kasm Chrome; the Companion extension
        detects the resulting cookie change and pushes fresh cookies to TA.
        Returns True if the refresh completed, False if not configured or failed.
        """
        trigger_url = os.environ.get("COOKIE_REFRESH_URL", "").rstrip("/")
        if not trigger_url:
            return False

        print(f"[cookie-refresh] calling {trigger_url}/refresh")
        try:
            resp = requests.post(f"{trigger_url}/refresh", timeout=60)
            if resp.ok:
                print("[cookie-refresh] done — waiting for extension to push cookies")
                time.sleep(self.COOKIE_REFRESH_WAIT)
                return True
            print(f"[cookie-refresh] sidecar returned {resp.status_code}: {resp.text[:100]}")
            return False
        except Exception as err:
            print(f"[cookie-refresh] failed: {err}")
            return False

    def _handle_format_unavailable(self, url: str, original_err: str) -> tuple:
        """
        SABR experiment retry path. Called when:
        - 'Requested format is not available' (web client, SABR-only)
        - 'Only images are available' (web client, Shorts)
        - empty DASH fragments crash (RuntimeError → StopIteration in dash.py)

        Strategy:
          1. Retry without cookies using android — works for public videos.
          2. If android also fails with age verification, retry with cookies + ios.
        """
        print(f"{url}: SABR-only / no formats — retrying without cookies (android)")
        sabr_obs = self.obs.copy()
        sabr_obs.pop("cookiefile", None)
        sabr_obs["extractor_args"] = {"youtube": {"player_client": ["android"]}}
        sabr_obs["check_formats"] = "selected"
        try:
            with yt_dlp.YoutubeDL(sabr_obs) as ydl:
                ydl.download([url])
            return True, True
        except (yt_dlp.utils.DownloadError, RuntimeError) as sabr_err:
            sabr_err_str = str(sabr_err)
            print(f"{url}: cookieless android retry failed: {sabr_err_str}")

            if "confirm your age" in sabr_err_str or "Sign in" in sabr_err_str:
                # age-restricted + SABR: web client forces SABR, ios/android skip
                # when cookies present. web_embedded bypasses age-restriction and
                # doesn't participate in the SABR experiment.
                print(f"{url}: age-restricted + SABR — retrying with cookies + web_embedded")
                self.build_obs()  # fresh StringIO — previous call consumed it to EOF
                embedded_obs = self.obs.copy()
                embedded_obs["extractor_args"] = {"youtube": {"player_client": ["web_embedded"]}}
                embedded_obs.pop("youtubepot-bgutilhttp", None)
                try:
                    with yt_dlp.YoutubeDL(embedded_obs) as ydl:
                        ydl.download([url])
                    self._validate_cookie()
                    return True, True
                except (yt_dlp.utils.DownloadError, RuntimeError) as emb_err:
                    print(f"{url}: web_embedded retry failed: {emb_err}")
                    return False, str(emb_err)

            return False, sabr_err_str

    def download(self, url):
        """make download request"""
        self.obs.update({"check_formats": "selected"})
        with yt_dlp.YoutubeDL(self.obs) as ydl:
            try:
                ydl.download([url])
            except RuntimeError as err:
                # dash.py raises RuntimeError("generator raised StopIteration")
                # when check_formats tests a DASH format with no fragments (SABR/empty).
                # Treat it as a format-unavailable error so the SABR retry runs.
                if "StopIteration" in str(err):
                    err_str = "Requested format is not available. Use --list-formats for a list of available formats"
                    print(f"{url}: empty DASH fragments — treating as format unavailable")
                else:
                    raise
                return self._handle_format_unavailable(url, err_str)
            except yt_dlp.utils.DownloadError as err:
                print(f"{url}: failed to download with message {err}")
                if "Temporary failure in name resolution" in str(err):
                    raise ConnectionError("lost the internet, abort!") from err
                if any(m in str(err) for m in self.BOT_MESSAGES):
                    print(self.BOT_ERROR_LOG)
                    rand_sleep(self.config)
                    raise ConnectionError(self.BOT_ERROR_LOG) from err

                if self.RELOAD_MESSAGE in str(err):
                    print(f"{url}: page reload required — triggering cookie refresh")
                    if self._trigger_cookie_refresh():
                        # rebuild obs with fresh cookies pushed by extension
                        self.build_obs()
                        print(f"{url}: retrying download with fresh cookies")
                        try:
                            with yt_dlp.YoutubeDL(self.obs) as ydl2:
                                ydl2.download([url])
                            self._validate_cookie()
                            return True, True
                        except yt_dlp.utils.DownloadError as retry_err:
                            print(f"{url}: retry failed: {retry_err}")
                            return False, str(retry_err)

                if "Requested format is not available" in str(err) or "Only images are available" in str(err):
                    return self._handle_format_unavailable(url, str(err))

                return False, str(err)

        self._validate_cookie()

        return True, True

    def extract(self, url) -> tuple[dict | None, str | None]:
        """
        make extract request
        returns response, error
        """
        with yt_dlp.YoutubeDL(self.obs) as ydl:
            try:
                response = ydl.extract_info(url)
            except cookiejar.LoadError as err:
                print(f"cookie file is invalid: {err}")
                return None, str(err)
            except yt_dlp.utils.ExtractorError as err:
                print(f"{url}: failed to extract: {err}, continue...")
                return None, str(err)
            except yt_dlp.utils.DownloadError as err:
                if "This channel does not have a" in str(err):
                    return None, None

                print(f"{url}: failed to get info from youtube: {err}")
                if "Temporary failure in name resolution" in str(err):
                    raise ConnectionError("lost the internet, abort!") from err
                if any(m in str(err) for m in self.BOT_MESSAGES):
                    print(self.BOT_ERROR_LOG)
                    rand_sleep(self.config)
                    raise ConnectionError(self.BOT_ERROR_LOG) from err

                return None, str(err)

        self._validate_cookie()

        return response, None

    def _validate_cookie(self):
        """check cookie and write it back for next use"""
        if not self.obs.get("cookiefile"):
            # empty in tests
            return

        self.obs["cookiefile"].seek(0)
        new_cookie = self.obs["cookiefile"].read().strip("\x00")

        if self.config["downloads"]["cookie_import"]:
            cookie_key = "cookie"
            expire = False
        else:
            cookie_key = "cookie_temp"
            expire = 60 * 30  # 30 min

        old_cookie = RedisArchivist().get_message_str(cookie_key)
        if new_cookie and old_cookie != new_cookie:
            print(f"refreshed stored {cookie_key}")
            RedisArchivist().set_message(
                cookie_key, new_cookie, expire=expire, save=True
            )


class CookieHandler:
    """handle youtube cookie for yt-dlp"""

    COOKIE_EMPTY = "# Netscape HTTP Cookie File\n"

    def __init__(self, config):
        self.cookie_io = False
        self.config = config

    def get(self, message_str: str = "cookie"):
        """get cookie io stream"""
        cookie = RedisArchivist().get_message_str(message_str)
        self.cookie_io = StringIO(cookie or self.COOKIE_EMPTY)
        return self.cookie_io

    def set_cookie(self, cookie):
        """set cookie str and activate in config"""
        cookie_clean = cookie.strip("\x00")
        RedisArchivist().set_message("cookie", cookie_clean, save=True)
        AppConfig().update_config({"downloads": {"cookie_import": True}})
        self.config["downloads"]["cookie_import"] = True
        print("[cookie]: activated and stored in Redis")

    @staticmethod
    def revoke():
        """revoke cookie"""
        RedisArchivist().del_message("cookie")
        RedisArchivist().del_message("cookie:valid")
        AppConfig().update_config({"downloads": {"cookie_import": False}})
        print("[cookie]: revoked")

    def validate(self) -> bool:
        """validate cookie using the liked videos playlist"""
        validation = RedisArchivist().get_message_dict("cookie:valid")
        if validation:
            print("[cookie]: used cached cookie validation")
            return True

        print("[cookie] validating cookie")
        obs_request = {
            "skip_download": True,
            "extract_flat": True,
        }
        validator = YtWrap(obs_request, self.config)
        response, error = validator.extract("LL")
        self.store_validation(bool(response))

        # update in redis to avoid expiring
        modified = validator.obs["cookiefile"].getvalue().strip("\x00")
        if modified:
            cookie_clean = modified.strip("\x00")
            RedisArchivist().set_message("cookie", cookie_clean)

        if not response:
            mess_dict = {
                "status": "message:download",
                "level": "error",
                "title": "Cookie validation failed, exiting...",
                "message": error,
            }
            RedisArchivist().set_message(
                "message:download", mess_dict, expire=4
            )
            print("[cookie]: validation failed, exiting...")

        print(f"[cookie]: validation success: {bool(response)}")
        return bool(response)

    @staticmethod
    def store_validation(response):
        """remember last validation"""
        now = datetime.now()
        message = {
            "status": response,
            "validated": int(now.timestamp()),
            "validated_str": now.strftime("%Y-%m-%d %H:%M"),
        }
        RedisArchivist().set_message("cookie:valid", message, expire=3600)
