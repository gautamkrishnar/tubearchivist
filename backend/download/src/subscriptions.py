"""
Functionality:
- handle channel subscriptions
- handle playlist subscriptions
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from appsettings.src.config import AppConfig
from channel.src.index import YoutubeChannel
from channel.src.remote_query import VideoQueryBuilder
from channel.src.youtube_api import get_channel_videos
from common.src.helper import get_channels, get_duration_str, get_playlists
from common.src.urlparser import ParsedURLType, Parser
from download.src.queue import PendingList
from download.src.thumbnails import ThumbManager
from playlist.src.index import YoutubePlaylist
from video.src.constants import VideoTypeEnum
from video.src.index import YoutubeVideo

# Max parallel channel scans.
_MAX_WORKERS = 10


class ChannelSubscription:
    """scan subscribed channels to find missing videos to add to pending"""

    def __init__(self, task=None):
        self.config = AppConfig().config
        self.task = task

    def find_missing(self) -> int:
        """find missing videos from channel subscriptions"""
        if self.task:
            self.task.send_progress(["Looking up channels."])

        all_channels = get_channels(
            subscribed_only=True,
            source=["channel_id", "channel_overwrites", "channel_tabs"],
        )
        if not all_channels:
            return 0

        api_key_raw = self.config["downloads"].get("youtube_api_key")
        if api_key_raw:
            keys = [k.strip() for k in api_key_raw.split(",") if k.strip()]
            if keys:
                return self._find_missing_via_api(all_channels, keys)

        # yt-dlp fallback path (unchanged)
        all_channel_urls = self._process_channel_urls(all_channels)

        if self.task:
            self.task.send_progress([f"Scanning {len(all_channels)} channels"])

        pending_handler = PendingList(
            youtube_ids=all_channel_urls,
            task=self.task,
            auto_start=self.config["subscriptions"].get("auto_start", False),
            flat=self.config["subscriptions"].get("extract_flat", False),
        )
        return pending_handler.parse_url_list()

    def _build_channel_tasks(
        self, all_channels: list[dict]
    ) -> list[tuple[str, dict[str, int | None]]]:
        """return [(channel_id, {vid_type: limit, ...}), ...] respecting overwrites"""
        tasks = []
        for channel in all_channels:
            tabs = channel.get("channel_tabs") or []
            if not tabs:
                continue
            enums = [getattr(VideoTypeEnum, t.upper()) for t in tabs]
            queries = VideoQueryBuilder(
                config=self.config,
                channel_overwrites=channel.get("channel_overwrites", {}),
            ).build_queries(vid_types=enums)
            if queries:
                limits = {vt.value: lim for vt, lim in queries}
                tasks.append((channel["channel_id"], limits))
        return tasks

    def _find_missing_via_api(
        self, all_channels: list[dict], keys: list[str]
    ) -> int:
        """
        Fast parallel API path:
          1. RSS pre-filter — skip channels with no new content (free, zero quota)
          2. Cooldown — skip channels scanned within _CHANNEL_SCAN_COOLDOWN seconds
          3. Parallel channel video list fetch (ThreadPoolExecutor)
          4. Deduplicate + filter already-indexed/queued
          5. Batch metadata fetch (50 videos per API call)
          6. Bulk write to ta_download
        """
        channel_tasks = self._build_channel_tasks(all_channels)
        if not channel_tasks:
            return 0

        # Build to_skip BEFORE scanning — pass it to get_channel_videos so it
        # can skip already-indexed videos during the scan and stop early.
        pending = PendingList(
            youtube_ids=[],
            task=self.task,
            auto_start=self.config["subscriptions"].get("auto_start", False),
        )
        pending.get_download()
        pending.get_indexed()
        pending.get_channels()
        to_skip = set(pending.to_skip)

        total_channels = len(channel_tasks)
        print(f"[api-scan] scanning {total_channels} channels ({len(to_skip)} already in TA)")
        if self.task:
            self.task.send_progress([f"Scanning {total_channels} channels via YouTube API"])

        # Parallel channel scans — get_channel_videos returns full metadata for
        # NEW videos only (already skips to_skip internally, early-stops when
        # a full page is all-indexed)
        all_found: list[dict] = []
        done = 0
        with ThreadPoolExecutor(max_workers=min(_MAX_WORKERS, total_channels)) as executor:
            futures = {
                executor.submit(get_channel_videos, ch_id, keys, limits, to_skip): ch_id
                for ch_id, limits in channel_tasks
            }
            for future in as_completed(futures):
                ch_id = futures[future]
                done += 1
                if self.task:
                    self.task.send_progress(
                        [f"Channel scan {done}/{total_channels}"],
                        progress=done / total_channels * 0.8,
                    )
                try:
                    result = future.result()
                    for type_vids in result.values():
                        all_found.extend(type_vids)
                except Exception as err:
                    print(f"[api-scan] {ch_id}: scan failed: {err}")

        print(f"[api-scan] {len(all_found)} new videos found across all channels")

        if not all_found:
            return 0

        # Deduplicate (channel with multiple tabs may return same video in different type buckets)
        seen: set[str] = set()
        new_videos: list[dict] = []
        for v in all_found:
            if v["id"] not in seen:
                seen.add(v["id"])
                new_videos.append(v)

        print(f"[api-scan] {len(new_videos)} unique new videos to queue")

        # Build pending entries from full metadata returned by get_channel_videos
        # (no separate metadata batch call needed — already fetched during scan)
        all_channels_set = set(pending.all_channels or [])
        now = int(datetime.now().timestamp())
        added = 0

        for meta in new_videos:
            vid_id = meta["id"]

            if meta.get("availability") in ("subscriber_only", "premium_only", "needs_auth"):
                print(f"[api-scan] {vid_id}: skip members-only/premium")
                continue

            if meta.get("live_status") in ("is_live", "is_upcoming"):
                print(f"[api-scan] {vid_id}: skip is_live/is_upcoming")
                continue

            vid_type = meta.get("vid_type", "videos")
            thumb_url = meta.get("thumbnail") or ""

            entry = {
                "channel_id": meta["channel_id"],
                "channel_indexed": meta["channel_id"] in all_channels_set,
                "channel_name": meta["channel"],
                "duration": get_duration_str(meta.get("duration", 0)),
                "published": meta.get("timestamp") or meta.get("upload_date"),
                "timestamp": now,
                "title": meta["title"],
                "vid_thumb_url": thumb_url or None,
                "vid_type": vid_type,
                "youtube_id": vid_id,
            }

            if thumb_url:
                ThumbManager(item_id=vid_id).download_video_thumb(thumb_url)

            pending.missing_videos.append(entry)

            if len(pending.missing_videos) >= 50:
                added += pending.add_to_pending()
                pending.missing_videos = []

        if pending.missing_videos:
            added += pending.add_to_pending()
            pending.missing_videos = []

        print(f"[api-scan] complete — added {added} new videos to queue")
        return added

    def _process_channel_urls(self, all_channels: list[dict]):
        """build ParsedURLType list for yt-dlp fallback path"""
        all_channel_urls: list[ParsedURLType] = []
        for channel in all_channels:
            channel_tabs = channel["channel_tabs"]
            if not channel_tabs:
                continue
            enums = [getattr(VideoTypeEnum, i.upper()) for i in channel_tabs]
            queries = VideoQueryBuilder(
                config=self.config,
                channel_overwrites=channel.get("channel_overwrites", {}),
            ).build_queries(vid_types=enums)
            for vid_type, limit in queries:
                all_channel_urls.append(
                    ParsedURLType(
                        type="channel",
                        url=channel["channel_id"],
                        vid_type=vid_type,
                        limit=limit,
                    )
                )
        return all_channel_urls


class PlaylistSubscription:
    """scan subscribed playlists for videos to add to pending"""

    def __init__(self, task=None):
        self.config = AppConfig().config
        self.task = task

    def find_missing(self) -> int:
        """find missing"""
        all_playlists = get_playlists(
            subscribed_only=True, source=["playlist_id"]
        )
        if not all_playlists:
            return 0

        size_limit = self.config["subscriptions"]["playlist_size"]
        all_playlist_urls: list[ParsedURLType] = []
        for playlist in all_playlists:
            all_playlist_urls.append(
                ParsedURLType(
                    type="playlist",
                    url=playlist["playlist_id"],
                    vid_type=VideoTypeEnum.UNKNOWN,
                    limit=size_limit,
                )
            )

        pending_handler = PendingList(
            youtube_ids=all_playlist_urls,
            task=self.task,
            auto_start=self.config["subscriptions"].get("auto_start", False),
            flat=self.config["subscriptions"].get("extract_flat", False),
        )
        return pending_handler.parse_url_list()


class SubscriptionScanner:
    """add missing videos to queue"""

    def __init__(self, task=False):
        self.task = task
        self.missing_videos = False
        self.auto_start = AppConfig().config["subscriptions"].get("auto_start")

    def scan(self):
        """scan channels and playlists"""
        if self.task:
            self.task.send_progress(["Rescanning channels and playlists."])

        added = 0
        added += ChannelSubscription(task=self.task).find_missing()
        if self.task and not self.task.is_stopped():
            added += PlaylistSubscription(task=self.task).find_missing()

        return added


class SubscriptionHandler:
    """subscribe to channels and playlists from url_str"""

    def __init__(self, url_str, task=False):
        self.url_str = url_str
        self.task = task
        self.to_subscribe = False

    def subscribe(self, expected_type=False):
        """subscribe to url_str items"""
        if self.task:
            self.task.send_progress(["Processing form content."])
        self.to_subscribe = Parser(self.url_str).parse()

        total = len(self.to_subscribe)
        for idx, item in enumerate(self.to_subscribe):
            if self.task:
                self._notify(idx, item, total)

            self.subscribe_type(item, expected_type=expected_type)

    def subscribe_type(self, item, expected_type):
        """process single item"""
        if item["type"] == "playlist":
            if expected_type and expected_type != "playlist":
                raise TypeError(
                    f"expected {expected_type} url but got {item.get('type')}"
                )

            playlist = YoutubePlaylist(item["url"])
            playlist.change_subscribe(new_subscribe_state=True)
            return

        if item["type"] == "video":
            video = YoutubeVideo(item["url"])
            video.get_from_youtube()
            video.process_youtube_meta()
            channel_id = video.channel_id
        elif item["type"] == "channel":
            channel_id = item["url"]
        else:
            raise ValueError("failed to subscribe to: " + item["url"])

        if expected_type and expected_type != "channel":
            raise TypeError(
                f"expected {expected_type} url but got {item.get('type')}"
            )

        self._subscribe(channel_id)

    def _subscribe(self, channel_id):
        """subscribe to channel"""
        YoutubeChannel(channel_id).change_subscribe(new_subscribe_state=True)

    def _notify(self, idx, item, total):
        """send notification message to redis"""
        subscribe_type = item["type"].title()
        message_lines = [
            f"Subscribe to {subscribe_type}",
            f"Progress: {idx + 1}/{total}",
        ]
        self.task.send_progress(message_lines, progress=(idx + 1) / total)
