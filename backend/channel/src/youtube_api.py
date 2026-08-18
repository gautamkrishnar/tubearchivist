"""
Functionality:
- fetch channel video lists via YouTube Data API v3
- fetch individual video metadata via YouTube Data API v3
- single UU playlist pass, classify into videos/shorts/streams
- round-robin key rotation across multiple API keys
- Redis cache: one UU scan per channel per 5 minutes regardless of how many
  type-specific calls are made (videos/shorts/streams entries each hit the cache)
"""

import json
import re
from datetime import datetime

import requests
from common.src.ta_redis import RedisArchivist

YOUTUBE_API_BASE = "https://www.googleapis.com/youtube/v3"
ROUND_ROBIN_KEY = "ta:youtube_api_key_idx"
CHANNEL_CACHE_PREFIX = "ta:ytapi:chan:"
CHANNEL_CACHE_TTL = 300  # 5 minutes — covers all type calls for the same scan run

SHORTS_MAX_SECONDS = 60
# default per-type scan depth when not explicitly requested
_SCAN_DEFAULT = 50
# hard cap on UU playlist pages per channel scan.
# each page = 2 quota units (playlistItems + videos.list).
# 20 pages × 2 × 76 channels = 3,040 units — safe for the 10,000/day free tier
# even with 3 scans per day (3 × 3,040 = 9,120 < 10,000).
MAX_PAGES_PER_CHANNEL = 20


def pick_api_key(keys: list[str]) -> str:
    """round-robin key selection — one Redis INCR per logical operation"""
    conn = RedisArchivist().conn
    idx = conn.incr(ROUND_ROBIN_KEY) - 1
    chosen = keys[idx % len(keys)]
    print(f"[youtube-api] key selected: ...{chosen[-6:]} (idx={idx % len(keys)})")
    return chosen


def _api_get(path: str, params: dict, api_key: str) -> dict:
    """make a YouTube Data API v3 GET request, raise on HTTP error with reason"""
    params = {**params, "key": api_key}
    resp = requests.get(f"{YOUTUBE_API_BASE}/{path}", params=params, timeout=15)
    if not resp.ok:
        body = resp.json() if "application/json" in resp.headers.get("content-type", "") else {}
        reason = body.get("error", {}).get("message", resp.text[:200])
        print(f"[youtube-api] HTTP {resp.status_code} on /{path}: {reason}")
        resp.raise_for_status()
    return resp.json()


def _duration_seconds(iso: str) -> int:
    """PT1H2M3S → total seconds"""
    m = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", iso or "")
    if not m:
        return 0
    return int(m.group(1) or 0) * 3600 + int(m.group(2) or 0) * 60 + int(m.group(3) or 0)


def _classify(detail: dict) -> str:
    """classify a video detail as videos / shorts / streams"""
    if detail.get("liveStreamingDetails"):
        return "streams"
    dur = _duration_seconds(detail.get("contentDetails", {}).get("duration", ""))
    return "shorts" if dur <= SHORTS_MAX_SECONDS else "videos"


def _best_thumb(thumbnails: dict) -> str:
    """pick highest-res available thumbnail URL"""
    for size in ("maxres", "high", "medium", "standard", "default"):
        url = thumbnails.get(size, {}).get("url")
        if url:
            return url
    return ""


def _parse_published(published_at: str) -> tuple[int, str]:
    """ISO 8601 publishedAt → (unix_timestamp, 'YYYYMMDD')"""
    try:
        dt = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
        return int(dt.timestamp()), dt.strftime("%Y%m%d")
    except (ValueError, AttributeError):
        print(f"[youtube-api] failed to parse publishedAt: {published_at!r}")
        return 0, ""


def _build_meta_from_item(item: dict) -> dict:
    """map a videos.list item to a yt-dlp-shaped metadata dict"""
    snippet = item["snippet"]
    content = item["contentDetails"]
    stats = item.get("statistics", {})
    live = item.get("liveStreamingDetails")

    thumb = _best_thumb(snippet.get("thumbnails", {}))
    timestamp, upload_date = _parse_published(snippet.get("publishedAt", ""))
    duration = _duration_seconds(content.get("duration", ""))
    if not live:
        live_status = "not_live"
    elif live.get("actualEndTime"):
        live_status = "was_live"       # completed stream — VOD, safe to download
    elif live.get("actualStartTime"):
        live_status = "is_live"        # currently broadcasting — skip
    else:
        live_status = "is_upcoming"    # scheduled but not started — skip
    privacy = item.get("status", {}).get("privacyStatus", "public")
    # members-only videos are "private" in the API when accessed without membership
    availability = "subscriber_only" if privacy == "private" else "public"

    return {
        "id": item["id"],
        "title": snippet.get("title", ""),
        "description": snippet.get("description", ""),
        "channel_id": snippet.get("channelId", ""),
        "channel": snippet.get("channelTitle", ""),
        "thumbnail": thumb,
        "thumbnails": [{"url": thumb}] if thumb else [],
        "upload_date": upload_date,
        "timestamp": timestamp,
        "duration": duration,
        "view_count": int(stats.get("viewCount") or 0),
        "like_count": int(stats.get("likeCount") or 0),
        "dislike_count": 0,
        "average_rating": None,
        "tags": snippet.get("tags", []),
        "categories": [],
        "live_status": live_status,
        "availability": availability,
    }


def get_videos_meta_batch(
    video_ids: list[str],
    api_key: str,
) -> dict[str, dict]:
    """
    Batch-fetch metadata for a list of video IDs.
    Makes one videos.list call per 50 IDs (API maximum).
    Returns {video_id: metadata_dict}.
    """
    results: dict[str, dict] = {}
    batch_size = 50
    total = len(video_ids)
    for start in range(0, total, batch_size):
        batch = video_ids[start : start + batch_size]
        print(f"[youtube-api] batch metadata {start + 1}-{min(start + batch_size, total)}/{total}")
        data = _api_get(
            "videos",
            {
                "part": "snippet,contentDetails,statistics,liveStreamingDetails,status",
                "id": ",".join(batch),
            },
            api_key,
        )
        for item in data.get("items", []):
            meta = _build_meta_from_item(item)
            results[item["id"]] = meta
            print(
                f"[youtube-api] {item['id']}: '{meta['title'][:50]}' "
                f"duration={meta['duration']}s live={meta['live_status']}"
            )
    unavailable = [v for v in video_ids if v not in results]
    if unavailable:
        print(f"[youtube-api] {len(unavailable)} unavailable (deleted/private): {unavailable[:5]}")
    return results


def get_video_meta(video_id: str, api_key: str) -> dict | None:
    """
    Fetch metadata for a single video. Prefer get_videos_meta_batch when
    processing multiple videos — it's far more quota-efficient.
    """
    print(f"[youtube-api] single-video metadata fetch for {video_id}")
    result = get_videos_meta_batch([video_id], api_key)
    return result.get(video_id)


def _do_scan(
    channel_id: str,
    api_key: str,
    limits: dict[str, int | None],
) -> dict[str, list[dict]]:
    """
    Core UU playlist scan — single pass, classify into videos/shorts/streams.
    limits keys must be the full set of types to track (missing types ignored).
    """
    uploads_playlist = "UU" + channel_id[2:]
    results: dict[str, list[dict]] = {t: [] for t in limits}
    configured = {t: lim for t, lim in limits.items() if lim is not None}
    page_token: str | None = None
    page_num = 0

    while True:
        if configured and all(len(results[t]) >= lim for t, lim in configured.items()):
            print(f"[youtube-api] {channel_id}: all limits satisfied after {page_num} page(s)")
            break

        page_num += 1
        page_data = _api_get(
            "playlistItems",
            {
                "part": "contentDetails,snippet",
                "playlistId": uploads_playlist,
                "maxResults": 50,
            } | ({"pageToken": page_token} if page_token else {}),
            api_key,
        )

        items = page_data.get("items", [])
        if not items:
            print(f"[youtube-api] {channel_id}: empty page {page_num}, done")
            break

        video_ids = [i["contentDetails"]["videoId"] for i in items]
        titles = {i["contentDetails"]["videoId"]: i["snippet"].get("title", "") for i in items}

        details_data = _api_get(
            "videos",
            {"part": "contentDetails,liveStreamingDetails", "id": ",".join(video_ids)},
            api_key,
        )
        details = {v["id"]: v for v in details_data.get("items", [])}

        skipped_unavailable = 0
        skipped_unrequested = 0
        for vid_id in video_ids:
            if vid_id not in details:
                skipped_unavailable += 1
                continue
            vid_type = _classify(details[vid_id])
            if vid_type not in results:
                skipped_unrequested += 1
                continue
            lim = limits[vid_type]
            if lim is None or len(results[vid_type]) < lim:
                results[vid_type].append(
                    {"id": vid_id, "title": titles.get(vid_id, ""), "vid_type": vid_type}
                )

        counts = {t: len(v) for t, v in results.items()}
        print(
            f"[youtube-api] {channel_id}: page {page_num} — "
            f"fetched={len(items)} unavailable={skipped_unavailable} "
            f"unrequested={skipped_unrequested} running_totals={counts}"
        )

        page_token = page_data.get("nextPageToken")
        if not page_token:
            print(f"[youtube-api] {channel_id}: no more pages after page {page_num}")
            break

        if page_num >= MAX_PAGES_PER_CHANNEL:
            print(
                f"[youtube-api] {channel_id}: hit MAX_PAGES_PER_CHANNEL={MAX_PAGES_PER_CHANNEL}, "
                f"stopping — totals so far: {counts}"
            )
            break

    return results


def get_channel_videos(
    channel_id: str,
    keys: list[str],
    limits: dict[str, int | None],
    to_skip: set | None = None,
) -> dict[str, list[dict]]:
    """
    Subscription-optimised channel scan — minimal API quota.

    Strategy:
      1. playlistItems.list(part=contentDetails) — video IDs only, 1 unit/page
      2. Filter against to_skip (already in TA) — free
      3. videos.list ONLY for new IDs — full metadata + classification in one call
      4. Early-stop: full page with 0 new IDs → everything older is indexed

    limits: {"videos": 50, "shorts": 10, "streams": 10} — None means unlimited.
    to_skip: set of video IDs already in TA (ta_video + ta_download).

    Returns: dict keyed by type. Each value is a list of full metadata dicts
             (same shape as _build_meta_from_item) plus "vid_type".
    """
    api_key = pick_api_key(keys)
    uploads_playlist = "UU" + channel_id[2:]
    to_skip = to_skip or set()

    results: dict[str, list[dict]] = {t: [] for t in limits}

    # Fetch only the first page (50 most recent uploads) — 1 API call.
    # The first page covers everything posted since the last scan for any
    # channel that runs regularly. If a channel posted >50 videos between
    # scans, the overflow is picked up on the next run.
    page_data = _api_get(
        "playlistItems",
        {
            "part": "contentDetails",
            "playlistId": uploads_playlist,
            "maxResults": 50,
        },
        api_key,
    )

    items = page_data.get("items", [])
    all_ids = [i["contentDetails"]["videoId"] for i in items]
    new_ids = [vid for vid in all_ids if vid not in to_skip]

    print(
        f"[youtube-api] {channel_id}: "
        f"{len(all_ids)} recent uploads, {len(new_ids)} new"
    )

    if not new_ids:
        return results

    # One videos.list call for new IDs — full metadata + classification
    details_data = _api_get(
        "videos",
        {
            "part": "snippet,contentDetails,statistics,liveStreamingDetails,status",
            "id": ",".join(new_ids),
        },
        api_key,
    )
    details = {v["id"]: v for v in details_data.get("items", [])}

    for vid_id in new_ids:
        if vid_id not in details:
            continue  # deleted/private
        meta = _build_meta_from_item(details[vid_id])
        vid_type = _classify(details[vid_id])
        if vid_type not in results:
            continue
        lim = limits[vid_type]
        if lim is None or len(results[vid_type]) < lim:
            meta["vid_type"] = vid_type
            results[vid_type].append(meta)

    summary = {t: len(v) for t, v in results.items()}
    print(f"[youtube-api] {channel_id}: done — {summary}")
    return results
