#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import json
import time
import logging
import datetime
import tempfile
from typing import Iterator
from diskcache import Index
from urllib.parse import urlparse
from xml.etree.ElementTree import Element

import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

import plexapi
import plexapi.base
import plexapi.media
import plexapi.video
import plexapi.myplex
import plexapi.server
import plexapi.library
import plexapi.exceptions


PLEX_URL = ""
PLEX_TOKEN = ""
WATCHED_HISTORY = ""
LOG_FILE = ""

PLEX_REQUESTS_SLEEP = 0
CHECK_USERS = [
]
USE_CACHE = False
CACHE_DIR = ""
DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

LOG_FORMAT = \
    "[%(name)s][%(process)05d][%(asctime)s][%(levelname)-8s][%(funcName)-15s]" \
    " %(message)s"
LOG_LEVEL = logging.INFO

METADATA_URL = "https://metadata.appln.tech"
MATCHES_URL = "/library/metadata/matches"

plexapi.server.TIMEOUT = 600
plexapi.server.X_PLEX_CONTAINER_SIZE = 1000
plexapi.base.USER_DONT_RELOAD_FOR_KEYS.update({
    'guid', 'guids', 'duration', 'title', 'userRating', 'viewCount', 'viewOffset', 'lastViewedAt', 'lastRatedAt'})

cache = Index()
session = requests.Session()
logger = logging.getLogger("PlexWatchedHistoryImporter")


def _get_config_str(key):
    return plexapi.CONFIG.get(key, default="", cast=str).strip("'").strip('"').strip()


def _check_plexapi_version():
    if plexapi.VERSION != "4.11.2":
        print("Please install PlexAPI Version: 4.11.2")
        raise Exception(f"Unsupported PlexAPI Version: {plexapi.VERSION}")


def _load_config():
    global PLEX_URL, PLEX_TOKEN, WATCHED_HISTORY, CHECK_USERS, LOG_FILE, LOG_LEVEL, USE_CACHE, CACHE_DIR
    if PLEX_URL == "":
        PLEX_URL = _get_config_str("sync.dst_url")
    if PLEX_TOKEN == "":
        PLEX_TOKEN = _get_config_str("sync.dst_token")
    if WATCHED_HISTORY == "":
        WATCHED_HISTORY = _get_config_str("sync.watched_history")
    if len(CHECK_USERS) == 0:
        config_check_users = _get_config_str("sync.check_users").split(",")
        CHECK_USERS = [user.strip().lower() for user in config_check_users if user]
    if LOG_FILE == "":
        LOG_FILE = _get_config_str("sync.import_log_file")
    debug = plexapi.utils.cast(bool, _get_config_str("sync.debug").lower())
    if debug:
        LOG_LEVEL = logging.DEBUG
    use_cache = plexapi.utils.cast(bool, _get_config_str("sync.use_cache").lower())
    if use_cache:
        USE_CACHE = True
    cache_dir = _get_config_str("sync.cache_dir")
    if CACHE_DIR == "":
        CACHE_DIR = cache_dir


def _setup_logger():
    logging.Formatter.converter = time.gmtime
    logging.raiseExceptions = False

    logger.setLevel(logging.DEBUG)
    logger.handlers = []
    logger.propagate = False

    detailed_formatter = logging.Formatter(fmt=LOG_FORMAT,
                                           datefmt=DATETIME_FORMAT)
    file_handler = logging.FileHandler(filename=LOG_FILE, mode="a+")
    file_handler.setFormatter(detailed_formatter)
    file_handler.setLevel(LOG_LEVEL)

    logger.addHandler(file_handler)


def _setup_session():
    global session
    session = requests.Session()
    retry_strategy = Retry(
        total=10,
        backoff_factor=0,
        raise_on_status=True,
        allowed_methods=["GET"],
        status_forcelist=[429, 500, 502, 503, 504],
    )
    session_adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=4,
                                  pool_maxsize=4, pool_block=True)
    session.mount('http://', session_adapter)
    session.mount('https://', session_adapter)


def _setup_cache():
    global cache

    cache_dir = tempfile.mkdtemp(prefix='diskcache-')
    if CACHE_DIR:
        cache_dir = CACHE_DIR

    logger.info(f"Using Cache Directory: {cache_dir}")

    cache = {
        'SHOW_METADATA_MAPPING': Index(f"{cache_dir}/show_metadata_mapping.cache"),
        'SHOW_GUID_RATING_KEY_MAPPING': Index(f"{cache_dir}/show_guid_rating_key_mapping.cache"),
        'MOVIE_GUID_RATING_KEY_MAPPING': Index(f"{cache_dir}/movie_guid_rating_key_mapping.cache"),
        'EPISODE_GUID_RATING_KEY_MAPPING': Index(f"{cache_dir}/episode_guid_rating_key_mapping.cache"),
    }

    cache['SHOW_GUID_RATING_KEY_MAPPING'].clear()
    cache['MOVIE_GUID_RATING_KEY_MAPPING'].clear()
    cache['EPISODE_GUID_RATING_KEY_MAPPING'].clear()


def _fetch_movie_metadata(tmdb_id: str) -> dict:
    movie_fetch_params = {
        'type': 1,
        'excludeElements': "Media",
        'guid': f"com.plexapp.agents.themoviedb://{tmdb_id}?lang=en",
    }

    response = session.post(METADATA_URL + MATCHES_URL, json=movie_fetch_params)
    if response.status_code != 200:
        print(response.__dict__)
        return {}

    metadata = response.json()
    if len(metadata.get("MediaContainer", {}).get("Metadata", [])) > 0:
        return metadata['MediaContainer']['Metadata'][0]

    return {}


def _get_movie_guid(tmdb_id: str) -> str:
    return _fetch_movie_metadata(tmdb_id).get("guid", "")


def _fetch_show_metadata(tvdb_id: str) -> dict:
    cached_show_metadata = cache['SHOW_METADATA_MAPPING'].get(tvdb_id)
    if cached_show_metadata is not None:
        return cached_show_metadata

    show_metadata = {}

    show_fetch_params = {
        'type': 2,
        'excludeElements': "Media",
        'guid': f"com.plexapp.agents.thetvdb://{tvdb_id}?lang=en",
    }
    response = session.post(METADATA_URL + MATCHES_URL, json=show_fetch_params)
    if response.status_code != 200:
        print(response.__dict__)
        return {}

    metadata = response.json()

    show_rating_key = ""
    if len(metadata.get("MediaContainer", {}).get("Metadata", [])) > 0:
        show_rating_key = metadata['MediaContainer']['Metadata'][0]['ratingKey']

    if show_rating_key:
        params = {
            'includeChildren': "1",
            'episodeOrder': "tvdbAiring",
        }
        response = session.get(METADATA_URL + f"/library/metadata/{show_rating_key}", params=params)
        if response.status_code != 200:
            print(response.__dict__)
            return {}

        metadata = response.json()
        show_metadata = metadata['MediaContainer']['Metadata'][0]
        show_metadata['Seasons'] = {}

    cache['SHOW_METADATA_MAPPING'][tvdb_id] = show_metadata
    return show_metadata


def _get_show_guid(tvdb_id: str) -> str:
    return _fetch_show_metadata(tvdb_id).get("guid", "")


def _get_episode_guid(tvdb_id: str, season_id: str, episode_id: str) -> str:
    show_metadata = _fetch_show_metadata(tvdb_id)
    if not show_metadata:
        return ""

    season_metadata = show_metadata['Seasons'].get(season_id)
    if season_metadata is None:
        season_metadata = {}

        season_rating_key = ""
        for season in show_metadata['Children'].get("Metadata", []):
            if str(season['index']) == season_id:
                season_rating_key = season['ratingKey']
                break

        if season_rating_key:
            params = {
                'includeChildren': "1",
            }
            response = session.get(METADATA_URL + f"/library/metadata/{season_rating_key}", params=params)
            if response.status_code != 200:
                print(response.__dict__)
                return ""

            metadata = response.json()
            season_metadata = metadata['MediaContainer']['Metadata'][0]

        cached_show_metadata = cache['SHOW_METADATA_MAPPING'][tvdb_id]
        cached_show_metadata['Seasons'][season_id] = season_metadata
        cache['SHOW_METADATA_MAPPING'][tvdb_id] = cached_show_metadata

    if not season_metadata:
        return ""

    for episode in season_metadata['Children'].get("Metadata", []):
        if str(episode['index']) == episode_id:
            return episode['guid']

    return ""


def _convert_to_plex_guid(guid: str, item_type: str) -> str:
    guid_url = urlparse(guid)

    if guid_url.scheme == "com.plexapp.agents.themoviedb":
        movie_guid = _get_movie_guid(guid_url.netloc)
        logger.debug(f"Converted: {item_type}: {guid}: {movie_guid}")
        return movie_guid

    if guid_url.scheme == "com.plexapp.agents.thetvdb":
        if item_type == "show":
            show_guid = _get_show_guid(guid_url.netloc)
            logger.debug(f"Converted: {item_type}: {guid}: {show_guid}")
            return show_guid
        elif item_type == "episode":
            if len(guid_url.path.split("/")) != 3:
                return ""
            _, season_id, episode_id = guid_url.path.split("/")
            episode_guid = _get_episode_guid(guid_url.netloc, season_id, episode_id)
            logger.debug(f"Converted: {item_type}: {guid}: {episode_guid}")
            return episode_guid

    return guid


# noinspection PyProtectedMember
def _section_item_iterator(plex_section: plexapi.library.LibrarySection, libtype: str) -> Iterator[Element]:
    key = f"/library/sections/{plex_section.key}/all?includeGuids=1&type={plexapi.utils.searchType(libtype)}"
    container_start = 0
    container_size = plexapi.server.X_PLEX_CONTAINER_SIZE
    total_size = plex_section._totalViewSize
    while total_size is None or container_start <= total_size:
        params = {
            'X-Plex-Container-Start': container_start,
            'X-Plex-Container-Size': container_size
        }
        items = plex_section._server.query(key, params=params)
        total_size = int(items.attrib.get("totalSize") or items.attrib.get("size"))
        for item in items:
            yield item
        container_start += container_size
        logger.debug(f"Loaded {plex_section.title}: {container_start}/{total_size}")


def _batch_section_get(plex_section: plexapi.library.LibrarySection, libtype: str) -> Iterator[Element]:
    yield from _section_item_iterator(plex_section, libtype)


def _get_guids(element: Element):
    guids = []

    for child in element:
        if child.tag == plexapi.media.Guid.TAG:
            guid_id = child.attrib.get("id")
            if guid_id:
                guids.append(guid_id)

    return guids


def _cache_guid_rating_key_mappings(plex_server: plexapi.server.PlexServer):
    sections = plex_server.library.sections()

    for section in sections:
        if isinstance(section, plexapi.library.MovieSection):
            for movie in _batch_section_get(section, "movie"):
                movie_guid = _convert_to_plex_guid(movie.attrib['guid'], "movie")
                if movie_guid == "":
                    movie_guid = movie.attrib['guid']

                movie_guid_rating_keys = cache['MOVIE_GUID_RATING_KEY_MAPPING'].get(movie_guid, [])
                movie_guid_rating_keys.append(int(movie.attrib['ratingKey']))
                cache['MOVIE_GUID_RATING_KEY_MAPPING'][movie_guid] = movie_guid_rating_keys

                guid: plexapi.media.Guid
                for guid in _get_guids(movie):
                    guid_rating_keys = cache['MOVIE_GUID_RATING_KEY_MAPPING'].get(guid, [])
                    guid_rating_keys.append(int(movie.attrib['ratingKey']))
                    cache['MOVIE_GUID_RATING_KEY_MAPPING'][guid] = guid_rating_keys

        elif isinstance(section, plexapi.library.ShowSection):
            for show in _batch_section_get(section, "show"):
                show_guid = _convert_to_plex_guid(show.attrib['guid'], "show")
                if show_guid == "":
                    show_guid = show.attrib['guid']

                show_guid_rating_keys = cache['SHOW_GUID_RATING_KEY_MAPPING'].get(show_guid, [])
                show_guid_rating_keys.append(int(show.attrib['ratingKey']))
                cache['SHOW_GUID_RATING_KEY_MAPPING'][show_guid] = show_guid_rating_keys

                guid: plexapi.media.Guid
                for guid in _get_guids(show):
                    guid_rating_keys = cache['SHOW_GUID_RATING_KEY_MAPPING'].get(guid, [])
                    guid_rating_keys.append(int(show.attrib['ratingKey']))
                    cache['SHOW_GUID_RATING_KEY_MAPPING'][guid] = guid_rating_keys

            for episode in _batch_section_get(section, "episode"):
                episode_guid = _convert_to_plex_guid(episode.attrib['guid'], "episode")
                if episode_guid == "":
                    episode_guid = episode.attrib['guid']

                episode_guid_rating_keys = cache['EPISODE_GUID_RATING_KEY_MAPPING'].get(episode_guid, [])
                episode_guid_rating_keys.append(int(episode.attrib['ratingKey']))
                cache['EPISODE_GUID_RATING_KEY_MAPPING'][episode_guid] = episode_guid_rating_keys

                guid: plexapi.media.Guid
                for guid in _get_guids(episode):
                    guid_rating_keys = cache['EPISODE_GUID_RATING_KEY_MAPPING'].get(guid, [])
                    guid_rating_keys.append(int(episode.attrib['ratingKey']))
                    cache['EPISODE_GUID_RATING_KEY_MAPPING'][guid] = guid_rating_keys

    return


def _cast(func, value):
    if func == "date_string":
        if isinstance(value, datetime.datetime):
            return value.strftime(DATETIME_FORMAT)
        else:
            return datetime.datetime(
                year=1000, month=1, day=1, hour=0, minute=0, second=0, microsecond=0).strftime(DATETIME_FORMAT)

    if value is None:
        return func()

    if func == str:
        return str(value)

    if not isinstance(value, func):
        raise ValueError(value)

    return value


def _get_username(user):
    username = _cast(str, user.username)
    # Username not set
    if username == "":
        username = _cast(str, user.email)
    # Plex Home or Managed Users don't require username/email
    if username == "":
        username = _cast(str, user.title)
    # Last fallback
    if username == "":
        username = _cast(str, user.id)
    return username


def _update_timeline(item, view_offset):
    try:
        item.updateTimeline(view_offset)
    except:
        logger.exception(f"Updating Item Timeline: {item.title}: {view_offset}")
    return


def _get_rating_keys(server, item_type, guid):
    rating_keys = []

    if item_type == "movie":
        rating_keys = cache['MOVIE_GUID_RATING_KEY_MAPPING'].get(guid, [])
    elif item_type == "show":
        rating_keys = cache['SHOW_GUID_RATING_KEY_MAPPING'].get(guid, [])
    elif item_type == "episode":
        rating_keys = cache['EPISODE_GUID_RATING_KEY_MAPPING'].get(guid, [])

    if len(rating_keys) > 0:
        return rating_keys

    # If we don't have the rating key in cache, don't search it unless cache is disabled
    if USE_CACHE:
        return rating_keys

    # If we don't have a rating key, try to get it from the library
    items = server.library.search(guid=guid)
    rating_keys = [int(item.ratingKey) for item in items]

    if item_type == "movie":
        cache['MOVIE_GUID_RATING_KEY_MAPPING'][guid] = rating_keys
    elif item_type == "show":
        cache['SHOW_GUID_RATING_KEY_MAPPING'][guid] = rating_keys
    elif item_type == "episode":
        cache['EPISODE_GUID_RATING_KEY_MAPPING'][guid] = rating_keys

    return rating_keys


def _set_movie_section_watched_history(server, movie_history):
    for movie_guid, movie_item_history in movie_history.items():
        rating_keys = _get_rating_keys(server, "movie", movie_guid)
        for rating_key in rating_keys:
            item: plexapi.video.Movie
            try:
                item = server.fetchItem(rating_key)
            except plexapi.exceptions.NotFound:
                logger.warning(f"Missing Movie: {movie_item_history['title']}: {movie_guid}")
                continue

            if not _cast(int, item.duration) > 0:
                logger.warning(f"Invalid Movie Duration: {item.title}: {item.duration}")
                continue

            if movie_item_history['viewCount'] > item.viewCount:
                for _ in range(movie_item_history['viewCount'] - item.viewCount):
                    logger.debug(f"Watching Movie: {item.title}")
                    item.markWatched()

            item_last_viewed_at = _cast("date_string", item.lastViewedAt)
            if datetime.datetime.strptime(item_last_viewed_at, DATETIME_FORMAT) <= datetime.datetime.strptime(
                    movie_item_history['lastViewedAt'], DATETIME_FORMAT):
                if movie_item_history['watched'] and not item.isWatched:
                    logger.debug(f"Watching Movie: {item.title}")
                    item.markWatched()
                if movie_item_history.get("viewPercent", 0.0) > 0.0:
                    view_offset = item.duration * movie_item_history['viewPercent']
                    logger.debug(f"Updating Movie Timeline: {item.title}: {view_offset}")
                    _update_timeline(item, view_offset)
                elif movie_item_history['viewOffset'] != 0:
                    view_offset = movie_item_history['viewOffset']
                    logger.debug(f"Updating Movie Timeline: {item.title}: {view_offset}")
                    _update_timeline(item, view_offset)
            else:
                logger.debug(f"Skipping Updating Watch Status of Movie: {item.title}")

            if movie_item_history['userRating'] != "":
                item_last_rated_at = _cast("date_string", item.lastRatedAt)
                if datetime.datetime.strptime(item_last_rated_at, DATETIME_FORMAT) <= datetime.datetime.strptime(
                        movie_item_history['lastRatedAt'], DATETIME_FORMAT):
                    logger.debug(f"Rating Movie: {item.title}: {movie_item_history['userRating']}")
                    item.rate(float(movie_item_history['userRating']))
                else:
                    logger.debug(f"Skipping Updating Rating of Episode: {item.title}")


def _set_show_section_watched_history(server, show_history):
    for show_guid, show_item_history in show_history.items():
        rating_keys = _get_rating_keys(server, "show", show_guid)
        for rating_key in rating_keys:
            item: plexapi.video.Show
            try:
                item = server.fetchItem(rating_key)
            except plexapi.exceptions.NotFound:
                logger.warning(f"Missing Show: {show_item_history['title']}: {show_guid}")
                continue

            item_last_viewed_at = _cast("date_string", item.lastViewedAt)
            if datetime.datetime.strptime(item_last_viewed_at, DATETIME_FORMAT) <= datetime.datetime.strptime(
                    show_item_history['lastViewedAt'], DATETIME_FORMAT):
                if show_item_history['watched'] and not item.isWatched:
                    logger.debug(f"Watching Show: {item.title}")
                    item.markWatched()
            else:
                logger.debug(f"Skipping Updating Watch Status of Show: {item.title}")

            if show_item_history['userRating'] != "":
                item_last_rated_at = _cast("date_string", item.lastRatedAt)
                if datetime.datetime.strptime(item_last_rated_at, DATETIME_FORMAT) <= datetime.datetime.strptime(
                        show_item_history['lastRatedAt'], DATETIME_FORMAT):
                    logger.debug(f"Rating Show: {item.title}: {show_item_history['userRating']}")
                    item.rate(float(show_item_history['userRating']))
                else:
                    logger.debug(f"Skipping Updating Rating of Show: {item.title}")

        for episode_guid, episode_item_history in show_item_history['episodes'].items():
            rating_keys = _get_rating_keys(server, "episode", episode_guid)
            for rating_key in rating_keys:
                item: plexapi.video.Episode
                try:
                    item = server.fetchItem(rating_key)
                except plexapi.exceptions.NotFound:
                    logger.warning(f"Missing Episode: {episode_item_history['title']}: {episode_guid}")
                    continue

                if not _cast(int, item.duration) > 0:
                    logger.warning(f"Invalid Episode Duration: {item.title}: {item.duration}")
                    continue

                if episode_item_history['viewCount'] > item.viewCount:
                    for _ in range(episode_item_history['viewCount'] - item.viewCount):
                        logger.debug(f"Watching Episode: {item.title}")
                        item.markWatched()

                item_last_viewed_at = _cast("date_string", item.lastViewedAt)
                if datetime.datetime.strptime(item_last_viewed_at, DATETIME_FORMAT) <= datetime.datetime.strptime(
                        episode_item_history['lastViewedAt'], DATETIME_FORMAT):
                    if episode_item_history['watched'] and not item.isWatched:
                        logger.debug(f"Watching Episode: {item.title}")
                        item.markWatched()
                    if episode_item_history.get("viewPercent", 0.0) > 0.0:
                        view_offset = item.duration * episode_item_history['viewPercent']
                        logger.debug(f"Updating Episode Timeline: {item.title}: {view_offset}")
                        _update_timeline(item, view_offset)
                    elif episode_item_history['viewOffset'] != 0:
                        view_offset = episode_item_history['viewOffset']
                        logger.debug(f"Updating Episode Timeline: {item.title}: {view_offset}")
                        _update_timeline(item, view_offset)
                else:
                    logger.debug(f"Skipping Updating Watch Status of Episode: {item.title}")

                if episode_item_history['userRating'] != "":
                    item_last_rated_at = _cast("date_string", item.lastRatedAt)
                    if datetime.datetime.strptime(item_last_rated_at, DATETIME_FORMAT) <= datetime.datetime.strptime(
                            episode_item_history['lastRatedAt'], DATETIME_FORMAT):
                        logger.debug(f"Rating Episode: {item.title}: {episode_item_history['userRating']}")
                        item.rate(float(episode_item_history['userRating']))
                    else:
                        logger.debug(f"Skipping Updating Rating of Episode: {item.title}")


def _set_user_server_watched_history(server, watched_history):
    _set_movie_section_watched_history(server, watched_history['movie'])
    _set_show_section_watched_history(server, watched_history['show'])


def main():
    _check_plexapi_version()

    _load_config()

    _setup_logger()

    _setup_session()

    _setup_cache()

    plex_server = plexapi.server.PlexServer(PLEX_URL, PLEX_TOKEN, session=session, timeout=60)
    logger.info(f"Plex Server: {plex_server.friendlyName}: {plex_server.version}")

    if USE_CACHE:
        logger.info("Building Cache of GUID to RatingKey")
        _cache_guid_rating_key_mappings(plex_server)

    with open(WATCHED_HISTORY, "r") as watched_history_file:
        watched_history = json.load(watched_history_file)

    logger.info(f"Starting Import")

    plex_account = plex_server.myPlexAccount()
    plex_users = plex_account.users()
    # Owner will be processed separately
    logger.info(f"Total Users: {len(plex_users) + 1}")

    if not (len(CHECK_USERS) > 0 and plex_account.username.lower() not in CHECK_USERS and
            plex_account.email.lower() not in CHECK_USERS and plex_account.title.lower() not in CHECK_USERS):
        username = _get_username(plex_account)

        logger.info(f"Processing Owner: {username}")

        if username in watched_history:
            user_history = watched_history[username]
            _set_user_server_watched_history(plex_server, user_history)
        else:
            logger.warning(f"Missing User from Watched History: {username}")

    for user_index, user in enumerate(plex_users):
        # TODO: Check for collisions
        if (len(CHECK_USERS) > 0 and user.username.lower() not in CHECK_USERS and
                user.email.lower() not in CHECK_USERS and user.title.lower() not in CHECK_USERS):
            continue

        username = _get_username(user)
        if username == "":
            logger.warning(f"Skipped User with Empty Username: {user}")
            continue

        logger.info(f"Processing User: {username}")

        if username not in watched_history:
            logger.warning(f"Missing User from Watched History: {username}")
            continue

        logger.info(f"Processing User: {username}")

        user_server_token = user.get_token(plex_server.machineIdentifier)

        try:
            _setup_session()
            user_server = plexapi.server.PlexServer(PLEX_URL, user_server_token, session=session, timeout=60)
        except plexapi.exceptions.Unauthorized:
            # This should only happen when no libraries are shared
            logger.warning(f"Skipped User with No Libraries Shared: {username}")
            continue

        user_history = watched_history[username]
        _set_user_server_watched_history(user_server, user_history)

    logger.info(f"Completed Import")


if __name__ == "__main__":
    main()
