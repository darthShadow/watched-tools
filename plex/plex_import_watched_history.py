#!/usr/bin/env python3
# -*- coding: utf-8 -*-


# python3 -m pip install --force -U --user PlexAPI


import json
import time
import logging
from typing import Iterator, Union
from collections import defaultdict

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

LOG_FORMAT = \
    "[%(name)s][%(process)05d][%(asctime)s][%(levelname)-8s][%(funcName)-15s]" \
    " %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
LOG_LEVEL = logging.INFO

plexapi.server.TIMEOUT = 60
plexapi.server.X_PLEX_CONTAINER_SIZE = 1000
plexapi.base.USER_DONT_RELOAD_FOR_KEYS.update({
    'guid', 'guids', 'duration', 'title', 'userRating', 'viewCount', 'viewOffset'})

_SHOW_GUID_RATING_KEY_MAPPING = defaultdict(list)
_MOVIE_GUID_RATING_KEY_MAPPING = defaultdict(list)
_EPISODE_GUID_RATING_KEY_MAPPING = defaultdict(list)

logger = logging.getLogger("PlexWatchedHistoryImporter")


def _get_config_str(key):
    return plexapi.CONFIG.get(key, default="", cast=str).strip("'").strip('"').strip()


def _check_plexapi_version():
    if plexapi.VERSION != "4.7.2":
        print("Please install PlexAPI Version: 4.7.2")
        raise Exception(f"Unsupported PlexAPI Version: {plexapi.VERSION}")


def _load_config():
    global PLEX_URL, PLEX_TOKEN, WATCHED_HISTORY, CHECK_USERS, LOG_FILE, LOG_LEVEL, USE_CACHE
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


def _setup_logger():
    logging.Formatter.converter = time.gmtime
    logging.raiseExceptions = False

    logger.setLevel(logging.DEBUG)
    logger.handlers = []
    logger.propagate = False

    detailed_formatter = logging.Formatter(fmt=LOG_FORMAT,
                                           datefmt=LOG_DATE_FORMAT)
    file_handler = logging.FileHandler(filename=LOG_FILE, mode="a+")
    file_handler.setFormatter(detailed_formatter)
    file_handler.setLevel(LOG_LEVEL)

    logger.addHandler(file_handler)


def _get_session():
    session = requests.Session()
    retry_strategy = Retry(
        total=10,
        backoff_factor=10,
        raise_on_status=True,
        allowed_methods=["GET"],
        status_forcelist=[429, 500, 502, 503, 504],
    )
    session_adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=4,
                                  pool_maxsize=4, pool_block=True)
    session.mount('http://', session_adapter)
    session.mount('https://', session_adapter)
    return session


# noinspection PyProtectedMember
def _section_item_iterator(plex_section: plexapi.library.LibrarySection, libtype: str) -> Iterator[Union[
        plexapi.video.Movie, plexapi.video.Show]]:
    key = f"/library/sections/{plex_section.key}/all?includeGuids=1&type={plexapi.utils.searchType(libtype)}"
    container_start = 0
    container_size = plexapi.X_PLEX_CONTAINER_SIZE
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


def _batch_section_get(plex_section: plexapi.library.LibrarySection, libtype: str) -> Iterator[Union[
        plexapi.video.Movie, plexapi.video.Show]]:
    yield from _section_item_iterator(plex_section, libtype)


def _get_guids(element):
    guids = []

    for child in element.children:
        if child.tag == plexapi.media.Guid.TAG:
            guid_id = child.attrib.get("id")
            if guid_id:
                guids.append(guid_id)

    return guids


def _cache_guid_rating_key_mappings(plex_server: plexapi.server.PlexServer):
    sections = plex_server.library.sections()

    for section in sections:
        if isinstance(section, plexapi.library.MovieSection):
            movie: plexapi.video.Movie
            for movie in _batch_section_get(section, "movie"):
                _MOVIE_GUID_RATING_KEY_MAPPING[movie.attrib['guid']].append(movie.attrib['ratingKey'])
                guid: plexapi.media.Guid
                for guid in _get_guids(movie):
                    _MOVIE_GUID_RATING_KEY_MAPPING[guid].append(movie.attrib['ratingKey'])

        elif isinstance(section, plexapi.library.ShowSection):
            show: plexapi.video.Show
            for show in _batch_section_get(section, "show"):
                _SHOW_GUID_RATING_KEY_MAPPING[show.attrib['guid']].append(show.attrib['ratingKey'])
                guid: plexapi.media.Guid
                for guid in _get_guids(show):
                    _SHOW_GUID_RATING_KEY_MAPPING[guid].append(show.attrib['ratingKey'])

            episode: plexapi.video.Episode
            for episode in _batch_section_get(section, "episode"):
                _EPISODE_GUID_RATING_KEY_MAPPING[episode.guid].append(episode.attrib['ratingKey'])
                guid: plexapi.media.Guid
                for guid in _get_guids(episode):
                    _EPISODE_GUID_RATING_KEY_MAPPING[guid].append(episode.attrib['ratingKey'])

    return


def _cast(func, value):
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


def _get_rating_keys(server, rating_key_guid_mapping, guid):
    if guid not in rating_key_guid_mapping:
        items = server.library.search(guid=guid)
        rating_key_guid_mapping[guid] = [item.ratingKey for item in items]

    return rating_key_guid_mapping[guid]


def _reload_item(item):
    item.reload(
        checkFiles=False, includeAllConcerts=False, includeBandwidths=False,
        includeChapters=False, includeChildren=False, includeConcerts=False,
        includeExternalMedia=False, includeExtras=False, includeFields='',
        includeGeolocation=False, includeLoudnessRamps=False, includeMarkers=False,
        includeOnDeck=False, includePopularLeaves=False, includePreferences=False,
        includeRelated=False, includeRelatedCount=0, includeReviews=False,
        includeStations=False)
    return


def _set_movie_section_watched_history(server, movie_history):
    for movie_guid, movie_item_history in movie_history.items():
        rating_keys = _get_rating_keys(server, _MOVIE_GUID_RATING_KEY_MAPPING, movie_guid)
        for rating_key in rating_keys:
            item = server.fetchItem(rating_key)
            # _reload_item(item)
            if not _cast(int, item.duration) > 0:
                logger.warning(f"Invalid Movie Duration: {item.title}: {item.duration}")
                continue
            if movie_item_history['watched'] and not item.isWatched:
                logger.debug(f"Watching Movie: {item.title}")
                item.markWatched()
            if movie_item_history['viewCount'] > item.viewCount:
                for _ in range(movie_item_history['viewCount'] - item.viewCount):
                    logger.debug(f"Watching Movie: {item.title}")
                    item.markWatched()
            if movie_item_history.get("viewPercent", 0.0) > 0.0:
                view_offset = item.duration * movie_item_history['viewPercent']
                logger.debug(f"Updating Movie Timeline: {item.title}: {view_offset}")
                item.updateTimeline(view_offset)
            elif movie_item_history['viewOffset'] != 0:
                view_offset = movie_item_history['viewOffset']
                logger.debug(f"Updating Movie Timeline: {item.title}: {view_offset}")
                item.updateTimeline(view_offset)
            if movie_item_history['userRating'] != "":
                logger.debug(f"Rating Movie: {item.title}: {movie_item_history['userRating']}")
                item.rate(movie_item_history['userRating'])


def _set_show_section_watched_history(server, show_history):
    for show_guid, show_item_history in show_history.items():
        rating_keys = _get_rating_keys(server, _SHOW_GUID_RATING_KEY_MAPPING, show_guid)
        for rating_key in rating_keys:
            item = server.fetchItem(rating_key)
            # _reload_item(item)
            if show_item_history['watched'] and not item.isWatched:
                logger.debug(f"Watching Show: {item.title}")
                item.markWatched()
            if show_item_history['userRating'] != "":
                logger.debug(f"Rating Show: {item.title}: {show_item_history['userRating']}")
                item.rate(show_item_history['userRating'])
        for episode_guid, episode_item_history in show_item_history['episodes'].items():
            rating_keys = _get_rating_keys(server, _EPISODE_GUID_RATING_KEY_MAPPING, episode_guid)
            for rating_key in rating_keys:
                item = server.fetchItem(rating_key)
                # _reload_item(item)
                if not _cast(int, item.duration) > 0:
                    logger.warning(f"Invalid Episode Duration: {item.title}: {item.duration}")
                    continue
                if episode_item_history['watched'] and not item.isWatched:
                    logger.debug(f"Watching Episode: {item.title}")
                    item.markWatched()
                if episode_item_history['viewCount'] > item.viewCount:
                    for _ in range(episode_item_history['viewCount'] - item.viewCount):
                        logger.debug(f"Watching Episode: {item.title}")
                        item.markWatched()
                if episode_item_history.get("viewPercent", 0.0) > 0.0:
                    view_offset = item.duration * episode_item_history['viewPercent']
                    logger.debug(f"Updating Episode Timeline: {item.title}: {view_offset}")
                    item.updateTimeline(view_offset)
                elif episode_item_history['viewOffset'] != 0:
                    view_offset = episode_item_history['viewOffset']
                    logger.debug(f"Updating Episode Timeline: {item.title}: {view_offset}")
                    item.updateTimeline(view_offset)
                if episode_item_history['userRating'] != "":
                    logger.debug(f"Rating Episode: {item.title}: {episode_item_history['userRating']}")
                    item.rate(episode_item_history['userRating'])


def _set_user_server_watched_history(server, watched_history):
    _set_movie_section_watched_history(server, watched_history['movie'])
    _set_show_section_watched_history(server, watched_history['show'])


def main():
    _check_plexapi_version()

    _load_config()

    _setup_logger()

    session = _get_session()
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

        user_history = watched_history[username]
        _set_user_server_watched_history(plex_server, user_history)

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
            session = _get_session()
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
