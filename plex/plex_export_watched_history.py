#!/usr/bin/env python3
# -*- coding: utf-8 -*-


"""
Metadata to be handled:
* Audiobooks
* Playlists -- https://github.com/pkkid/python-plexapi/issues/551

"""


import copy
import json
import time
import logging
import datetime
import tempfile
from typing import Iterator
from diskcache import Index
from urllib.parse import urlparse
from collections import defaultdict
from xml.etree.ElementTree import Element

import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

import plexapi
import plexapi.base
import plexapi.video
import plexapi.utils
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

plexapi.server.TIMEOUT = 60
plexapi.server.X_PLEX_CONTAINER_SIZE = 1000
plexapi.base.USER_DONT_RELOAD_FOR_KEYS.update({
    'guid', 'guids', 'duration', 'title', 'userRating', 'viewCount', 'viewOffset', 'lastViewedAt', 'lastRatedAt'})

cache = {}
session = requests.Session()
logger = logging.getLogger("PlexWatchedHistoryExporter")

SHOW_HISTORY = {
    'guid': "",
    'title': "",
    'watched': False,
    'userRating': "",
    'episodes': defaultdict(lambda: copy.deepcopy(EPISODE_HISTORY))
}
MOVIE_HISTORY = {
    'guid': "",
    'title': "",
    'watched': False,
    'viewCount': 0,
    'viewOffset': 0,
    'userRating': ""
}
EPISODE_HISTORY = {
    'guid': "",
    'title': "",
    'watched': False,
    'viewCount': 0,
    'viewOffset': 0,
    'userRating': ""
}


def _get_config_str(key):
    return plexapi.CONFIG.get(key, default="", cast=str).strip("'").strip('"').strip()


def _check_plexapi_version():
    if plexapi.VERSION != "4.11.2":
        print("Please install PlexAPI Version: 4.11.2")
        raise Exception(f"Unsupported PlexAPI Version: {plexapi.VERSION}")


def _load_config():
    global PLEX_URL, PLEX_TOKEN, WATCHED_HISTORY, CHECK_USERS, LOG_FILE, LOG_LEVEL, USE_CACHE, CACHE_DIR
    if PLEX_URL == "":
        PLEX_URL = _get_config_str("sync.src_url")
    if PLEX_TOKEN == "":
        PLEX_TOKEN = _get_config_str("sync.src_token")
    if WATCHED_HISTORY == "":
        WATCHED_HISTORY = _get_config_str("sync.watched_history")
    if len(CHECK_USERS) == 0:
        config_check_users = _get_config_str("sync.check_users").split(",")
        CHECK_USERS = [user.strip().lower() for user in config_check_users if user]
    if LOG_FILE == "":
        LOG_FILE = _get_config_str("sync.export_log_file")
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
        'SHOW_RATING_KEY_GUID_MAPPING': Index(f"{cache_dir}/show_rating_key_guid_mapping.cache"),
        'MOVIE_RATING_KEY_GUID_MAPPING': Index(f"{cache_dir}/movie_rating_key_guid_mapping.cache"),
        'EPISODE_RATING_KEY_GUID_MAPPING': Index(f"{cache_dir}/episode_rating_key_guid_mapping.cache"),
    }

    cache['SHOW_RATING_KEY_GUID_MAPPING'].clear()
    cache['MOVIE_RATING_KEY_GUID_MAPPING'].clear()
    cache['EPISODE_RATING_KEY_GUID_MAPPING'].clear()


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


def _cache_rating_key_guid_mappings(plex_server: plexapi.server.PlexServer):
    sections = plex_server.library.sections()

    for section in sections:
        if isinstance(section, plexapi.library.MovieSection):
            for movie in _batch_section_get(section, "movie"):
                movie_guid = _convert_to_plex_guid(movie.attrib['guid'], "movie")
                if movie_guid == "":
                    movie_guid = movie.attrib['guid']
                cache['MOVIE_RATING_KEY_GUID_MAPPING'][int(movie.attrib['ratingKey'])] = movie_guid

        elif isinstance(section, plexapi.library.ShowSection):
            for show in _batch_section_get(section, "show"):
                show_guid = _convert_to_plex_guid(show.attrib['guid'], "show")
                if show_guid == "":
                    show_guid = show.attrib['guid']
                cache['SHOW_RATING_KEY_GUID_MAPPING'][int(show.attrib['ratingKey'])] = show_guid

            for episode in _batch_section_get(section, "episode"):
                episode_guid = _convert_to_plex_guid(episode.attrib['guid'], "episode")
                if episode_guid == "":
                    episode_guid = episode.attrib['guid']
                cache['EPISODE_RATING_KEY_GUID_MAPPING'][int(episode.attrib['ratingKey'])] = episode_guid

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


def _get_guid(item_type, item):
    item_guid = None

    if item_type == "movie":
        item_guid = cache['MOVIE_RATING_KEY_GUID_MAPPING'].get(int(item.ratingKey))
    elif item_type == "show":
        item_guid = cache['SHOW_RATING_KEY_GUID_MAPPING'].get(int(item.ratingKey))
    elif item_type == "episode":
        item_guid = cache['EPISODE_RATING_KEY_GUID_MAPPING'].get(int(item.ratingKey))

    if item_guid is not None:
        return item_guid

    item_guid = _convert_to_plex_guid(item.guid, item.type)
    if item_guid == "":
        item_guid = item.guid

    if item_type == "movie":
        cache['MOVIE_RATING_KEY_GUID_MAPPING'][int(item.ratingKey)] = item_guid
    elif item_type == "show":
        cache['SHOW_RATING_KEY_GUID_MAPPING'][int(item.ratingKey)] = item_guid
    elif item_type == "episode":
        cache['EPISODE_RATING_KEY_GUID_MAPPING'][int(item.ratingKey)] = item_guid

    return item_guid


def _get_view_percent(offset, duration):
    return round(float(offset / duration), 2)


def _tv_item_iterator(plex_section):
    libtype = "show"

    # Get shows that have been fully watched
    watched_kwargs = {'show.unwatchedLeaves': False}

    items = plex_section.search(
        libtype=libtype,
        **watched_kwargs
    )

    for item in items:
        logger.debug(f"Fully Watched Show: {item.title}")
        item.reload(
            checkFiles=False, includeAllConcerts=False, includeBandwidths=False,
            includeChapters=False, includeChildren=False, includeConcerts=False,
            includeExternalMedia=False, includeExtras=False, includeFields='',
            includeGeolocation=False, includeLoudnessRamps=False, includeMarkers=False,
            includeOnDeck=False, includePopularLeaves=False, includePreferences=False,
            includeRelated=False, includeRelatedCount=0, includeReviews=False,
            includeStations=False)
        yield item

    # Get shows have have not been fully watched but have episodes have been fully watched
    # Searching by episode.viewCount instead of show.viewCount to handle shows with
    # episodes that were watched and then unwatched
    partially_watched_kwargs = {'show.unwatchedLeaves': True, 'episode.viewCount!=': 0}

    items = plex_section.search(
        libtype=libtype,
        **partially_watched_kwargs
    )

    for item in items:
        logger.debug(f"Partially Watched Show with Fully Watched Episodes: {item.title}")
        item.reload(
            checkFiles=False, includeAllConcerts=False, includeBandwidths=False,
            includeChapters=False, includeChildren=False, includeConcerts=False,
            includeExternalMedia=False, includeExtras=False, includeFields='',
            includeGeolocation=False, includeLoudnessRamps=False, includeMarkers=False,
            includeOnDeck=False, includePopularLeaves=False, includePreferences=False,
            includeRelated=False, includeRelatedCount=0, includeReviews=False,
            includeStations=False)
        yield item

    # Get shows have have not been fully watched and have no episodes that have been fully
    # watched but have episodes that are in-progress
    partially_watched_kwargs = {'show.unwatchedLeaves': True, 'show.viewCount=': 0,
                                'episode.inProgress': True}

    items = plex_section.search(
        libtype=libtype,
        **partially_watched_kwargs
    )

    for item in items:
        logger.debug(f"Partially Watched Show with Partially Watched Episodes: {item.title}")
        item.reload(
            checkFiles=False, includeAllConcerts=False, includeBandwidths=False,
            includeChapters=False, includeChildren=False, includeConcerts=False,
            includeExternalMedia=False, includeExtras=False, includeFields='',
            includeGeolocation=False, includeLoudnessRamps=False, includeMarkers=False,
            includeOnDeck=False, includePopularLeaves=False, includePreferences=False,
            includeRelated=False, includeRelatedCount=0, includeReviews=False,
            includeStations=False)
        yield item


def _movie_item_iterator(plex_section):
    libtype = "movie"
    watched_kwargs = {'movie.viewCount!=': 0}

    items = plex_section.search(
        libtype=libtype,
        **watched_kwargs
    )

    for item in items:
        item.reload(
            checkFiles=False, includeAllConcerts=False, includeBandwidths=False,
            includeChapters=False, includeChildren=False, includeConcerts=False,
            includeExternalMedia=False, includeExtras=False, includeFields='',
            includeGeolocation=False, includeLoudnessRamps=False, includeMarkers=False,
            includeOnDeck=False, includePopularLeaves=False, includePreferences=False,
            includeRelated=False, includeRelatedCount=0, includeReviews=False,
            includeStations=False)
        yield item

    partially_watched_kwargs = {'movie.viewCount=': 0, 'movie.inProgress': True}

    items = plex_section.search(
        libtype=libtype,
        **partially_watched_kwargs
    )

    for item in items:
        item.reload(
            checkFiles=False, includeAllConcerts=False, includeBandwidths=False,
            includeChapters=False, includeChildren=False, includeConcerts=False,
            includeExternalMedia=False, includeExtras=False, includeFields='',
            includeGeolocation=False, includeLoudnessRamps=False, includeMarkers=False,
            includeOnDeck=False, includePopularLeaves=False, includePreferences=False,
            includeRelated=False, includeRelatedCount=0, includeReviews=False,
            includeStations=False)
        yield item


def _batch_get(plex_section):
    if isinstance(plex_section, plexapi.library.ShowSection):
        yield from _tv_item_iterator(plex_section)
    elif isinstance(plex_section, plexapi.library.MovieSection):
        yield from _movie_item_iterator(plex_section)
    else:
        logger.warning(f"Skipping Un-processable Section: {plex_section.title} [{plex_section.type}]")
        return


def _get_movie_section_watched_history(section, movie_history):
    movies_watched_history = _batch_get(section)
    for movie in movies_watched_history:
        movie_guid = _get_guid("movie", movie)
        if urlparse(movie_guid).scheme != "plex":
            logger.warning(f"Skipping Un-Processable Movie: {movie.title}: {movie_guid}")
            continue
        movie_duration = _cast(int, movie.duration)
        if not movie_duration > 0:
            logger.warning(f"Invalid Movie Duration: {movie.title}: {movie.duration}")
            continue
        if movie.isWatched:
            logger.debug(f"Fully Watched Movie: {movie.title} [{movie_guid}]")
            movie_history[movie_guid].update({
                'guid': _cast(str, movie_guid),
                'title': _cast(str, movie.title),
                'watched': _cast(bool, movie.isWatched),
                'viewCount': _cast(int, movie.viewCount),
                'viewOffset': _cast(int, movie.viewOffset),
                'userRating': _cast(str, movie.userRating),
                'viewPercent': _get_view_percent(_cast(int, movie.viewOffset),
                                                 movie_duration),
                'lastRatedAt': _cast("date_string", movie.lastRatedAt),
                'lastViewedAt': _cast("date_string", movie.lastViewedAt),
            })
        else:
            logger.debug(f"Partially Watched Movie: {movie.title} [{movie_guid}]")
            existing_watched = movie_history[movie_guid]['watched']
            # Prefer fully watched over partially watched entries
            # TODO: Check for userRating & viewOffset too, however this shouldn't ever be
            #  different since Plex tracks the item via the GUID across libraries/sections
            if existing_watched:
                continue
            movie_history[movie_guid].update({
                'guid': _cast(str, movie_guid),
                'title': _cast(str, movie.title),
                'watched': _cast(bool, movie.isWatched),
                'viewCount': _cast(int, movie.viewCount),
                'viewOffset': _cast(int, movie.viewOffset),
                'userRating': _cast(str, movie.userRating),
                'viewPercent': _get_view_percent(_cast(int, movie.viewOffset),
                                                 movie_duration),
                'lastRatedAt': _cast("date_string", movie.lastRatedAt),
                'lastViewedAt': _cast("date_string", movie.lastViewedAt),
            })


def _get_show_section_watched_history(section, show_history):
    shows_watched_history = _batch_get(section)
    for show in shows_watched_history:
        show_guid = _get_guid("show", show)
        if urlparse(show_guid).scheme != "plex":
            logger.warning(f"Skipping Un-Processable Show: {show.title}: {show_guid}")
            continue
        show_item_history = show_history[show_guid]
        if show.isWatched:
            logger.debug(f"Fully Watched Show: {show.title} [{show_guid}]")
            show_item_history.update({
                'guid': _cast(str, show_guid),
                'title': _cast(str, show.title),
                'watched': _cast(bool, show.isWatched),
                'userRating': _cast(str, show.userRating),
                'lastRatedAt': _cast("date_string", show.lastRatedAt),
                'lastViewedAt': _cast("date_string", show.lastViewedAt),
            })
            for episode in show.episodes(viewCount__gt=0):
                episode_guid = _get_guid("episode", episode)
                if urlparse(episode_guid).scheme != "plex":
                    logger.warning(f"Skipping Un-Processable Episode: {show.title}: {episode.title}: {episode_guid}")
                    continue
                logger.debug(f"Fully Watched Episode: {episode.title} [{episode_guid}]")
                episode_duration = _cast(int, episode.duration)
                if not episode_duration > 0:
                    logger.warning(f"Invalid Episode Duration: {episode.title}: {episode.duration}")
                    continue
                show_item_history['episodes'][episode_guid].update({
                    'guid': _cast(str, episode_guid),
                    'title': _cast(str, episode.title),
                    'watched': _cast(bool, episode.isWatched),
                    'viewCount': _cast(int, episode.viewCount),
                    'viewOffset': _cast(int, episode.viewOffset),
                    'userRating': _cast(str, episode.userRating),
                    'viewPercent': _get_view_percent(_cast(int, episode.viewOffset),
                                                     episode_duration),
                    'lastRatedAt': _cast("date_string", episode.lastRatedAt),
                    'lastViewedAt': _cast("date_string", episode.lastViewedAt),
                })
        else:
            logger.debug(f"Partially Watched Show: {show.title} [{show_guid}]")
            # Prefer fully watched over partially watched entries
            # TODO: Check for userRating & viewOffset too, however this shouldn't ever be
            #  different since Plex tracks the item via the GUID across libraries/sections
            existing_watched = show_item_history['watched']
            if existing_watched:
                continue
            show_item_history.update({
                'guid': _cast(str, show_guid),
                'title': _cast(str, show.title),
                'watched': _cast(bool, show.isWatched),
                'userRating': _cast(str, show.userRating),
                'lastRatedAt': _cast("date_string", show.lastRatedAt),
                'lastViewedAt': _cast("date_string", show.lastViewedAt),
            })
            for episode in show.episodes(viewCount__gt=0):
                episode_guid = _get_guid("episode", episode)
                if urlparse(episode_guid).scheme != "plex":
                    logger.warning(f"Skipping Un-Processable Episode: {show.title}: {episode.title}: {episode_guid}")
                    continue
                logger.debug(f"Fully Watched Episode: {episode.title} [{episode_guid}]")
                episode_duration = _cast(int, episode.duration)
                if not episode_duration > 0:
                    logger.warning(f"Invalid Episode Duration: {episode.title}: {episode.duration}")
                    continue
                show_item_history['episodes'][episode_guid].update({
                    'guid': _cast(str, episode_guid),
                    'title': _cast(str, episode.title),
                    'watched': _cast(bool, episode.isWatched),
                    'viewCount': _cast(int, episode.viewCount),
                    'viewOffset': _cast(int, episode.viewOffset),
                    'userRating': _cast(str, episode.userRating),
                    'viewPercent': _get_view_percent(_cast(int, episode.viewOffset),
                                                     episode_duration),
                    'lastRatedAt': _cast("date_string", episode.lastRatedAt),
                    'lastViewedAt': _cast("date_string", episode.lastViewedAt),
                })
            for episode in show.episodes(viewOffset__gt=0):
                episode_guid = _get_guid("episode", episode)
                if urlparse(episode_guid).scheme != "plex":
                    logger.warning(f"Skipping Un-Processable Episode: {show.title}: {episode.title}: {episode_guid}")
                    continue
                logger.debug(f"Partially Watched Episode: {episode.title} [{episode_guid}]")
                episode_duration = _cast(int, episode.duration)
                if not episode_duration > 0:
                    logger.warning(f"Invalid Episode Duration: {episode.title}: {episode.duration}")
                    continue
                show_item_history['episodes'][episode_guid].update({
                    'guid': _cast(str, episode_guid),
                    'title': _cast(str, episode.title),
                    'watched': _cast(bool, episode.isWatched),
                    'viewCount': _cast(int, episode.viewCount),
                    'viewOffset': _cast(int, episode.viewOffset),
                    'userRating': _cast(str, episode.userRating),
                    'viewPercent': _get_view_percent(_cast(int, episode.viewOffset),
                                                     episode_duration),
                    'lastRatedAt': _cast("date_string", episode.lastRatedAt),
                    'lastViewedAt': _cast("date_string", episode.lastViewedAt),
                })
        show_history[show_guid] = show_item_history


def _get_user_server_watched_history(server):
    show_history = defaultdict(lambda: copy.deepcopy(SHOW_HISTORY))
    movie_history = defaultdict(lambda: copy.deepcopy(MOVIE_HISTORY))
    music_history = {}
    for section in server.library.sections():
        if section.type == "movie":
            _get_movie_section_watched_history(section, movie_history)
        elif section.type == "show":
            _get_show_section_watched_history(section, show_history)
        else:
            logger.warning(f"Skipping Un-processable Section: {section.title} [{section.type}]")

    user_history = {
        'show': show_history,
        'movie': movie_history,
        'music': music_history,
    }

    return user_history


def main():
    _check_plexapi_version()

    _load_config()

    _setup_logger()

    _setup_session()

    _setup_cache()

    plex_server = plexapi.server.PlexServer(PLEX_URL, PLEX_TOKEN, session=session, timeout=60)
    logger.info(f"Plex Server: {plex_server.friendlyName}: {plex_server.version}")

    if USE_CACHE:
        logger.info("Building Cache of Rating Key to GUID")
        _cache_rating_key_guid_mappings(plex_server)

    watched_history = {}

    logger.info(f"Starting Export")

    plex_account = plex_server.myPlexAccount()
    plex_users = plex_account.users()
    # Owner will be processed separately
    logger.info(f"Total Users: {len(plex_users) + 1}")

    if not (len(CHECK_USERS) > 0 and plex_account.username.lower() not in CHECK_USERS and
            plex_account.email.lower() not in CHECK_USERS and plex_account.title.lower() not in CHECK_USERS):
        username = _get_username(plex_account)

        logger.info(f"Processing Owner: {username}")

        user_history = _get_user_server_watched_history(plex_server)
        user_history['username'] = username

        watched_history[username] = user_history

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

        user_server_token = user.get_token(plex_server.machineIdentifier)

        try:
            _setup_session()
            user_server = plexapi.server.PlexServer(PLEX_URL, user_server_token, session=session, timeout=60)
        except plexapi.exceptions.Unauthorized:
            # This should only happen when no libraries are shared
            logger.warning(f"Skipped User with No Libraries Shared: {username}")
            continue

        user_history = _get_user_server_watched_history(user_server)
        user_history['username'] = username

        watched_history[username] = user_history

    with open(WATCHED_HISTORY, "w") as watched_history_file:
        json.dump(watched_history, watched_history_file, sort_keys=True, indent=4)

    logger.info(f"Completed Export")


if __name__ == "__main__":
    main()
