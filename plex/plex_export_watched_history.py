#!/usr/bin/env python3
# -*- coding: utf-8 -*-


# python3 -m pip install --force -U --user PlexAPI

"""
Metadata to be handled:
* Audiobooks
* Playlists -- https://github.com/pkkid/python-plexapi/issues/551

"""

import copy
import json
import time
import logging
import collections
from urllib.parse import urlparse

import plexapi
import plexapi.base
import plexapi.video
import plexapi.myplex
import plexapi.server
import plexapi.library
import plexapi.exceptions

PLEX_URL = ""
PLEX_TOKEN = ""
WATCHED_HISTORY = ""
LOG_FILE = ""

BATCH_SIZE = 10000
PLEX_REQUESTS_SLEEP = 0
CHECK_USERS = [
]

LOG_FORMAT = \
    "[%(name)s][%(process)05d][%(asctime)s][%(levelname)-8s][%(funcName)-15s]" \
    " %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
LOG_LEVEL = logging.INFO

plexapi.server.TIMEOUT = 3600
plexapi.server.X_PLEX_CONTAINER_SIZE = 500
plexapi.base.DONT_RELOAD_FOR_KEYS.update({'guid', 'guids', 'userRating', 'viewCount', 'viewOffset'})

_SHOW_RATING_KEY_GUID_MAPPING = {}
_MOVIE_RATING_KEY_GUID_MAPPING = {}
_EPISODE_RATING_KEY_GUID_MAPPING = {}

logger = logging.getLogger("PlexWatchedHistoryExporter")

SHOW_HISTORY = {
    'guid': "",
    'title': "",
    'watched': False,
    'userRating': "",
    'episodes': collections.defaultdict(lambda: copy.deepcopy(EPISODE_HISTORY))
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


def _load_config():
    global PLEX_URL, PLEX_TOKEN, WATCHED_HISTORY, CHECK_USERS, LOG_FILE, LOG_LEVEL
    if PLEX_URL == "":
        PLEX_URL = _get_config_str("sync.src_url")
    if PLEX_TOKEN == "":
        PLEX_TOKEN = _get_config_str("sync.src_token")
    if WATCHED_HISTORY == "":
        WATCHED_HISTORY = _get_config_str("sync.watched_history")
    if len(CHECK_USERS) == 0:
        config_check_users = _get_config_str("sync.check_users").split(",")
        CHECK_USERS = [user.strip() for user in config_check_users if user]
    if LOG_FILE == "":
        LOG_FILE = _get_config_str("sync.export_log_file")
    debug = plexapi.utils.cast(bool, _get_config_str("sync.debug").lower())
    if debug:
        LOG_LEVEL = logging.DEBUG


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


def _get_guid(rating_key_guid_mapping, item):
    if item.ratingKey in rating_key_guid_mapping:
        item_guid = rating_key_guid_mapping[item.ratingKey]
    else:
        item_guid = item.guid
        rating_key_guid_mapping[item.ratingKey] = item_guid
    return item_guid


def _get_view_percent(offset, duration):
    return round(float(offset / duration), 2)


def _tv_item_iterator(plex_section, start, batch_size):
    libtype = "show"

    # Get shows that have been fully watched
    watched_kwargs = {'show.unwatchedLeaves': False}

    items = plex_section.search(
        libtype=libtype,
        container_start=start,
        maxresults=batch_size,
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
        container_start=start,
        maxresults=batch_size,
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
        container_start=start,
        maxresults=batch_size,
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


def _movie_item_iterator(plex_section, start, batch_size):
    libtype = "movie"
    watched_kwargs = {'movie.viewCount!=': 0}
    partially_watched_kwargs = {'movie.viewCount=': 0, 'movie.inProgress': True}

    items = plex_section.search(
        libtype=libtype,
        container_start=start,
        maxresults=batch_size,
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

    items = plex_section.search(
        libtype=libtype,
        container_start=start,
        maxresults=batch_size,
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


def _batch_get(plex_section, batch_size):
    start = 0

    while True:
        if start >= plex_section.totalSize:
            break

        if isinstance(plex_section, plexapi.library.ShowSection):
            yield from _tv_item_iterator(plex_section, start, batch_size)
        elif isinstance(plex_section, plexapi.library.MovieSection):
            yield from _movie_item_iterator(plex_section, start, batch_size)
        else:
            logger.warning(f"Skipping Un-processable Section: {plex_section.title} [{plex_section.type}]")
            return

        start = start + 1 + batch_size


def _get_movie_section_watched_history(section, movie_history):
    movies_watched_history = _batch_get(section, BATCH_SIZE)
    for movie in movies_watched_history:
        movie_guid = _get_guid(_MOVIE_RATING_KEY_GUID_MAPPING, movie)
        if urlparse(movie_guid).scheme != "plex":
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
            })


def _get_show_section_watched_history(section, show_history):
    shows_watched_history = _batch_get(section, BATCH_SIZE)
    for show in shows_watched_history:
        show_guid = _get_guid(_SHOW_RATING_KEY_GUID_MAPPING, show)
        if urlparse(show_guid).scheme != "plex":
            continue
        show_item_history = show_history[show_guid]
        if show.isWatched:
            logger.debug(f"Fully Watched Show: {show.title} [{show_guid}]")
            show_item_history.update({
                'guid': _cast(str, show_guid),
                'title': _cast(str, show.title),
                'watched': _cast(bool, show.isWatched),
                'userRating': _cast(str, show.userRating),
            })
            for episode in show.episodes(viewCount__gt=0):
                episode_guid = _get_guid(_EPISODE_RATING_KEY_GUID_MAPPING, episode)
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
            })
            for episode in show.episodes(viewCount__gt=0):
                episode_guid = _get_guid(_EPISODE_RATING_KEY_GUID_MAPPING, episode)
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
                })
            for episode in show.episodes(viewOffset__gt=0):
                episode_guid = _get_guid(_EPISODE_RATING_KEY_GUID_MAPPING, episode)
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
                })
        show_history[show_guid] = show_item_history


def _get_user_server_watched_history(server):
    show_history = collections.defaultdict(lambda: copy.deepcopy(SHOW_HISTORY))
    movie_history = collections.defaultdict(lambda: copy.deepcopy(MOVIE_HISTORY))
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
    _load_config()

    _setup_logger()

    plex_server = plexapi.server.PlexServer(PLEX_URL, PLEX_TOKEN, timeout=300)
    plex_account = plex_server.myPlexAccount()

    watched_history = {}

    logger.info(f"Starting Export")

    plex_users = plex_account.users()
    # Owner will be processed separately
    logger.info(f"Total Users: {len(plex_users) + 1}")

    if not (len(CHECK_USERS) > 0 and plex_account.username not in CHECK_USERS and
            plex_account.email not in CHECK_USERS and plex_account.title not in CHECK_USERS):
        username = _get_username(plex_account)

        logger.info(f"Processing Owner: {username}")

        user_history = _get_user_server_watched_history(plex_server)
        user_history['username'] = username

        watched_history[username] = user_history

    for user_index, user in enumerate(plex_users):
        # TODO: Check for collisions
        if (len(CHECK_USERS) > 0 and user.username not in CHECK_USERS and
                user.email not in CHECK_USERS and user.title not in CHECK_USERS):
            continue

        username = _get_username(user)
        if username == "":
            logger.warning(f"Skipped User with Empty Username: {user}")
            continue

        logger.info(f"Processing User: {username}")

        user_server_token = user.get_token(plex_server.machineIdentifier)

        try:
            user_server = plexapi.server.PlexServer(PLEX_URL, user_server_token, timeout=300)
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
