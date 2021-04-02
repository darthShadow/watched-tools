#!/usr/bin/env python3
# -*- coding: utf-8 -*-


# python3 -m pip install --force -U --user PlexAPI


import json
import time
import logging
import requests

import plexapi
import plexapi.video
import plexapi.myplex
import plexapi.server
import plexapi.library
import plexapi.exceptions


PLEX_URL = ""
PLEX_TOKEN = ""
WATCHED_HISTORY = ""


BATCH_SIZE = 10000
PLEX_REQUESTS_SLEEP = 0
CHECK_USERS = [
]


LOG_FORMAT = \
    "[%(name)s][%(process)05d][%(asctime)s][%(levelname)-8s][%(funcName)-15s]" \
    " %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
LOG_LEVEL = logging.INFO
LOG_FILE = "plex-import-watched-history.log"


plexapi.server.TIMEOUT = 3600
plexapi.server.X_PLEX_CONTAINER_SIZE = 2500


_SHOW_GUID_RATING_KEY_MAPPING = {}
_MOVIE_GUID_RATING_KEY_MAPPING = {}
_EPISODE_GUID_RATING_KEY_MAPPING = {}


session = requests.Session()
logger = logging.getLogger("PlexWatchedHistoryImporter")


def _get_config_str(key):
    return plexapi.CONFIG.get(key, default="", cast=str).strip("'").strip('"').strip()


def _load_config():
    global PLEX_URL, PLEX_TOKEN, WATCHED_HISTORY, LOG_LEVEL, CHECK_USERS
    if PLEX_URL == "":
        PLEX_URL = _get_config_str("sync.dst_url")
    if PLEX_TOKEN == "":
        PLEX_TOKEN = _get_config_str("sync.dst_token")
    if WATCHED_HISTORY == "":
        WATCHED_HISTORY = _get_config_str("sync.watched_history")
    if len(CHECK_USERS) == 0:
        config_check_users = _get_config_str("sync.check_users").split(",")
        CHECK_USERS = [user.strip() for user in config_check_users if user]
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


def _get_rating_keys(server, rating_key_guid_mapping, guid):
    if guid not in rating_key_guid_mapping:
        items = server.library.search(guid=guid)
        rating_key_guid_mapping[guid] = [item.ratingKey for item in items]

    return rating_key_guid_mapping[guid]


def _set_movie_section_watched_history(server, movie_history):
    for movie_guid, movie_item_history in movie_history.items():
        rating_keys = _get_rating_keys(server, _MOVIE_GUID_RATING_KEY_MAPPING, movie_guid)
        for rating_key in rating_keys:
            item = server.fetchItem(rating_key)
            if movie_item_history['watched'] and not item.isWatched:
                logger.debug(f"Watching Movie: {item.title}")
                item.markWatched()
            if movie_item_history['viewCount'] > item.viewCount:
                for _ in range(movie_item_history['viewCount'] - item.viewCount):
                    logger.debug(f"Watching Movie: {item.title}")
                    item.markWatched()
            if movie_item_history['viewOffset'] != 0:
                logger.debug(f"Updating Movie Timeline: {item.title}: {movie_item_history['viewOffset']}")
                item.updateTimeline(movie_item_history['viewOffset'])
            if movie_item_history['userRating'] != "":
                logger.debug(f"Rating Movie: {item.title}: {movie_item_history['userRating']}")
                item.rate(movie_item_history['userRating'])


def _set_show_section_watched_history(server, show_history):
    for show_guid, show_item_history in show_history.items():
        rating_keys = _get_rating_keys(server, _SHOW_GUID_RATING_KEY_MAPPING, show_guid)
        for rating_key in rating_keys:
            item = server.fetchItem(rating_key)
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
                if episode_item_history['watched'] and not item.isWatched:
                    logger.debug(f"Watching Episode: {item.title}")
                    item.markWatched()
                if episode_item_history['viewCount'] > item.viewCount:
                    for _ in range(episode_item_history['viewCount'] - item.viewCount):
                        logger.debug(f"Watching Episode: {item.title}")
                        item.markWatched()
                if episode_item_history['viewOffset'] != 0:
                    logger.debug(f"Updating Episode Timeline: {item.title}: {episode_item_history['viewOffset']}")
                    item.updateTimeline(episode_item_history['viewOffset'])
                if episode_item_history['userRating'] != "":
                    logger.debug(f"Rating Episode: {item.title}: {episode_item_history['userRating']}")
                    item.rate(episode_item_history['userRating'])


def _set_user_server_watched_history(server, watched_history):
    _set_movie_section_watched_history(server, watched_history['movie'])
    _set_show_section_watched_history(server, watched_history['show'])


def main():
    _load_config()

    _setup_logger()

    plex_server = plexapi.server.PlexServer(PLEX_URL, PLEX_TOKEN, session=session, timeout=300)
    plex_account = plex_server.myPlexAccount()

    with open(WATCHED_HISTORY, "r") as watched_history_file:
        watched_history = json.load(watched_history_file)

    logger.info(f"Starting Import")

    plex_users = plex_account.users()
    logger.info(f"Total Users: {len(plex_users)}")

    for user_index, user in enumerate(plex_users):
        if len(CHECK_USERS) > 0 and user.username not in CHECK_USERS:
            continue
        if user.username not in watched_history:
            logger.warning(f"Missing User from Watched History: {user.username}")
            continue
        logger.info(f"Processing User: {user.username}")
        user_server_token = user.get_token(plex_server.machineIdentifier)
        user_server = plexapi.server.PlexServer(PLEX_URL, user_server_token, timeout=300)
        user_history = watched_history[user.username]
        _set_user_server_watched_history(user_server, user_history)

    logger.info(f"Completed Import")


if __name__ == "__main__":
    main()
