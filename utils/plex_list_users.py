#!/usr/bin/env python3
# -*- coding: utf-8 -*-


# python3 -m pip install --force -U --user PlexAPI


import plexapi
import plexapi.server


PLEX_URL = ""
PLEX_TOKEN = ""


plexapi.server.TIMEOUT = 3600
plexapi.server.X_PLEX_CONTAINER_SIZE = 2500


def _get_config_str(key):
    return plexapi.CONFIG.get(key, default="", cast=str).strip("'").strip('"').strip()


def _load_config():
    global PLEX_URL, PLEX_TOKEN
    if PLEX_URL == "":
        PLEX_URL = _get_config_str("sync.src_url")
    if PLEX_TOKEN == "":
        PLEX_TOKEN = _get_config_str("sync.src_token")


def main():
    _load_config()

    plex_server = plexapi.server.PlexServer(PLEX_URL, PLEX_TOKEN, timeout=300)
    plex_account = plex_server.myPlexAccount()

    plex_users = plex_account.users()
    # Owner will be processed separately
    print(f"Total Users: {len(plex_users) + 1}")

    print(f"Owner: | Username:{plex_account.username} | E-Mail:{plex_account.email} |"
          f" Title:{plex_account.title} | ID:{plex_account.id}")

    for user in plex_users:
        print(f"User: | Username:{user.username} | E-Mail:{user.email} |"
              f" Title:{user.title} | ID:{user.id}")


if __name__ == "__main__":
    main()
