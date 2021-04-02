# plex-watched-tools
Plex Watched Tools

### Usage:

* Install PlexAPI: `python3 -m pip install --force -U --user PlexAPI` (Minimum supported python version is 3.8)

* Copy the included `sample.ini` to `sync.ini` and update the following variables:
    * `src_url`
    * `src_token`
    * `dst_url`
    * `dst_token`
    * `check_users` (If you want to export/import only specific users)

* Example value of `check_users`: `"abc,xyz,def"` (These must be the usernames of the required users)

* Export Watched History for Server:
    `PLEXAPI_CONFIG_PATH="<path_to_sync.ini>" python3 plex_export_watched_history.py`

* Export Watched History for Server:
    `PLEXAPI_CONFIG_PATH="<path_to_sync.ini>" python3 plex_import_watched_history.py`

### Debugging:

* Set `debug` to `true` in `sync.ini`

* Generated log files:
    * plex-export-watched-history.log
    * plex-import-watched-history.log
