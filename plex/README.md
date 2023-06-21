# plex-watched-tools
Plex Watched Tools.

Exports/Imports the following info for TV & Movie & Audiobook (using the Audnexus Agent) Libraries:
* Watched Status
* View Count
* User Rating

Pending Items:
* [ ] Playlists (https://github.com/pkkid/python-plexapi/issues/551)

### Requirements

* Plex with all libraries using the new TV & Movie agents (Only for Import. Export supports both Old & New Agents)
* PlexAPI == 4.7.2 (Install/Update via: `python3 -m pip install --force -U PlexAPI==4.7.2`)
* Diskcache == 5.3.0 (Install/Update via `python3 -m pip install --force -U diskcache==5.3.0`)
* Python >= 3.8

### Usage:

* Copy the included `sample.ini` to `sync.ini` and update the following variables:
    * `src_url`
    * `src_token`
    * `dst_url`
    * `dst_token`
    * `check_users` (If you want to export/import only specific users)
    * `watched_history` (If you want to specify a custom location/file)
    * `use_cache` (Only set it to `true` if you are exporting/importing more than a handful of users)
    * `cache_dir` (**OPTIONAL**)
    * `plex_sections` (**OPTIONAL**) (If you want to export/import only specific libraries)
    * `max_processes` (**OPTIONAL**) (If you want to increase the number of processes used for exporting/importing)

* Example value of `check_users`: `"abc,xyz,def"` (These must be the usernames of the required users. The matching is **case-insensitive**.)

* Export Watched History for Server:
    `PLEXAPI_CONFIG_PATH="<path_to_sync.ini>" python3 plex_export_watched_history.py`

* Import Watched History for Server:
    `PLEXAPI_CONFIG_PATH="<path_to_sync.ini>" python3 plex_import_watched_history.py`

### Debugging:

* Set `debug` to `true` in `sync.ini`

* Adjust the log file locations:
    * `export_log_file`
    * `import_log_file`

* Default log files:
    * plex-export-watched-history.log
    * plex-import-watched-history.log
