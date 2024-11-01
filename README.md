# btrsnap

A data versioning system based on `btrfs`, `rsync`, and some lightweight metadata management.

## installation

```bash
ln -sf $PATH_TO_BTRSNAP_DEV/data /usr/local/share/btrsnap/data
```

### file overview

* `_backend-test-fixture`: for testing, this copies data from `/usr/local/share/btrsnap/data` to the `btrsnap` repo. It can be extended to tweak the files for testing.

* `_find-repo-files --path MY/PATH`: this retrieves the list of files in a repo. It runs on the machine it's called on, but it returns a string. The point is that it can be called via ssh on a remote machine. This is useful to get the state of the remote repository.

* `_init_btrsnap_repo`: bash script to setup a repository

* `btrsnap.py` called w a host of options, see `--help`.

## testing

```
mkdir -p /usr/local/share/btrsnap && cd $_ \
    && ln -sf $BTRSNAP_ROOT/data .
```

## TODO

* `btrsnap {sync, resolve, clone, status, info, blame, tag, tags, version, help}`


<!-- done -->
