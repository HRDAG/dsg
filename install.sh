#!/bin/bash

# this is mostly to remember all the stuff we've done.
# at some point this could be wrapped into a setup.py

# make sure we're running in our own cwd:
if [[ ! -f install.sh ]] ; then
	echo "can only run from same directory as install.sh"
	exit 1
fi
# make sure user has root:
if [[ $(sudo -v) ]] ; then
	echo "you can only run this script if you have sudo privs"
	exit 1
fi

export PATH_TO_BTRSNAP_DEV=$(pwd)

sudo ln -sf $PATH_TO_BTRSNAP_DEV/data/ /usr/local/share/btrsnap/data/

# done.
