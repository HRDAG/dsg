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

sudo ln -sf $HOME/projects/hrdag/btrsnap/bin/_find-repo-files /usr/local/bin/
sudo ln -sf $HOME/projects/hrdag/btrsnap/bin/_backend-test-fixture /usr/local/bin/
sudo ln -sf $HOME/projects/hrdag/btrsnap/bin/_init-btrsnap-repo /usr/local/bin/

# for testing
sudo mkdir -p /usr/local/share/btrsnap && \
	cd $_ && ln -sf $PATH_TO_BTRSNAP_DEV/data .

# done.
