#!/bin/bash

# this is mostly to remember all the stuff we've done.
# at some point this could be wrapped into a setup.py

# make sure we're running in our own cwd:
if [[ ! -f tests/install.sh ]] ; then
	echo "can only run from directory above tests/install.sh"
	exit 1
fi
# make sure user has root:
if [[ $(sudo -v) ]] ; then
	echo "you can only run this script if you have sudo privs"
	exit 1
fi

PATH_TO_BTRSNAP_DEV=$(pwd)
INSTALLED_TEST_DATA_PATH=/usr/local/share/btrsnap

if [[ ! -d $INSTALLED_TEST_DATA_PATH ]] ; then
  sudo mkdir -p $INSTALLED_TEST_DATA_PATH
fi

sudo ln -sf $HOME/projects/hrdag/btrsnap/bin/_find-repo-files /usr/local/bin/
sudo ln -sf $HOME/projects/hrdag/btrsnap/bin/_backend-test-fixture /usr/local/bin/
sudo ln -sf $HOME/projects/hrdag/btrsnap/bin/_init-btrsnap-repo /usr/local/bin/

# for testing
# probably should run btrsnap/bin/_stdize_timestamp on the data
cd $PATH_TO_BTRSNAP_DEV/data && $PATH_TO_BTRSNAP_DEV/bin/_stdize_timestamp

cd $INSTALLED_TEST_DATA_PATH && sudo rm -rf * && \
	sudo cp -a $PATH_TO_BTRSNAP_DEV/data test

# done.
