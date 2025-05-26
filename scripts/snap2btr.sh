#!/bin/bash
#
# Author: PB
# Maintainer: PB
# Original date: 2024.10.11
# License: (c) HRDAG, GPL-2 or newer
#
# `snap2btr.sh` migrates a [snap](https://github.com/HRDAG/snap) repository to
# a [btrsnap](https://github.com/HRDAG/btrsnap) repository.
#
# To use the script, create a new btrsnap dir with a snap repo name. E.g., for
# country XX, `/var/repos/btrsnap/XX` matches `/var/repos/snap/XX`. `cd` to the
# new dir.
#
# With the script on the path, call the script with the name of the repo:
# `snap2btr.sh XX`.
#
#------
# btrsnap/scripts/snap2btr.sh

reponame="$1"
snappath="/eleanor/var/repos/snap/${reponame}"
btrroot="/var/repos/btrsnap"
btrpath="${btrroot}/${reponame}"

function run() {
  cmd_output=$(eval "$1")
  return_value=$?
  if [[ $return_value != 0 ]]; then
    echo "Command $1 failed: $return_value"
    exit $return_value
  fi
  return $return_value
}

function chknew() {
  # NB: we can't diff check symlinks bc if they're dangling, diff returns
  # error. but replicating a dangling symlink isn't an error, it's just
  # migrating what's there. so we don't check symlink references.
  # We _could_ add a check on all symlinks with a complicated find, probably.
  # Symlinks are important and worth checking.
  #
  # We don't care much about the contents of the .svn or .snap dirs. The
  # .snap/push.log is important tho bc it has all the commit and tag messages.
  #
  opts="--brief --recursive --exclude '.svn' --exclude '.snap' --no-dereference"
  run "diff $opts ${snappath}/$1 ${btrpath}/$1"
}

function onesnap() {
  snap_i=$1
  s_i="s${snap_i}"
  snap_p=$((snap_i - 1))
  s_p="s${snap_p}"

  if ! ((snap_i >= 2)); then
    echo "snap not in range: ${snap_i}"
    exit 1
  fi

  run "cd ${btrpath}"

  if [[ -d "$btrpath/$s_i" ]]; then
    echo "$btrpath/$s_i exists, failing"
    exit 1
  fi

  echo "  Creating btrfs snapshot from ${s_p} to ${s_i}"
  run "btrfs subvolume snapshot $btrpath/${s_p} $btrpath/${s_i}"

  echo "  Syncing changes from snap/${s_i} to btrsnap/${s_i}"
  opts="--archive --delete"
  run "rsync $opts ${snappath}/${s_i}/ ${btrpath}/${s_i}"
  run "sudo chown -R pball:${reponame} $btrpath/${s_i}"
}

#--- main -----

echo "Starting snap2btr migration for repo: ${reponame}"
echo "Source: ${snappath}"
echo "Destination: ${btrpath}"
echo

if [[ "$EUID" != 0 ]]; then
  echo "must be run as root"
  exit 1
fi
if [[ ! -d "$snappath" ]]; then
  echo "no snap: $snappath, failing"
  exit 1
fi

run "btrfs device stats $btrroot" # test if btrroot is btrfs
run "cd $btrpath"

# Create group if it doesn't exist
if ! getent group "${reponame}" > /dev/null 2>&1; then
  echo "Creating group ${reponame}..."
  run "sudo groupadd ${reponame}"
fi

echo "Creating initial snapshot s1..."
run "btrfs subvolume create $btrpath/s1"
echo "Copying s1 data from snap to btrsnap..."
run "rsync -a $snappath/s1/ $btrpath/s1"
run "sudo chown -R pball:${reponame} $btrpath/s1"
run "chknew s1" && echo "s1 diff'd ok"

# TODO: if someone added a dir to snappath with a space name in the name
# this would break! yuck. I don't think that happens, but beware.
allsnaps=$(ls "${snappath}" |
  grep ^s |
  grep -v s1$ |
  awk '{ print substr( $0, 2, length($0)-1) }' |
  sort --version-sort)

if [[ -z "${allsnaps// /}" ]]; then
  echo "No additional snapshots found beyond s1"
  i="1" # if s1 is the only snap, we have to set i for the HEAD symlink
else
  echo "Found $(echo "$allsnaps" | wc -w) additional snapshots to process"
  for i in $allsnaps; do
    echo "Processing snapshot s$i..."
    onesnap "$i"
    echo "Snapshot s$i completed"
  done
fi
echo "Creating HEAD symlink to s${i}..."
# Remove existing HEAD if it exists
if [[ -L "$btrpath/HEAD" ]]; then
  run "rm $btrpath/HEAD"
fi
run "ln -sf s${i} $btrpath/HEAD"
# Verify HEAD points to correct target
if [[ "$(readlink $btrpath/HEAD)" == "s${i}" ]]; then
  echo "HEAD symlink verified: points to s${i}"
else
  echo "ERROR: HEAD symlink incorrect!"
  exit 1
fi

alltags=$(ls "${snappath}" | grep ^v)
if [[ -n "$alltags" ]]; then
  echo "Creating version tags..."
  for t in $alltags; do
    tagged=$(readlink ${snappath}/$t)
    echo "  Creating tag $t -> ${tagged}"
    run "ln -sf ${tagged} $btrpath/$t"
  done
else
  echo "No version tags found"
fi

echo
echo "Migration completed successfully!"
# done
