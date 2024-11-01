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
  snap_p=$((snap_i-1))
  s_p="s${snap_p}"

  if ((! snap_i >= 2)); then
    echo "snap not in range: ${snap_i}"
    exit 1
  fi

  run "cd ${btrpath}"

  if [[ -d "$btrpath/$s_i" ]]; then
    echo "$btrpath/$s_i exists, failing"
    exit 1
  fi

  run "btrfs subvolume snapshot $btrpath/${s_p} $btrpath/${s_i}"

  opts="--archive --delete"
  run "rsync $opts ${snappath}/${s_i}/ ${btrpath}/${s_i}"
}


#--- main -----

if [[ "$EUID" != 0 ]]; then
  echo "must be run as root"
  exit 1
fi
if [[ ! -d "$snappath" ]]; then
  echo "no snap: $snappath, failing"
  exit 1
fi

run "btrfs device stats $btrroot"   # test if btrroot is btrfs
run "cd $btrpath"

run "btrfs subvolume create $btrpath/s1"
run "rsync -a $snappath/s1/ $btrpath/s1"
run "chknew s1" && echo "s1 diff'd ok"

# TODO: if someone added a dir to snappath with a space name in the name
# this would break! yuck. I don't think that happens, but beware.
allsnaps=$(ls "${snappath}" | \
  grep ^s | \
  grep -v s1$ | \
  awk '{ print substr( $0, 2, length($0)-1) }' | \
  sort --version-sort )

if [[ -z "${allsnaps// }" ]]; then
  i="1"   # if s1 is the only snap, we have to set i for the HEAD symlink
else
  for i in $allsnaps
  do
    echo $i
    onesnap $i
  done
fi
run "ln -sf $btrpath/s${i} $btrpath/HEAD"
run "chknew HEAD/" && echo "HEAD diff'd ok"

alltags=$(ls "${snappath}" | grep ^v)
for t in $alltags
do
  tagged=$(readlink ${snappath}/$t)
  run "ln -sf ${tagged} $btrpath/$t"
done


# done
