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
snappath="/var/repos/snap/${reponame}"
btrpath="/var/repos/btrsnap/${reponame}"


function run() {
  cmd_output=$(eval $1)
  return_value=$?
  if [[ $return_value != 0 ]]; then
    echo "Command $1 failed: $return_value"
    exit $return_value
  fi
  return $return_value
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

  run "btrfs subvolume snapshot ${s_p} ${s_i}"

  opts="--archive --delete"
  run "rsync $opts ${snappath}/${s_i}/ ${btrpath}/${s_i}"
  
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
  run "diff $opts ${snappath}/${s_i} ${btrpath}/${s_i}" 
  # TODO: prune .snap and .svn dirs
}


#--- main -----

if [[ "$EUID" != 0 ]]; then
  echo "must be run as root"
  exit 1
fi 
if [[ "$PWD" != "$btrpath" ]]; then
  echo "wrong starting point: must be in $btrpath"
  exit 1
fi
if [[ ! -d "$snappath" ]]; then
  echo "no snap: $snappath, failing" 
  exit 1
fi
run "btrfs device stats $btrpath"   # test if btrpath is btrfs 

run "btrfs subvolume create $btrpath/s1"
run "cp --recursive --preserve=all $snappath/s1 s1"

allsnaps=$(ls ${snappath} | \
  grep ^s | \
  grep -v s1$ | \
  awk '{ print substr( $0, 2, length($0)-1) }' | \
  sort --version-sort )

for i in $allsnaps
do
  echo $i
  onesnap $i
done
run "ln -sf s${i} HEAD"

alltags=$(ls ${snappath} | grep ^v)
for t in $alltags 
do
  tagged=$(readlink ${snappath}/$t)  
  run "ln -sf ${tagged} $t"
done


# done
