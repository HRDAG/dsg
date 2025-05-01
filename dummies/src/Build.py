#!/usr/bin/env python3
# vim: set ts=4 sts=0 sw=4 si fenc=utf-8 et:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:
# Authors:     BP
# Maintainers: BP
# Copyright:   2024, HRDAG, GPL v2 or later
# =========================================

# ---- dependencies {{{
from pathlib import Path
from sys import stdout
import argparse
from loguru import logger
import subprocess
import yaml
import pandas as pd
from Dummy import *
#}}}

checked_paths = []

# --- support methods --- {{{
def getargs():
    parser = argparse.ArgumentParser()
    parser.add_argument("--writedir", default="output/test_states")
    parser.add_argument("--states", default="hand/states")
    args = parser.parse_args()
    assert Path(args.writedir).exists()
    assert Path(args.states).exists()
    return args


def setuplogging(logfile):
    logger.add(logfile,
               colorize=True,
               format="<green>{time:YYYY-MM-DD⋅at⋅HH:mm:ss}</green>⋅<level>{message}</level>",
               level="INFO")
    return 1


def readyaml(yamlfile):
    with open(yamlfile, 'r') as f:
        data = yaml.safe_load(f)
        f.close()
    return data


@logger.catch
def easy_start(writedir, nn='easy_start'):
    """Intended for testing first sync of a new project."""
    logger.info(f'Setting up {nn} directory with easy file names.')
    assert make_dummy(fname="dummy.txt", dirname=writedir)
    assert make_dummy_data(fname="dummy.parquet", dirname=writedir)
    return 1


@logger.catch
def easy_change(writedir, nn='easy_change'):
    """Intended for testing simple file additions/modifications."""
    logger.info(f'Setting up {nn} directory with easy file names.')
    assert make_dummy_change(fname="dummy.txt", dirname=writedir)
    assert make_dummy_data_change(fname="dummy.parquet", dirname=writedir)
    return 1


@logger.catch
def make_state(statedir, files):
    """Exploring a generic-ish way to create/modify a project state
    with real or fake files."""
    check_writedir(dirname=statedir)
    checked_paths.append(statedir)
    if type(files) is list:
        for f in files:
            if f[-4:] == '.txt': make_dummy(fname=f, dirname=statedir)
            if f[-8:] == '.parquet': make_dummy_data(fname=f, dirname=statedir)
    else:
        assert Path(files).exists(), f"Path `{files}` could not be found"
        subprocess.call(['cp', '-r', files, statedir])
    return 1
# }}}

# --- main --- {{{
if __name__ == '__main__':
    args = getargs()
    setuplogging(f"{args.writedir}/Build.log")

    logger.info(f'verifying basic state construction works for `{args.writedir}`')
    check_writedir(dirname=args.writedir)
    checked_paths.append(args.writedir)
    assert easy_start(writedir=args.writedir)
    assert easy_change(writedir=args.writedir)

    logger.info(f'begin test state construction using `{args.states}`')
    states = readyaml(yamlfile=args.states)
    for nn, info in states.items():
        logger.info(f'setting up test state `{nn}`')
        assert make_state(statedir=f"{info['writedir']}/{nn}", files=info['files'])

    logger.info('done')
# }}}

#done.
