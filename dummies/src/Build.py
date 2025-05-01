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
import pandas as pd
from Dummy import *
#}}}

# --- support methods --- {{{
def getargs():
    parser = argparse.ArgumentParser()
    parser.add_argument("--writedir", default="output/test_states")
    args = parser.parse_args()
    assert Path(args.writedir).exists()
    return args


def setuplogging(logfile):
    logger.add(logfile,
               colorize=True,
               format="<green>{time:YYYY-MM-DD⋅at⋅HH:mm:ss}</green>⋅<level>{message}</level>",
               level="INFO")
    return 1


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
    assert make_dummy(fname="dummy.txt", dirname=writedir)
    assert make_dummy_change(fname="dummy.txt", dirname=writedir)
    assert make_dummy_data(fname="dummy.parquet", dirname=writedir)
    assert make_dummy_data_change(fname="dummy.parquet", dirname=writedir)
    return 1


@logger.catch
def addstate(writedir, nn, files):
    """Exploring a generic-ish way to create a dummy project with real or fake files."""
    return 1


@logger.catch
def changestate(writedir, nn, curr):
    """Exploring a generic-ish way to modify a dummy project with real or fake files.
    The idea is that {curr} represents the current state,
    which we will copy and the modify to create the 'work' and 'last' sync states.
    """
    check_writedir(dirname=curr)
    subprocess.call(['cp', '-r', curr, f'{writedir}/{nn}'])
    return 1
# }}}

# --- main --- {{{
if __name__ == '__main__':
    args = getargs()
    setuplogging(f"{args.writedir}/Build.log")
    assert easy_start(writedir=f"{args.writedir}/easy_start")
    assert easy_change(writedir=f"{args.writedir}/easy_change")
    logger.info('done')
# }}}

#done.
