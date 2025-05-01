#!/usr/bin/env python3
# vim: set ts=4 sts=0 sw=4 si fenc=utf-8 et:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:
# Authors:     BP
# Maintainers: BP
# Copyright:   2025, HRDAG, GPL v2 or later
# =========================================

# ---- dependencies {{{
from pathlib import Path
from sys import stdout
import subprocess
from loguru import logger
import pandas as pd
#}}}

checked_paths = []

# --- support methods --- {{{
def setup_logging(logfile):
    logger.add(logfile,
               colorize=True,
               format="<green>{time:YYYY-MM-DD⋅at⋅HH:mm:ss}</green>⋅<level>{message}</level>",
               level="INFO")
    return 1


@logger.catch
def check_writedir(dirname, makeifnot=True):
    if dirname in checked_paths: return 1
    logger.info(f'checking path {dirname} exists.')
    if not Path(dirname).exists():
        if not makeifnot:
            logger.info(f'Path {dirname} not found and was not written.')
            sys.exit()
        try: subprocess.call(['mkdir', dirname])
        except:
            logger.info(f'Path {dirname} not found and could not be written.')
            sys.exit(f'Please make sure {dirname} exists and then try building \
                    the test directories again.')
    checked_paths.append(dirname)
    return 1


@logger.catch
def check_path(expected_path, exists_ok, msg):
    if exists_ok: assert expected_path.exists(), msg
    else: assert not expected_path.exists(), msg
    return 1


@logger.catch
def make_dummy(fname, dirname):
    """checks that:
    - {fname} does NOT already exist in {dirname}.
    - {dirname} is a valid path BEFORE trying to create {fname}.
    - {fname} was successfully created inside {dirname}."""
    assert check_writedir(dirname=dirname)
    path = Path(f"{dirname}/{fname}")
    assert check_path(
        expected_path=path,
        exists_ok=False,
        msg=f"Expecting to *create* file {Path(fname).name} in directory {dirname}, \
                but file already exists.")
    cmd = ["touch", path]
    try: subprocess.call(cmd)
    except: return f"There was an unhandled error creating file {Path(fname).name} \
            in directory {dirname}."
    return path.exists()


@logger.catch
def make_dummy_change(fname, dirname):
    """checks that:
    - {fname} DOES already exist in {dirname}.
    - {dirname} is a valid path BEFORE trying to modify {fname}.
    - {fname} was successfully modified."""
    assert check_writedir(dirname=dirname)
    path = Path(f"{dirname}/{fname}")
    assert check_path(
        expected_path=path,
        exists_ok=True,
        msg=f"Expecting to *modify* file {Path(fname).name} in directory {dirname}, \
                but file does not exist.")
    cmd = ["touch", path]
    try: subprocess.call(cmd)
    except: return f"There was an unhandled error modifying file \
            {Path(fname).name} in directory {dirname}."
    return path.exists()


@logger.catch
def make_dummy_data(fname, dirname):
    """ASSUMES that the data should be exported as a parquet file.
    checks that:
    - {fname} does NOT already exist in {dirname}.
    - {dirname} is a valid path BEFORE trying to create {fname}.
    - {fname} was successfully created inside {dirname}."""
    assert check_writedir(dirname=dirname)
    if "." in fname: assert ".parquet" == fname[-8:], f"\
    Expecting to create a parquet data file, but {Path(fname).name} refers to another type."
    path = Path(f"{dirname}/{fname}")
    assert check_path(
        expected_path=path,
        exists_ok=False,
        msg=f"Expecting to *create* file {Path(fname).name} in directory {dirname},\
                but file already exists.")
    data = [{
        "a": 1, "b": 2, "c": '3jh4bv5'}, {
        "a": 1145, "b": 2.345, "c": '23jb4k23j'}]
    df = pd.DataFrame(data)
    try: df.to_parquet(path)
    except: return f"There was an unhandled error creating file {Path(fname).name} \
            in directory {dirname}."
    return path.exists()


@logger.catch
def make_dummy_data_change(fname, dirname):
    """ASSUMES that the data should be exported as a parquet file.
    checks that:
    - {fname} DOES already exist in {dirname}.
    - {dirname} is a valid path BEFORE trying to modify {fname}.
    - {fname} was successfully modified."""
    assert check_writedir(dirname=dirname)
    if "." in fname: assert ".parquet" == fname[-8:], f"\
            Expecting to work on a parquet data file, but {Path(fname).name} refers to another type."
    path = Path(f"{dirname}/{fname}")
    assert check_path(
        expected_path=path,
        exists_ok=True,
        msg=f"Expecting to *modify* file {Path(fname).name} in directory {dirname}, \
                but file does not exist.")
    olddata = pd.read_parquet(path)
    newdata = pd.DataFrame([{
        "a": 8, "b": 5, "c": '1h2j34b5jh'}, {
        "a": 7893, "b": 2.248937, "c": '1j4325b'}])
    df = pd.concat([olddata, newdata])
    try: df.to_parquet(path)
    except: return f"There was an unhandled error modifying file {Path(fname).name} \
            in directory {dirname}."
    return path.exists()
# }}}

# --- main --- {{{
if __name__ == '__main__':
    setup_logging("output/Dummy.log")

    logger.info('test run of Dummy methods + check for write access.')
    assert make_dummy(fname="dummy.txt", dirname="output")
    assert make_dummy_change(fname="dummy.txt", dirname="output")
    assert make_dummy_data(fname="dummy.parquet", dirname="output")
    assert make_dummy_data_change(fname="dummy.parquet", dirname="output")

    logger.info('all base test directories were created and/or modified.')
    logger.info('done with baseline methods tests.')
# }}}

# done.
