# vim: set ts=8 sts=0 sw=8 si fenc=utf-8 noet:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:
# Authors:     BP
# Maintainers: BP
# Copyright:   2025, HRDAG, GPL v2 or later
# =========================================

sync_dir := output/test_states
test_states := hand/states.yml

.PHONY: all clean build

all: build

clean:
	rm -rf output/*

build:
	-mkdir output
	python src/Dummy.py
	find output/* -maxdepth 0 ! -name "*.log" -print0 | xargs -0n1 rm -rf
	-mkdir $(sync_dir)
	python src/Build.py \
		--writedir=$(sync_dir) \
		--states=$(test_states)

# done.
