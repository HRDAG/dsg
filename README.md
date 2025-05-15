# dsg

This is HRDAG's **d**ata **s**ync **g**izmo. 

A data versioning system based on Scott's `snap`. But generalized across backends. This doesn't do anything yet, so all the actions is in the issues and in `tests/`. To run tests and see coverage: 
```bash
git/dsg$  pytest --cov=src/dsg tests/ --cov-report=term-missing
``` 

#### A few decisions
* we assume python >=3.13 
* we'll adopt pypoetry project structure conventions
* data objects to be shared will be pydantic classes for validation
* we strive for 100% test coverage with pytest 

#### Reviewing Integration Tests

To review the remote-local file integration tests, you can preserve the test directories by running:

```bash
# Set a recognizable name for the temporary test directory
PYTESTTMP=dsg-review-test 

# Create the directory and run the test with the environment variables
mkdir -p /tmp/$PYTESTTMP && PYTHONPATH=. KEEP_TEST_DIR=1 TMPDIR=/tmp/$PYTESTTMP pytest -v tests/test_manifest_integration.py::test_multiple_sync_states
```

This will run the integration test and preserve the test directories. You can then examine the local and remote repositories at:

```
/tmp/dsg-review-test/pytest-of-<username>/pytest-*/test_multiple_sync_states*/local/tmpx
/tmp/dsg-review-test/pytest-of-<username>/pytest-*/test_multiple_sync_states*/remote/tmpx
```

These directories represent the local and remote repositories with their respective file states, allowing you to manually review the file changes and manifests.

<!-- done -->