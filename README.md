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

<!-- done -->
