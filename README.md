# San Francisco Tree Proximity Counter
Find trees within a radius of an address in San Francisco using the sf-trees [Datasette](https://san-francisco.datasettes.com/sf-trees/Street_Tree_List).
The radius is calculated as a measure of blocks in meters.

## Quickstart
Builds virtual env, installs requirements, and runs the code:
```
make build
```

For subsequent runs:
```
make start
```

## Command Line Arguments
For argument usage information run:

```
make help
```

## Make specifics

Creates the virtual environment venv.
```
make init
```

Runs Flake8 linting.
```
make lint
```

Installs requirements.
```
make setup
```

Runs the code.
```
make start
```

Runs the code with debug logging
```
make debug
```

Removes the virtual environment venv.
```
make clean
```