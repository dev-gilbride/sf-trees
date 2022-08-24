# San Francisco Tree Proximity Counter
Find trees within a radius of an address in San Francisco using the sf-trees [Datasette](https://san-francisco.datasettes.com/sf-trees/Street_Tree_List).
The radius is calculated as a measure of blocks in meters.

## Run Project Make
```
make build
```
Builds virtual env, installs requirements, and runs the code.

```
make lint
```
Runs Flake8 linting.

```
make debug
```
Runs the code with debug logging

```
make clean
```
Removes virtual env.

## Command Line Arguments
For argument usage information run:

```
python main.py -h
```