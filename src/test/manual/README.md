# Manual Tests

## Linux Files

- Only Linux scripts/apps in app, app/helpers and jrex are executable
  - [x] 2024-09-05 6.0.0-M8 a8d1ccbd
  - [x] 2024-09-01 6.0.0-M3 c919ca34

## Repository Initialization

- Create a new repository
  - good success information is displayed
  - good success information is logged
  - Windows script waits for confirmation
  - [x] 2024-09-03 6.0.0-M5 ed880216 Windows
  - [x] 2024-09-01 6.0.0-M3 c919ca34 Linux
- Trying to create a new repository when repository is already initialized
  - good failure information is displayed
  - good failure information is logged
  - Windows script waits for confirmation
  - [x] 2024-09-05 6.0.0-M8 a8d1ccbd Linux
  - [x] 2024-09-03 6.0.0-M5 ed880216 Windows

## fsc backup

- Called without parameters
  - good failure information is displayed
  - good failure information is logged
  - doesn't wait for confirmation
  - [x] 2024-09-03 6.0.0-M5 ed880216 Windows

- Called with `jre /jre` (the target directory does not exist)
  - good failure information is displayed
  - good failure information is logged
  - doesn't wait for confirmation
  - [o] 2024-09-05 6.0.0-M8 a8d1ccbd Linux
  - [x] 2024-09-03 6.0.0-M6 dfaffe8d Windows
  - [o] 2024-09-03 6.0.0-M5 ed880216 Windows

- Called with `jre /!backup` (is supposed to work)
  - good success information is displayed
  - good success information is logged
  - doesn't wait for confirmation
  - [x] 2024-09-03 6.0.0-M6 dfaffe8d Windows

- An ongoing backup is interrupted
  - good information is displayed
  - good information is logged
  - doesn't wait for confirmation
  - [?] TODO

## fsc list

- Called without parameters
  - good failure information is displayed
  - nothing is logged
  - doesn't wait for confirmation
  - [x] 2024-09-03 6.0.0-M7 0264ac9c Windows

- Called with `/does-not-exist`
  - good failure information is displayed
  - nothing is logged
  - doesn't wait for confirmation
  - [x] 2024-09-03 6.0.0-M7 0264ac9c Windows

- Called with `/backup/jre` (is supposed to work)
  - good success information is displayed
  - nothing is logged
  - doesn't wait for confirmation
  - [x] 2024-09-03 6.0.0-M7 0264ac9c Windows

## stats

- Called without valid repository
  - good failure information is displayed
  - nothing is logged
  - Windows script waits for confirmation
  - [x] 2024-09-03 6.0.0-M6 dfaffe8d Windows

- Called with a valid repository
  - good success information is displayed
  - nothing is logged
  - Windows script waits for confirmation
  - [x] 2024-09-03 6.0.0-M6 dfaffe8d Windows

## Logging and Command Line Output

Write operations write to logfile. Read-only operations only write to console. They should be silent except for the actual command output unless an error occurs.
