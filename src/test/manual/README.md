# Manual Tests

## Linux Files

- Only Linux scripts/apps in app, app/helpers and jrex/bin are executable
  - [x] 2024-10-19 6.0.0-M9 0e008d63 Linux
  - [x] 2024-09-05 6.0.0-M8 a8d1ccbd Linux
  - [x] 2024-09-01 6.0.0-M3 c919ca34 Linux
  - [x] 2024-10-14 5.3.2-M1 a8044ff1 Linux

## Repository Initialization

- Create a new repository
  - good success information is displayed
  - good success information is logged
  - [x] 2024-10-19 6.0.0-M9 0e008d63 Linux
  - [x] 2024-09-01 6.0.0-M3 c919ca34 Linux
  - [x] 2024-10-14 5.3.2-M1 a8044ff1 Linux
  - ... and Windows script waits for confirmation
  - [x] 2024-09-03 6.0.0-M5 ed880216 Windows
  - [x] 2024-09-01 6.0.0-M3 c919ca34 Linux
- Trying to create a new repository when repository is already initialized
  - good failure information is displayed
  - good failure information is logged
  - [x] 2024-10-19 6.0.0-M9 0e008d63 Linux
  - [x] 2024-09-05 6.0.0-M8 a8d1ccbd Linux
  - [x] 2024-10-14 5.3.2-M1 a8044ff1 Linux
  - ... and Windows script waits for confirmation
  - [x] 2024-09-03 6.0.0-M5 ed880216 Windows

## fsc backup

- Called without parameters
  - good failure information is displayed
  - good failure information is logged
  - doesn't wait for confirmation
  - [x] 2024-10-19 6.0.0-M9 0e008d63 Linux
  - [x] 2024-09-03 6.0.0-M5 ed880216 Windows
  - [x] 2024-10-19 5.3.2-M3 ed3fc276 Linux
  - [x] 2024-10-18 5.3.2-M2 7fb86473 Linux
  - [0] 2024-10-14 5.3.2-M1 a8044ff1 Linux

- Called with `bad /lib` (the source directory does not exist)
  - good failure information is displayed
  - good failure information is logged
  - doesn't wait for confirmation
  - [x] 2024-10-19 6.0.0-M9 0e008d63 Linux
  - [x] 2024-10-19 5.3.2-M3 ed3fc276 Linux
  - [x] 2024-10-18 5.3.2-M2 7fb86473 Linux

- Called with `lib /lib` (the target directory does not exist)
  - good failure information is displayed
  - good failure information is logged
  - doesn't wait for confirmation
  - [x] 2024-10-19 6.0.0-M9 0e008d63 Linux
  - [o] 2024-09-05 6.0.0-M8 a8d1ccbd Linux
  - [x] 2024-09-03 6.0.0-M6 dfaffe8d Windows
  - [o] 2024-09-03 6.0.0-M5 ed880216 Windows
  - [x] 2024-10-19 5.3.2-M3 ed3fc276 Linux
  - [o] 2024-10-18 5.3.2-M2 7fb86473 Linux

- Called with Linux: `lib /\!backup`, Windows: `lib /!backup` (is supposed to work)
  - good success information is displayed
  - good success information is logged
  - doesn't wait for confirmation
  - [x] 2024-10-19 6.0.0-M9 0e008d63 Linux
  - [x] 2024-09-03 6.0.0-M6 dfaffe8d Windows
  - [x] 2024-10-19 5.3.2-M3 ed3fc276 Linux

- An ongoing backup is interrupted
  - good information is displayed
  - good information is logged
  - doesn't wait for confirmation
  - [x] 2024-10-19 6.0.0-M9 0e008d63 Linux

## fsc list

- Called without parameters
  - good failure information is displayed
  - nothing is logged
  - doesn't wait for confirmation
  - [x] 2024-10-19 6.0.0-M9 0e008d63 Linux
  - [x] 2024-09-03 6.0.0-M7 0264ac9c Windows

- Called with `/does-not-exist`
  - good failure information is displayed
  - nothing is logged
  - doesn't wait for confirmation
  - [x] 2024-10-19 6.0.0-M9 0e008d63 Linux
  - [x] 2024-09-03 6.0.0-M7 0264ac9c Windows

- Called with `/backup/lib` (is supposed to work)
  - good success information is displayed
  - nothing is logged
  - doesn't wait for confirmation
  - [x] 2024-10-19 6.0.0-M9 0e008d63 Linux
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
  - [o] 2024-10-19 6.0.0-M9 0e008d63 Linux
  - ... and Windows script waits for confirmation
  - [x] 2024-09-03 6.0.0-M6 dfaffe8d Windows

## Logging and Command Line Output

Write operations write to logfile. Read-only operations only write to console. They should be silent except for the actual command output unless an error occurs.
