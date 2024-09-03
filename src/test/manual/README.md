# Manual Tests

## Linux Files

- Linux scripts in app and app/helpers are executable
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
  - [o] 2024-09-03 6.0.0-M5 ed880216 Windows


## Plans for Logging and Command Line Output

Write operations should write to logfile. Read-only operations should only write to console. They should be silent except for the actual command output unless an error occurs.
