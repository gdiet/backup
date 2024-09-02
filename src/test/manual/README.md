# Manual Tests

## Linux Files

- Linux scripts in app and app/helpers are executable
  - [x] 2024-09-01 6.0.0-M3 c919ca34

## Repository Initialization

- Linux: Create a new repository & good success information is displayed
  - [x] 2024-09-01 6.0.0-M3 c919ca34
- Linux: Trying to create a new repository when repository is already initialized & good failure information is displayed
  - [x] ???

## Plans for Logging and Command Line Output

Write operations should write to logfile. Read-only operations should only write to console. They should be silent except for the actual command output unless an error occurs.
