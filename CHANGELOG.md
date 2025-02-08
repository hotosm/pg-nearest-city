# Changelog

## 0.1.0 (2025-02-08)

## 0.0.0 (2025-02-08)

### Feat

- re-added usage with context manager
- added sync code generation with unasync
- init status and logger
- async wrapper
- auto init when used with context manager
- initialization checks
- added support for external db connections and for closing internal ones
- delete voronoi file after init to lower disk usage
- gzipped files to lower disk usage
- first commit, add stub project, license

### Fix

- added pre-generated sync files
- return on invalid table structure
- changed test dbconfig to match compose file
- moved check for init files existance when they're actually needed

### Refactor

- moved shared logic into base class
