# Radicale SQL storage plugin

A storage plugin for [Radicale](https://radicale.org) with some extra features.

Can automatically generate birthday calendars based on address books.

## Configuration

Example

```ini
[storage]
type=radicale_sql
url=sqlite:///data.db
generate_birthday_calendars=False
```

## TODO

- ~~ensure all database operations run in one transaction~~
- implement caching
- write unit tests
- ~~write integration test~~
- extend integration test to include auto-generated collections
- run cleanup of `item_history` and `collection_state` tables
- integrate alembic
