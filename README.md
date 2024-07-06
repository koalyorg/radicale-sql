# Radicale SQL storage plugin

A storage plugin for [Radicale](https://radicale.org) with some extra features.

Can automatically generate birthday calendars based on address books.

This is a fork from [Radicale SQL storage plugin](https://git.redxef.at/redxef/radicale-sql/src/branch/master).

## Configuration

Example

```ini
[storage]
type=radicale_sql
url=sqlite:///data.db
generate_birthday_calendars=False
```
