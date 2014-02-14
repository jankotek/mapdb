DBMaker and DB
==============

MapDB is set of loosely coupled components. Composing and wiring those together is hard,
so there are helper classes which do it for you. It uses maker (builder) pattern so most options
are quickly available via code assistant in IDE. Also it shields user from database internals.

[DBMaker](apidocs/org/mapdb/DBMaker.html) handles database configuration, creation and opening.
MapDB has several modes and configuration options. Most of those can be set using this class.

[DB](apidocs/org/mapdb/DB.html) represents opened database. It has methods for creating and accessing
collections and other databases. It also handles transactions with methods such as `commit()`,
`rollback()` and `close()`.


