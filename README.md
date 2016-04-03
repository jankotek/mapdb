
MapDB combines embedded database engine and Java collections.
It is free under Apache 2 license. MapDB is flexible and can be used in many roles:

* Drop-in replacement for Maps, Lists, Queues and other collections.
* Off-heap collections not affected by Garbage Collector
* Multilevel cache with expiration and disk overflow.
* RDBMs replacement with  transactions, MVCC, incremental backups etc…
* Local data processing and filtering. MapDB has utilities to process huge quantities of data in reasonable time.

Hello world
-------------------

[![Join the chat at https://gitter.im/jankotek/mapdb](https://badges.gitter.im/jankotek/mapdb.svg)](https://gitter.im/jankotek/mapdb?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

TODO Maven or JAR

TODO hello world

Support
------------

For questions and general support there is:

 * [Reddit Forum](https://www.reddit.com/r/mapdb)
 
 * [Mail Group](https://groups.google.com/forum/#!forum/mapdb)
 
 * [Slack Chat](https://mapdb.slack.com/)

Issues (anything with stack-trace) go on [Github](https://github.com/jankotek/mapdb/issues). Pull requests are welcomed.

You can also contact author [directly](mailto:jan@kotek.net).
I work on MapDB full time, its development is sponsored by my consulting services.


Development
--------------------

MapDB is written in Kotlin. You will need Intellij Idea 15 Community Edition to edit it.

Use Maven to build MapDB: `mvn install`

You might experience problem with `mapdb-jcache-tck-test` module.
It expects ``mapdb-jcache`` module to be already installed in local maven repo.
Source code module dependency does not work. To run all tests use  command: `mvn install test`

MapDB comes with extensive unit tests, by default only tiny fraction is executed, so build finishes under 10 minutes.
Full test suite has over million test cases and runs several hours/days.
To run full test suite set `-Dmdbtest=1` property.
It is recommended to run tests in parallel: `-DthreadCount=16`. 
It is also possible to override temporary folder with `-Djava.io.tmpdir=/path` directive.

An example to run full acceptance tests:

```
mvn clean install test  -Dmdbtest=1 -DthreadCount=16 -Djava.io.tmpdir=/mnt/big
```
