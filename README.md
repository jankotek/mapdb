<img src="https://raw.githubusercontent.com/jankotek/mapdb-site/master/art/rocket-small.png" width=90 height=90 align="left"/>

MapDB: database engine 
=======================
[![Build Status](https://travis-ci.org/jankotek/mapdb.svg?branch=master)](https://travis-ci.org/jankotek/mapdb)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.mapdb/mapdb/badge.svg)](https://search.maven.org/#search%7Cga%7C1%7Cg%3A%22org.mapdb%22%20AND%20a%3Amapdb)
[![Join the chat at https://gitter.im/jankotek/mapdb](https://badges.gitter.im/jankotek/mapdb.svg)](https://gitter.im/jankotek/mapdb?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)


MapDB combines embedded database engine and Java collections.
It is free under Apache 2 license. MapDB is flexible and can be used in many roles:

* Drop-in replacement for Maps, Lists, Queues and other collections.
* Off-heap collections not affected by Garbage Collector
* Multilevel cache with expiration and disk overflow.
* RDBMs replacement with  transactions, MVCC, incremental backups etc…
* Local data processing and filtering. MapDB has utilities to process huge quantities of data in reasonable time.

Hello world
-------------------

TODO Maven or JAR

TODO hello world

Support
------------

More [details](http://www.mapdb.org/support/).

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
