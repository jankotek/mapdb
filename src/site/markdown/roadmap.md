Roadmap
========
New (feature) requests will usually be added at the very end of the list. The priority is increased for
important and popular requests. Of course, patches are always welcome, but are not always applied as is.
See also Providing Patches TODO link.

Version 1.0: TODOs
-------------------

 * validate XTEA encryption, replace if too weak

 * validate SerializerPOJO

 * Static fields in `Serializer.*` needs work

 * `StoreAppend` needs better compaction

 * Javadoc

 * Examples

 * Stress tests

 * Performance benchmarks, find bottlenecks before 1.0 freeze

Future store ideas:
-------------

 * Volume support for multiple hard-drives [issue #103](https://github.com/jankotek/MapDB/issues/103)

 * Index-less store [issue #93](https://github.com/jankotek/MapDB/issues/93)

 * StoreDirect/WAL compaction requires global lock.

 * Remove SnapshotEngine and merge it down into Store, use `StoreDirect.MASK_DISCARD` bite

 * Incremental backups in StoreDirect/WAL using `StoreDirect.MASK_ARCHIVE` bite

 * Support for encryption provided by JVM (AES-256)

 * Support for Deflate encryption provided by JVM, with custom dictionary

Data structures
----------------

 * Huge int array [issue #100](https://github.com/jankotek/MapDB/issues/100)

 * Secondary index based on regular expressions

 * Secondary index based on field value (no function wrapper needed)

 * Secondary index based on rainbow table

 * BTreeMap does not collapse tree after removal, needs online compaction.


Derived projects
------------------

 * C# port

 * Redis Server API support [issue #92](https://github.com/jankotek/MapDB/issues/92)

 * Master-Slave replication and fail-over

 * Spring Framework binding

 * JMX binding

 * JTA binding


Documentation & Examples
-----------------

 * Raw partition support [issue #104](https://github.com/jankotek/MapDB/issues/104)


Not Planned
--------------

 * Anything which requires dependency outside of Java6

 * Network server interface (should be in separate project)

