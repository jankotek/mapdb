Credits
=======

Most of MapDB was written by Jan Kotek. He started this project and pretty much runs the show.

There were many people who contributed ideas, bug fixes and improvements.
I am sorry if I forgot someone, please send me a line if you should be here.

### JDBM 1

MapDB is loosely based on older project called JDBM. First version was started around 2001 and was released in 2005
at [SourceForge](http://jdbm.sourceforge.net/). MapDB is complete rewrite, there is no code left from original
JDBM 1.0 in MapDB. But it heavily introduced our design and feature set.
MapDB keeps layout set in JDBM 1. Most importantly it is:

* Idea of passing around  *object instance with serializer* rather than *already serialized byte array*.
This is the very basic idea which defines MapDB today. It allows instance cache, great performance and
tight component integration.

* Engine (RecordManager) as abstract component
* Instance Cache as Engine (RecordManager) wrapper
* Index tree as separated class from Engine (RecordManager)

Credit goes to Cees de Groot and Alex Boisvert.

### JDBM 2 and 3
JDBM 1 stagnated since 2005, until it was restarted by Jan Kotek in 2009. JDBM 2 added Map interface and
basic serialization (`SerializerBase` in MapDB). JDBM 3 brought NIO updates, POJO serialization and many
performance improvements. MapDB does not contain any code from JDBM 2&3 except serialization.

There were many people who improved JDMB (and indirectly MapDB)
* Kevin Day sent many ideas and patches, most importantly delta compression in BTree and packed longs.
* Bryan Thompson worked on concurrency branch of JDBM 1.0, and helped to shape JDBM
* Thomas Mueller (H2 DB) for code reading and advices.


### Serialization

Serialization is the only code MapDB shares with JDBM 2 & 3. This  code is complex and
initially had lot of bugs. There were MANY people who submitted bug-fixes and I am sorry for
not listing them all. Most importantly:
* Nathan Sweet wrote Kryo serialization framework which inspired our POJO serializer. We also took Long Packer utils from Kryo framework.
* Roman Levenstein refactored original simple POJO serializer and greatly improved its performance.

### Collections

* Theoretical design of BTreeMap is based on [paper](http://www.cs.cornell.edu/courses/cs4411/2009sp/blink.pdf)
from Philip L. Lehman and S. Bing Yao.

* More practical aspects of BTreeMap implementation are based on [notes](http://www.doc.ic.ac.uk/~td202/)
and [demo application](http://www.doc.ic.ac.uk/~td202/btree/) from Thomas Dinsdale-Young.

* Java Collections are very complex to implement. We took unit tests from Google Collections which was great help.
Credit goes to Jared Levy, George van den Driessche and other Google Collections developers.

* Long(Concurrent)HashMap and some other classes were taken from Apache Harmony and refactored for our needs.

* BTreeMap uses some code  `ConcurrentSkipListMap` taken from Apache Harmony to implement all aspects of `ConcurrentNavigableMap`. Credit goes to  Doug Lea and others.

* Luc Peuvrier wrote some unit tests for `ConcurrerentNavigableMap` interface.

### Other

* Credit goes to my wife (Pinelopi) for tolerating awful amount of time I spend on this project. Also for proof reading and general advices.

* XTea encryption was taken from H2 Database (Thomas Mueller)

* LZF compression was ported to Java by Thomas Mueller. Original C implementation was written by Marc Alexander Lehmann and Oren J. Maurice

### Support

I would love to get more support, however so far only two companies supported this project.

* EJ-Technologies donated [JProfiler](http://www.ej-technologies.com/products/jprofiler/overview.html).
It is excellent tool and MapDB would not be possible without it.

* JetBrains donated [Intellij Idea Ultimate](http://www.jetbrains.com/idea/)



