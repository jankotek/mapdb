Queues
======

MapDB has three `Queue` implementations:

 * Queue aka First-In-First-Out
 * Stack aka Last-In-First-Out
 * CircularQueue with limited size

Lock-free queues in MapDB have several limitations, for example it is not 
possible to count elements without actually removing them. 
Queues in MapDB usually implement only minimal subset from
`BlockingQueue` interface. In future we will introduce `List` which 
will fully implement `BlockingDequeue` and `List` interfaces, this 
will use global `ReadWriteLock`.

To instantiate queue use get method:

```java
    // first-in-first-out queue
    BlockingQueue fifo = db.getQueue(“fifo”);

    // last-in-first-out queue (stack)
    BlockingQueue lifo = db.getStack(“lifo”);

    // circular queue with limited size
    BlockingQueue c = db.getCircularQueue(“circular”);
```

FIFO Queue
------------

This only takes two extra parameters. Fist you  can supply custom serializers used on entries.

Second param decides lock based eviction. Lock-free (false) means better concurrency, but places tomb-stones in place of removed entries, over time the store size grows and will need compaction. With locks (true) the removed entries are deleted under global locks, so there is concurrency penalty.

```java
    // first-in-first-out queue
    BlockingQueue<String> fifo = db.createQueue(“fifo”, Serializer.STRING, false);
```

Stack
-------

This only takes two extra parameters. Fist you can supply custom serializers used on entries.

Second param decides lock based eviction. Lock-free (false) means better concurrency, but places tomb-stones in place of removed entries, over time the store size grows and will need compaction. With locks (true) the removed entries are deleted under global locks, so there is concurrency penalty.

```java
    // last-in-first-out queue
    BlockingQueue<String> stack = db.createStack(“stack”, Serializer.STRING, false);
```

CircularQueue
---------------

This only takes two extra parameters. Fist you can supply custom serializers used on entries.

Second parameter is queue size. If number of entries exceeds the queue size, some entries will get overwritten and lost.

```java
    // circular queue
    BlockingQueue<String> circular = db.createCircularQueue(“circular”, Serializer.STRING, 10000);
```
