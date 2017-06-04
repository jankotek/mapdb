package org.mapdb.queue;

/**
 * Callback interface for {@link QueueLong}
 */
public interface QueueLongTakeUntil {

    boolean take(long nodeRecid, QueueLong.Node node);
}
