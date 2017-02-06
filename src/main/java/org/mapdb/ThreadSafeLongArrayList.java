package org.mapdb;

import net.jcip.annotations.ThreadSafe;
import org.eclipse.collections.api.LazyLongIterable;
import org.eclipse.collections.api.LongIterable;
import org.eclipse.collections.api.bag.primitive.MutableLongBag;
import org.eclipse.collections.api.block.function.primitive.LongToObjectFunction;
import org.eclipse.collections.api.block.function.primitive.ObjectLongIntToObjectFunction;
import org.eclipse.collections.api.block.function.primitive.ObjectLongToObjectFunction;
import org.eclipse.collections.api.block.predicate.primitive.LongPredicate;
import org.eclipse.collections.api.block.procedure.primitive.LongIntProcedure;
import org.eclipse.collections.api.block.procedure.primitive.LongProcedure;
import org.eclipse.collections.api.iterator.MutableLongIterator;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.list.primitive.ImmutableLongList;
import org.eclipse.collections.api.list.primitive.LongList;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * long list
 *
 * @author 佐井
 * @since 2016-09-14 07:29
 *
 * //TODO migrate to Eclipse Collections, see https://github.com/eclipse/eclipse-collections/issues/196
 */
@ThreadSafe
class ThreadSafeLongArrayList implements MutableLongList {


    public ThreadSafeLongArrayList() {
        list = new LongArrayList();
    }

    private LongArrayList list;

    private ReadWriteLock readWriteLock = new ReentrantReadWriteLock(false);

    private Lock readLock = readWriteLock.readLock();
    private Lock writeLock = readWriteLock.writeLock();

    @Override
    public void addAtIndex(int index, long element) {
        writeLock.lock();
        try {
            list.addAtIndex(index, element);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean addAllAtIndex(int index, long... source) {
        writeLock.lock();
        try {
            return list.addAllAtIndex(index, source);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean addAllAtIndex(int index, LongIterable source) {
        writeLock.lock();
        try {
            return list.addAllAtIndex(index, source);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public long removeAtIndex(int index) {
        writeLock.lock();
        try {
            return list.removeAtIndex(index);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public long set(int index, long element) {
        writeLock.lock();
        try {
            return list.set(index, element);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public MutableLongIterator longIterator() {
        return list.longIterator();
    }

    @Override
    public long[] toArray() {
        readLock.lock();
        try {
            return list.toArray();
        } finally {

            readLock.unlock();
        }
    }

    @Override
    public boolean contains(long value) {
        readLock.lock();
        try {
            return list.contains(value);
        } finally {

            readLock.unlock();
        }
    }

    @Override
    public boolean containsAll(long... source) {
        readLock.lock();
        try {
            return list.containsAll(source);
        } finally {

            readLock.unlock();
        }
    }

    @Override
    public boolean containsAll(LongIterable source) {
        readLock.lock();
        try {
            return list.containsAll(source);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void forEach(LongProcedure procedure) {
        list.forEach(procedure);
    }

    @Override
    public void each(LongProcedure procedure) {
        readLock.lock();
        try {
            list.each(procedure);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public boolean add(long element) {
        writeLock.lock();
        try {
            return list.add(element);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean addAll(long... source) {
        writeLock.lock();
        try {
            return list.addAll(source);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean addAll(LongIterable source) {
        writeLock.lock();
        try {
            return list.addAll(source);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean remove(long value) {
        writeLock.lock();
        try {
            return list.remove(value);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean removeAll(LongIterable source) {
        writeLock.lock();
        try {
            return list.removeAll(source);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean removeAll(long... source) {
        writeLock.lock();
        try {
            return list.removeAll(source);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean retainAll(LongIterable elements) {
        writeLock.lock();
        try {
            return list.retainAll(elements);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean retainAll(long... source) {
        writeLock.lock();
        try {
            return list.retainAll(source);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void clear() {
        writeLock.lock();
        try {
            list.clear();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public long get(int index) {
        readLock.lock();
        try {
            return list.get(index);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public long dotProduct(LongList list) {
        readLock.lock();
        try {
            return list.dotProduct(list);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public int binarySearch(long value) {
        readLock.lock();
        try {
            return list.binarySearch(value);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public int lastIndexOf(long value) {
        readLock.lock();
        try {
            return list.lastIndexOf(value);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public long getLast() {
        readLock.lock();
        try {
            return list.getLast();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public LazyLongIterable asReversed() {
        return list.asReversed();
    }

    @Override
    public long getFirst() {
        readLock.lock();
        try {
            return list.getFirst();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public int indexOf(long value) {
        readLock.lock();
        try {
            return list.indexOf(value);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public MutableLongList select(LongPredicate predicate) {
        readLock.lock();
        try {
            return list.select(predicate);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public MutableLongList reject(LongPredicate predicate) {
        readLock.lock();
        try {
            return list.reject(predicate);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public MutableLongList with(long element) {
        writeLock.lock();
        try {
            return list.with(element);
        } finally {

            writeLock.unlock();
        }
    }

    @Override
    public MutableLongList without(long element) {
        writeLock.lock();
        try {
            return list.without(element);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public MutableLongList withAll(LongIterable elements) {
        writeLock.lock();
        try {
            return list.withAll(elements);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public MutableLongList withoutAll(LongIterable elements) {
        writeLock.lock();
        try {
            return list.withoutAll(elements);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public <V> MutableList<V> collect(LongToObjectFunction<? extends V> function) {
        readLock.lock();
        try {
            return list.collect(function);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public long detectIfNone(LongPredicate predicate, long ifNone) {
        readLock.lock();
        try {
            return list.detectIfNone(predicate, ifNone);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public int count(LongPredicate predicate) {
        readLock.lock();
        try {
            return list.count(predicate);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public boolean anySatisfy(LongPredicate predicate) {
        readLock.lock();
        try {
            return list.anySatisfy(predicate);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public boolean allSatisfy(LongPredicate predicate) {
        readLock.lock();
        try {
            return list.allSatisfy(predicate);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public boolean noneSatisfy(LongPredicate predicate) {
        readLock.lock();
        try {
            return list.noneSatisfy(predicate);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public MutableLongList toList() {
        readLock.lock();
        try {
            return list.toList();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public MutableLongSet toSet() {
        readLock.lock();
        try {
            return list.toSet();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public MutableLongBag toBag() {
        readLock.lock();
        try {
            return list.toBag();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public LazyLongIterable asLazy() {
        readLock.lock();
        try {
            return list.asLazy();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public <T> T injectInto(T injectedValue, ObjectLongToObjectFunction<? super T, ? extends T> function) {
        readLock.lock();
        try {
            return list.injectInto(injectedValue, function);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public long sum() {
        readLock.lock();
        try {
            return list.sum();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public long max() {
        readLock.lock();
        try {
            return list.max();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public long maxIfEmpty(long defaultValue) {
        readLock.lock();
        try {
            return list.maxIfEmpty(defaultValue);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public long min() {
        readLock.lock();
        try {
            return list.min();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public long minIfEmpty(long defaultValue) {
        readLock.lock();
        try {
            return list.minIfEmpty(defaultValue);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public double average() {
        readLock.lock();
        try {
            return list.average();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public double median() {
        readLock.lock();
        try {
            return list.median();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public long[] toSortedArray() {
        readLock.lock();
        try {
            return list.toSortedArray();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public MutableLongList toSortedList() {
        readLock.lock();
        try {
            return list.toSortedList();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public MutableLongList reverseThis() {
        writeLock.lock();
        try {
            return list.reverseThis();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public MutableLongList toReversed() {
        writeLock.lock();
        try {
            return list.toReversed();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public MutableLongList distinct() {
        readLock.lock();
        try {
            return list.distinct();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public <T> T injectIntoWithIndex(T injectedValue, ObjectLongIntToObjectFunction<? super T, ? extends T> function) {
        readLock.lock();
        try {
            return list.injectIntoWithIndex(injectedValue, function);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void forEachWithIndex(LongIntProcedure procedure) {
        readLock.lock();
        try {
            list.forEachWithIndex(procedure);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public MutableLongList sortThis() {
        writeLock.lock();
        try {
            return list.sortThis();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public MutableLongList asUnmodifiable() {
        readLock.lock();
        try {
            return list.asUnmodifiable();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public MutableLongList asSynchronized() {
        return this;
    }

    @Override
    public ImmutableLongList toImmutable() {
        readLock.lock();
        try {
            return list.toImmutable();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public MutableLongList subList(int fromIndex, int toIndex) {
        throw new UnsupportedOperationException("subList not yet implemented!");
    }

    @Override
    public int size() {
        readLock.lock();
        try {
            return list.size();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public boolean isEmpty() {
        readLock.lock();
        try {
            return list.isEmpty();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public boolean notEmpty() {
        readLock.lock();
        try {
            return list.notEmpty();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public String makeString() {
        readLock.lock();
        try {
            return list.makeString();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public String makeString(String separator) {
        readLock.lock();
        try {
            return list.makeString(separator);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public String makeString(String start, String separator, String end) {
        readLock.lock();
        try {
            return list.makeString(start, separator, end);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void appendString(Appendable appendable) {
        readLock.lock();
        try {
            list.appendString(appendable);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void appendString(Appendable appendable, String separator) {
        readLock.lock();
        try {
            list.appendString(appendable, separator);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void appendString(Appendable appendable, String start, String separator, String end) {
        readLock.lock();
        try {
            list.appendString(appendable, start, separator, end);
        } finally {
            readLock.unlock();
        }
    }
}
