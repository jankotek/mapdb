/*
 * Copyright 2014 Goldman Sachs.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mapdb.tree.indexTreeLongLongMapTests_GS_GENERATED;

import org.eclipse.collections.api.collection.primitive.MutableLongCollection;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.iterator.MutableLongIterator;
import org.eclipse.collections.impl.bag.mutable.primitive.LongHashBag;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;
import org.eclipse.collections.impl.test.Verify;
import org.junit.Assert;
import org.junit.Test;

import java.util.NoSuchElementException;

/**
 * Abstract JUnit test for {@link MutableLongCollection}s
 * This file was automatically generated from template file abstractMutablePrimitiveCollectionTestCase.stg.
 */
public abstract class AbstractMutableLongCollectionTestCase extends AbstractLongIterableTestCase
{
    @Override
    protected abstract MutableLongCollection classUnderTest();

    @Override
    protected abstract MutableLongCollection newWith(long... elements);

    @Override
    protected abstract MutableLongCollection newMutableCollectionWith(long... elements);

    @Test
    public void clear()
    {
        MutableLongCollection emptyCollection = this.newWith();
        emptyCollection.clear();
        Verify.assertSize(0, emptyCollection);

        MutableLongCollection collection = this.classUnderTest();
        collection.clear();
        Verify.assertEmpty(collection);
        Verify.assertSize(0, collection);
        Assert.assertFalse(collection.contains(0L));
        Assert.assertFalse(collection.contains(1L));
        Assert.assertFalse(collection.contains(2L));
        Assert.assertFalse(collection.contains(3L));

        MutableLongCollection collection1 = this.newWith(0L, 1L, 31L, 32L);
        collection1.clear();
        Verify.assertEmpty(collection1);
        Verify.assertSize(0, collection1);
        Assert.assertFalse(collection1.contains(0L));
        Assert.assertFalse(collection1.contains(1L));
        Assert.assertFalse(collection1.contains(31L));
        Assert.assertFalse(collection1.contains(32L));

        MutableLongCollection collection2 = this.newWith(0L, 1L, 1L, 2L, 2L, 2L);
        collection2.clear();
        Verify.assertSize(0, collection2);
        Assert.assertEquals(this.newMutableCollectionWith(), collection2);
    }

    @Override
    @Test
    public void testEquals()
    {
        super.testEquals();
        Verify.assertPostSerializedEqualsAndHashCode(this.newWith());
    }

    @Override
    @Test
    public void contains()
    {
        super.contains();
        MutableLongCollection collection = this.newWith(14L, 2L, 30L, 31L, 32L, 35L, 0L, 1L);
        Assert.assertFalse(collection.contains(29L));
        Assert.assertFalse(collection.contains(49L));

        long[] numbers = {14L, 2L, 30L, 31L, 32L, 35L, 0L, 1L};
        for (long number : numbers)
        {
            Assert.assertTrue(collection.contains(number));
            Assert.assertTrue(collection.remove(number));
            Assert.assertFalse(collection.contains(number));
        }

        Assert.assertFalse(collection.contains(-1L));
        Assert.assertFalse(collection.contains(29L));
        Assert.assertFalse(collection.contains(49L));
    }

    @Test
    public void add()
    {
        MutableLongCollection emptyCollection = this.newWith();
        Assert.assertTrue(emptyCollection.add(1L));
        Assert.assertEquals(this.newMutableCollectionWith(1L), emptyCollection);
        MutableLongCollection collection = this.classUnderTest();
        Assert.assertTrue(collection.add(4L));
        Assert.assertEquals(this.newMutableCollectionWith(1L, 2L, 3L, 4L), collection);
    }

    @Test
    public void addAllArray()
    {
        MutableLongCollection collection = this.classUnderTest();
        Assert.assertFalse(collection.addAll());
        Assert.assertTrue(collection.addAll(4L, 5L, 6L));
        Assert.assertEquals(this.newMutableCollectionWith(1L, 2L, 3L, 4L, 5L, 6L), collection);
    }

    @Test
    public void addAllIterable()
    {
        MutableLongCollection collection = this.classUnderTest();
        Assert.assertFalse(collection.addAll(this.newMutableCollectionWith()));
        Assert.assertTrue(collection.addAll(this.newMutableCollectionWith(4L, 5L, 6L)));
        Assert.assertEquals(this.newMutableCollectionWith(1L, 2L, 3L, 4L, 5L, 6L), collection);
    }

    @Test
    public void remove()
    {
        MutableLongCollection collection = this.classUnderTest();
        Assert.assertFalse(collection.remove(-1L));
        Assert.assertEquals(this.newMutableCollectionWith(1L, 2L, 3L), collection);
        Assert.assertTrue(collection.remove(3L));
        Assert.assertEquals(this.newMutableCollectionWith(1L, 2L), collection);
    }

    @Test
    public void removeAll()
    {
        Assert.assertFalse(this.newWith().removeAll());
        Assert.assertFalse(this.newWith().removeAll(1L));

        MutableLongCollection collection = this.classUnderTest();
        Assert.assertFalse(collection.removeAll());
        Assert.assertFalse(collection.removeAll(-1L));
        Assert.assertEquals(this.newMutableCollectionWith(1L, 2L, 3L), collection);
        Assert.assertTrue(collection.removeAll(1L, 5L));
        Assert.assertEquals(this.newMutableCollectionWith(2L, 3L), collection);
        Assert.assertTrue(collection.removeAll(3L, 2L));
        Assert.assertEquals(this.newMutableCollectionWith(), collection);

        MutableLongCollection collection1 = this.newWith(0L, 1L, 1L, 2L, 2L, 2L);
        Assert.assertFalse(collection1.removeAll());
        Assert.assertTrue(collection1.removeAll(0L, 1L));
        Assert.assertEquals(this.newMutableCollectionWith(2L, 2L, 2L), collection1);
    }

    @Test
    public void removeAll_iterable()
    {
        MutableLongCollection collection = this.classUnderTest();
        Assert.assertFalse(collection.removeAll(this.newMutableCollectionWith()));
        Assert.assertFalse(collection.removeAll(this.newMutableCollectionWith(-1L)));
        Assert.assertEquals(this.newMutableCollectionWith(1L, 2L, 3L), collection);
        Assert.assertTrue(collection.removeAll(this.newMutableCollectionWith(1L, 5L)));
        Assert.assertEquals(this.newMutableCollectionWith(2L, 3L), collection);
        MutableLongCollection collection1 = this.classUnderTest();
        Assert.assertTrue(collection1.removeAll(this.newMutableCollectionWith(3L, 2L)));
        Assert.assertEquals(this.newMutableCollectionWith(1L), collection1);

        MutableLongCollection collection2 = this.newWith(0L, 1L, 1L, 2L, 2L, 2L, 3L);
        Assert.assertFalse(collection2.removeAll(new LongArrayList()));
        Assert.assertTrue(collection2.removeAll(LongArrayList.newListWith(0L, 1L)));
        Assert.assertEquals(this.newMutableCollectionWith(2L, 2L, 2L, 3L), collection2);
        Assert.assertFalse(collection2.removeAll(LongArrayList.newListWith(0L)));
        Assert.assertTrue(collection2.removeAll(LongArrayList.newListWith(2L)));
        Assert.assertEquals(this.newMutableCollectionWith(3L), collection2);

        MutableLongCollection collection3 = this.newWith(0L, 1L, 1L, 2L, 2L, 2L);
        Assert.assertTrue(collection3.removeAll(LongHashBag.newBagWith(0L, 1L, 1L)));
        Assert.assertEquals(this.newMutableCollectionWith(2L, 2L, 2L), collection3);
    }

    @Test
    public void retainAll()
    {
        MutableLongCollection collection = this.classUnderTest();
        Assert.assertFalse(collection.retainAll(1L, 2L, 3L));
        Assert.assertEquals(this.newMutableCollectionWith(1L, 2L, 3L), collection);
        Assert.assertTrue(collection.retainAll(1L, 2L, 5L));
        Assert.assertEquals(this.newMutableCollectionWith(1L, 2L), collection);

        MutableLongCollection collection1 = this.classUnderTest();
        Assert.assertTrue(collection1.retainAll(-3L, 1L));
        Assert.assertEquals(this.newMutableCollectionWith(1L), collection1);
        Assert.assertTrue(collection1.retainAll(-1L));
        Verify.assertEmpty(collection1);

        MutableLongCollection collection2 = this.newWith(0L, 1L, 1L, 2L, 2L, 2L, 3L, 3L, 3L);
        Assert.assertFalse(collection2.retainAll(0L, 1L, 2L, 3L));
        Assert.assertTrue(collection2.retainAll(0L, 1L, 3L));
        Assert.assertEquals(this.newMutableCollectionWith(0L, 1L, 1L, 3L, 3L, 3L), collection2);
        Assert.assertFalse(collection2.retainAll(0L, 1L, 3L));
        Assert.assertTrue(collection2.retainAll(5L, 3L));
        Assert.assertEquals(this.newMutableCollectionWith(3L, 3L, 3L), collection2);

        MutableLongCollection collection3 = this.newWith(0L, 1L, 1L, 2L, 2L, 2L);
        Assert.assertTrue(collection3.retainAll(2L, 8L, 8L, 2L));
        Assert.assertEquals(this.newMutableCollectionWith(2L, 2L, 2L), collection3);

        MutableLongCollection collection4 = this.classUnderTest();
        Assert.assertTrue(collection4.retainAll());
        Verify.assertEmpty(collection4);
    }

    @Test
    public void retainAll_iterable()
    {
        MutableLongCollection collection = this.classUnderTest();
        Assert.assertFalse(collection.retainAll(this.newMutableCollectionWith(1L, 2L, 3L)));
        Assert.assertEquals(this.newMutableCollectionWith(1L, 2L, 3L), collection);
        Assert.assertTrue(collection.retainAll(this.newMutableCollectionWith(1L, 2L, 5L)));
        Assert.assertEquals(this.newMutableCollectionWith(1L, 2L), collection);

        MutableLongCollection collection1 = this.classUnderTest();
        Assert.assertTrue(collection1.retainAll(this.newMutableCollectionWith(-3L, 1L)));
        Assert.assertEquals(this.newMutableCollectionWith(1L), collection1);
        Assert.assertTrue(collection1.retainAll(this.newMutableCollectionWith(-1L)));
        Verify.assertEmpty(collection1);

        MutableLongCollection collection2 = this.newWith(0L, 1L, 1L, 2L, 2L, 2L, 3L, 3L, 3L);
        Assert.assertFalse(collection2.retainAll(this.newMutableCollectionWith(0L, 1L, 2L, 3L)));
        Assert.assertTrue(collection2.retainAll(LongArrayList.newListWith(0L, 1L, 3L)));
        Assert.assertEquals(this.newMutableCollectionWith(0L, 1L, 1L, 3L, 3L, 3L), collection2);
        Assert.assertFalse(collection2.retainAll(LongArrayList.newListWith(0L, 1L, 3L)));
        Assert.assertTrue(collection2.retainAll(LongArrayList.newListWith(5L, 3L)));
        Assert.assertEquals(this.newMutableCollectionWith(3L, 3L, 3L), collection2);

        MutableLongCollection collection3 = this.newWith(0L, 1L, 1L, 2L, 2L, 2L);
        Assert.assertTrue(collection3.retainAll(LongHashBag.newBagWith(2L, 8L, 8L, 2L)));
        Assert.assertEquals(this.newMutableCollectionWith(2L, 2L, 2L), collection3);

        MutableLongCollection collection4 = this.classUnderTest();
        Assert.assertTrue(collection4.retainAll(new LongArrayList()));
        Verify.assertEmpty(collection4);
    }

    @Test
    public void with()
    {
        MutableLongCollection emptyCollection = this.newWith();
        MutableLongCollection collection = emptyCollection.with(1L);
        MutableLongCollection collection0 = this.newWith().with(1L).with(2L);
        MutableLongCollection collection1 = this.newWith().with(1L).with(2L).with(3L);
        MutableLongCollection collection2 = this.newWith().with(1L).with(2L).with(3L).with(4L);
        MutableLongCollection collection3 = this.newWith().with(1L).with(2L).with(3L).with(4L).with(5L);
        Assert.assertSame(emptyCollection, collection);
        Assert.assertEquals(this.newMutableCollectionWith(1L), collection);
        Assert.assertEquals(this.newMutableCollectionWith(1L, 2L), collection0);
        Assert.assertEquals(this.newMutableCollectionWith(1L, 2L, 3L), collection1);
        Assert.assertEquals(this.newMutableCollectionWith(1L, 2L, 3L, 4L), collection2);
        Assert.assertEquals(this.newMutableCollectionWith(1L, 2L, 3L, 4L, 5L), collection3);
    }

    @Test
    public void withAll()
    {
        MutableLongCollection emptyCollection = this.newWith();
        MutableLongCollection collection = emptyCollection.withAll(this.newMutableCollectionWith(1L));
        MutableLongCollection collection0 = this.newWith().withAll(this.newMutableCollectionWith(1L, 2L));
        MutableLongCollection collection1 = this.newWith().withAll(this.newMutableCollectionWith(1L, 2L, 3L));
        MutableLongCollection collection2 = this.newWith().withAll(this.newMutableCollectionWith(1L, 2L, 3L, 4L));
        MutableLongCollection collection3 = this.newWith().withAll(this.newMutableCollectionWith(1L, 2L, 3L, 4L, 5L));
        Assert.assertSame(emptyCollection, collection);
        Assert.assertEquals(this.newMutableCollectionWith(1L), collection);
        Assert.assertEquals(this.newMutableCollectionWith(1L, 2L), collection0);
        Assert.assertEquals(this.classUnderTest(), collection1);
        Assert.assertEquals(this.newMutableCollectionWith(1L, 2L, 3L, 4L), collection2);
        Assert.assertEquals(this.newMutableCollectionWith(1L, 2L, 3L, 4L, 5L), collection3);
    }

    @Test
    public void without()
    {
        MutableLongCollection collection = this.newWith(1L, 2L, 3L, 4L, 5L);
        Assert.assertSame(collection, collection.without(9L));
        Assert.assertEquals(this.newMutableCollectionWith(1L, 2L, 3L, 4L, 5L), collection.without(9L));
        Assert.assertEquals(this.newMutableCollectionWith(2L, 3L, 4L, 5L), collection.without(1L));
        Assert.assertEquals(this.newMutableCollectionWith(3L, 4L, 5L), collection.without(2L));
        Assert.assertEquals(this.newMutableCollectionWith(4L, 5L), collection.without(3L));
        Assert.assertEquals(this.newMutableCollectionWith(5L), collection.without(4L));
        Assert.assertEquals(this.newMutableCollectionWith(), collection.without(5L));
        Assert.assertEquals(this.newMutableCollectionWith(), collection.without(6L));
    }

    @Test
    public void withoutAll()
    {
        MutableLongCollection collection = this.newWith(1L, 2L, 3L, 4L, 5L);
        Assert.assertSame(collection, collection.withoutAll(this.newMutableCollectionWith(8L, 9L)));
        Assert.assertEquals(this.newMutableCollectionWith(1L, 2L, 3L, 4L, 5L), collection.withoutAll(this.newMutableCollectionWith(8L, 9L)));
        Assert.assertEquals(this.newMutableCollectionWith(2L, 3L, 4L), collection.withoutAll(this.newMutableCollectionWith(1L, 5L)));
        Assert.assertEquals(this.newMutableCollectionWith(3L, 4L), collection.withoutAll(this.newMutableCollectionWith(2L, 20L)));
        Assert.assertEquals(this.newMutableCollectionWith(), collection.withoutAll(this.newMutableCollectionWith(3L, 4L)));
        Assert.assertEquals(this.newMutableCollectionWith(), collection.withoutAll(this.newMutableCollectionWith(9L)));

        MutableLongCollection collection1 = this.newWith(0L, 1L, 1L, 2L, 2L, 2L);
        Assert.assertEquals(this.newMutableCollectionWith(2L, 2L, 2L), collection1.withoutAll(LongHashBag.newBagWith(0L, 1L)));
    }

    @Test
    public void asSynchronized()
    {
        MutableLongCollection collection = this.classUnderTest();
        Assert.assertEquals(collection, collection.asSynchronized());
        Verify.assertInstanceOf(this.newWith(1L, 2L, 3L).asSynchronized().getClass(), this.classUnderTest().asSynchronized());

        MutableLongCollection collection1 = this.newWith(1L, 2L, 2L, 3L, 3L, 3L);
        MutableLongCollection synchronizedCollection = this.newWith(1L, 2L, 2L, 3L, 3L, 3L).asSynchronized();
        Verify.assertInstanceOf(synchronizedCollection.getClass(), collection1.asSynchronized());
        Assert.assertEquals(synchronizedCollection, collection1.asSynchronized());
    }

    @Test
    public void asUnmodifiable()
    {
        Verify.assertInstanceOf(this.newWith(1L, 2L, 3L).asUnmodifiable().getClass(), this.classUnderTest().asUnmodifiable());
        Assert.assertEquals(this.newWith(1L, 2L, 3L).asUnmodifiable(), this.classUnderTest().asUnmodifiable());

        MutableLongCollection collection = this.newWith(1L, 2L, 2L, 3L, 3L, 3L);
        MutableLongCollection unmodifiableCollection = this.newWith(1L, 2L, 2L, 3L, 3L, 3L).asUnmodifiable();
        Verify.assertInstanceOf(unmodifiableCollection.getClass(), collection.asUnmodifiable());
        Assert.assertEquals(unmodifiableCollection, collection.asUnmodifiable());
    }

    @Override
    @Test(expected = NoSuchElementException.class)
    public void longIterator_throws_non_empty_collection()
    {
        super.longIterator_throws_non_empty_collection();
        MutableLongCollection collection = this.newWith();
        collection.add(1L);
        collection.add(2L);
        collection.add(3L);
        LongIterator iterator = collection.longIterator();
        while (iterator.hasNext())
        {
            iterator.next();
        }
        iterator.next();
    }

    @Test
    public void longIterator_with_remove()
    {
        MutableLongCollection longIterable = this.newWith(0L, 1L, 31L, 32L);
        final MutableLongIterator iterator = longIterable.longIterator();
        while (iterator.hasNext())
        {
            iterator.next();
            iterator.remove();
        }
        Verify.assertEmpty(longIterable);
        Verify.assertThrows(NoSuchElementException.class, new Runnable() {
            @Override
            public void run() {
                iterator.next();
            }
        });
    }

    @Test
    public void longIterator_throws_for_remove_before_next()
    {
        MutableLongCollection longIterable = this.classUnderTest();
        final MutableLongIterator iterator = longIterable.longIterator();
        Assert.assertTrue(iterator.hasNext());
        Verify.assertThrows(IllegalStateException.class, new Runnable() {
            @Override
            public void run() {
                iterator.remove();
            }
        });
    }

    @Test
    public void longIterator_throws_for_consecutive_remove()
    {
        MutableLongCollection longIterable = this.classUnderTest();
        final MutableLongIterator iterator = longIterable.longIterator();
        Assert.assertTrue(iterator.hasNext());
        iterator.next();
        iterator.remove();
        Verify.assertThrows(IllegalStateException.class,new Runnable() {
            @Override
            public void run() {
                iterator.remove();
            }
        });

    }
}
