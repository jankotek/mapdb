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

import org.eclipse.collections.api.LazyLongIterable;
import org.eclipse.collections.api.LongIterable;
import org.eclipse.collections.api.RichIterable;
import org.eclipse.collections.api.block.function.primitive.LongToObjectFunction;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.impl.bag.mutable.primitive.LongHashBag;
import org.eclipse.collections.impl.block.factory.primitive.LongPredicates;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.eclipse.collections.impl.test.Verify;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.NoSuchElementException;

/**
 * Abstract JUnit test for {@link LongIterable}s
 * This file was automatically generated from template file abstractPrimitiveIterableTestCase.stg.
 */
public abstract class AbstractLongIterableTestCase
{
    protected abstract LongIterable classUnderTest();

    protected abstract LongIterable newWith(long... elements);

    protected abstract LongIterable newMutableCollectionWith(long... elements);

    protected abstract RichIterable<Long> newObjectCollectionWith(Long... elements);

    @Test
    public void newCollectionWith()
    {
        LongIterable iterable = this.newWith(1L, 2L, 3L);
        Verify.assertSize(3, iterable);
        Verify.assertSize(4, this.newWith(0L, 1L, 31L, 32L));
        Assert.assertTrue(iterable.containsAll(1L, 2L, 3L));

        LongIterable iterable1 = this.newWith();
        Verify.assertEmpty(iterable1);
        Assert.assertFalse(iterable1.containsAll(1L, 2L, 3L));

        LongIterable iterable2 = this.newWith(1L);
        Verify.assertSize(1, iterable2);
        Assert.assertFalse(iterable2.containsAll(1L, 2L, 3L));
    }

    @Test
    public void newCollection()
    {
        Assert.assertEquals(this.newMutableCollectionWith(), this.newWith());
        Assert.assertEquals(this.newMutableCollectionWith(1L, 2L, 3L), this.newWith(1L, 2L, 3L));
        Assert.assertEquals(this.newMutableCollectionWith(0L, 1L, 31L, 32L), this.newWith(0L, 1L, 31L, 32L));
    }

    @Test
    public void isEmpty()
    {
        Verify.assertEmpty(this.newWith());
        Verify.assertNotEmpty(this.classUnderTest());
        Verify.assertNotEmpty(this.newWith(0L, 1L, 31L, 32L));
        Verify.assertNotEmpty(this.newWith(0L, 1L, 2L));
        Verify.assertNotEmpty(this.newWith(0L, 31L));
        Verify.assertNotEmpty(this.newWith(31L, 32L));
        Verify.assertNotEmpty(this.newWith(32L, 33L));
    }

    @Test
    public void notEmpty()
    {
        Assert.assertFalse(this.newWith().notEmpty());
        Assert.assertTrue(this.classUnderTest().notEmpty());
        Assert.assertTrue(this.newWith(0L, 1L, 31L, 32L).notEmpty());
        Assert.assertTrue(this.newWith(0L, 1L, 2L).notEmpty());
        Assert.assertTrue(this.newWith(0L, 31L).notEmpty());
        Assert.assertTrue(this.newWith(31L, 32L).notEmpty());
        Assert.assertTrue(this.newWith(32L, 33L).notEmpty());
    }

    @Test
    public void contains()
    {
        LongIterable iterable = this.newWith(14L, 2L, 30L, 31L, 32L, 35L, 0L, 1L);
        Assert.assertFalse(iterable.contains(29L));
        Assert.assertFalse(iterable.contains(49L));

        long[] numbers = {14L, 2L, 30L, 31L, 32L, 35L, 0L, 1L};
        for (long number : numbers)
        {
            Assert.assertTrue(iterable.contains(number));
        }

        Assert.assertFalse(iterable.contains(-1L));
        Assert.assertFalse(iterable.contains(29L));
        Assert.assertFalse(iterable.contains(49L));

        LongIterable iterable1 = this.newWith(0L, 1L, 1L, 2L, 2L, 2L);
        Assert.assertTrue(iterable1.contains(0L));
        Assert.assertTrue(iterable1.contains(1L));
        Assert.assertTrue(iterable1.contains(2L));
        Assert.assertFalse(iterable1.contains(3L));

        LongIterable iterable2 = this.classUnderTest();
        for (long each = 1; each <= iterable2.size(); each++)
        {
            Assert.assertTrue(iterable2.contains(each));
        }
        Assert.assertFalse(iterable2.contains(iterable2.size() + 1));
    }







    @Test
    public void containsAllArray()
    {
        Assert.assertTrue(this.classUnderTest().containsAll(this.classUnderTest().toArray()));
        Assert.assertFalse(this.classUnderTest().containsAll(this.classUnderTest().size() + 1));

        LongIterable iterable = this.newWith(1L, 2L, 3L);
        Assert.assertTrue(iterable.containsAll(1L));
        Assert.assertTrue(iterable.containsAll(1L, 2L, 3L));
        Assert.assertFalse(iterable.containsAll(1L, 2L, 3L, 4L));
        Assert.assertFalse(iterable.containsAll(1L, 2L, 4L));
        Assert.assertFalse(iterable.containsAll(4L, 5L, 6L));

        LongIterable iterable1 = this.newWith(14L, 2L, 30L, 32L, 35L, 0L, 1L);
        Assert.assertTrue(iterable1.containsAll(14L));
        Assert.assertTrue(iterable1.containsAll(35L));
        Assert.assertFalse(iterable1.containsAll(-1L));
        Assert.assertTrue(iterable1.containsAll(14L, 1L, 30L));
        Assert.assertTrue(iterable1.containsAll(14L, 1L, 32L));
        Assert.assertTrue(iterable1.containsAll(14L, 1L, 35L));
        Assert.assertFalse(iterable1.containsAll(0L, 2L, 35L, -1L));
        Assert.assertFalse(iterable1.containsAll(31L, -1L));

        LongIterable iterable2 = this.newWith(0L, 1L, 1L, 2L, 2L, 2L);
        Assert.assertTrue(iterable2.containsAll(0L));
        Assert.assertTrue(iterable2.containsAll(0L, 0L, 0L));
        Assert.assertTrue(iterable2.containsAll(0L, 1L, 1L));
        Assert.assertTrue(iterable2.containsAll(0L, 1L, 2L));
        Assert.assertFalse(iterable2.containsAll(0L, 1L, 2L, 3L, 4L));
        Assert.assertFalse(iterable2.containsAll(3L, 4L));
    }

    @Test
    public void containsAllIterable()
    {
        LongIterable source = this.classUnderTest();
        Assert.assertTrue(source.containsAll(this.classUnderTest()));
        Assert.assertFalse(source.containsAll(LongArrayList.newListWith(source.size() + 1)));

        LongIterable iterable = this.newWith(1L, 2L, 3L);
        Assert.assertTrue(this.newWith().containsAll(new LongArrayList()));
        Assert.assertFalse(this.newWith().containsAll(LongArrayList.newListWith(1L)));
        Assert.assertTrue(iterable.containsAll(LongArrayList.newListWith(1L)));
        Assert.assertTrue(iterable.containsAll(LongArrayList.newListWith(1L, 2L, 3L)));
        Assert.assertFalse(iterable.containsAll(LongArrayList.newListWith(1L, 2L, 3L, 4L)));
        Assert.assertFalse(iterable.containsAll(LongArrayList.newListWith(1L, 2L, 4L)));
        Assert.assertFalse(iterable.containsAll(LongArrayList.newListWith(4L, 5L, 6L)));

        LongIterable iterable1 = this.newWith(14L, 2L, 30L, 32L, 35L, 0L, 1L);
        Assert.assertTrue(iterable1.containsAll(LongHashSet.newSetWith(14L)));
        Assert.assertTrue(iterable1.containsAll(LongHashSet.newSetWith(35L)));
        Assert.assertFalse(iterable1.containsAll(LongHashSet.newSetWith(-1L)));
        Assert.assertTrue(iterable1.containsAll(LongHashSet.newSetWith(14L, 1L, 30L)));
        Assert.assertTrue(iterable1.containsAll(LongHashSet.newSetWith(14L, 1L, 32L)));
        Assert.assertTrue(iterable1.containsAll(LongHashSet.newSetWith(14L, 1L, 35L)));
        Assert.assertFalse(iterable1.containsAll(LongHashSet.newSetWith(0L, 2L, 35L, -1L)));
        Assert.assertFalse(iterable1.containsAll(LongHashSet.newSetWith(31L, -1L)));

        LongIterable iterable2 = this.newWith(0L, 1L, 1L, 2L, 2L, 2L);
        Assert.assertTrue(iterable2.containsAll(LongArrayList.newListWith(0L)));
        Assert.assertTrue(iterable2.containsAll(LongArrayList.newListWith(0L, 0L, 0L)));
        Assert.assertTrue(iterable2.containsAll(LongArrayList.newListWith(0L, 1L, 1L)));
        Assert.assertTrue(iterable2.containsAll(LongArrayList.newListWith(0L, 1L, 2L)));
        Assert.assertFalse(iterable2.containsAll(LongArrayList.newListWith(0L, 1L, 2L, 3L, 4L)));
        Assert.assertFalse(iterable2.containsAll(LongArrayList.newListWith(3L, 4L)));
    }

    @Test
    public abstract void longIterator();

    @Test(expected = NoSuchElementException.class)
    public void longIterator_throws()
    {
        LongIterator iterator = this.classUnderTest().longIterator();
        while (iterator.hasNext())
        {
            iterator.next();
        }
        iterator.next();
    }

    @Test(expected = NoSuchElementException.class)
    public void longIterator_throws_non_empty_collection()
    {
        LongIterable iterable = this.newWith(1L, 2L, 3L);
        LongIterator iterator = iterable.longIterator();
        while (iterator.hasNext())
        {
            iterator.next();
        }
        iterator.next();
    }

    @Test
    public void forEach()
    {
        long[] sum = new long[1];
        this.classUnderTest().forEach(each -> sum[0] += each);

        int size = this.classUnderTest().size();
        long sum1 = (long) ((size * (size + 1)) / 2);
        Assert.assertEquals(sum1, sum[0]);
    }

    @Test
    public void size()
    {
        Verify.assertSize(0, this.newWith());
        Verify.assertSize(1, this.newWith(3L));
        Verify.assertSize(3, this.newWith(1L, 2L, 3L));
    }

    @Test
    public void count()
    {
        LongIterable iterable = this.classUnderTest();
        int size = iterable.size();
        Assert.assertEquals(size >= 3 ? 3 : size, iterable.count(LongPredicates.lessThan(4L)));
        Assert.assertEquals(2L, this.newWith(1L, 0L, 2L).count(LongPredicates.greaterThan(0L)));
    }

    @Test
    public void anySatisfy()
    {
        Assert.assertTrue(this.newWith(1L+100, -1L+100, 2L+100).anySatisfy(LongPredicates.greaterThan(0L+100)));
        Assert.assertFalse(this.newWith(1L+100, -1L+100, 2L+100).anySatisfy(LongPredicates.equal(0L+100)));
        Assert.assertTrue(this.newWith(-1L+100, -1L+100, -2L+100, 31L+100, 32L+100).anySatisfy(LongPredicates.greaterThan(0L+100)));
        Assert.assertTrue(this.newWith(2L+100, -1L+100, -2L+100, 31L+100, 32L+100).anySatisfy(LongPredicates.greaterThan(0L+100)));
        Assert.assertFalse(this.newWith(1L+100, -1L+100, 31L+100, 32L+100).anySatisfy(LongPredicates.equal(0L+100)));
        Assert.assertTrue(this.newWith(32L).anySatisfy(LongPredicates.greaterThan(0L)));
        LongIterable iterable = this.newWith(0L, 1L, 2L);
        Assert.assertTrue(iterable.anySatisfy(value -> value < 3L));
        Assert.assertFalse(iterable.anySatisfy(LongPredicates.greaterThan(3L)));

        LongIterable iterable1 = this.classUnderTest();
        int size = iterable1.size();
        Assert.assertEquals(size > 3, iterable1.anySatisfy(LongPredicates.greaterThan(3L)));
        Assert.assertEquals(size != 0, iterable1.anySatisfy(LongPredicates.lessThan(3L)));
    }

    @Test
    public void allSatisfy()
    {
        Assert.assertFalse(this.newWith(1L, 0L, 2L).allSatisfy(LongPredicates.greaterThan(0L)));
        Assert.assertTrue(this.newWith(1L, 2L, 3L).allSatisfy(LongPredicates.greaterThan(0L)));
        Assert.assertFalse(this.newWith(1L, 0L, 31L, 32L).allSatisfy(LongPredicates.greaterThan(0L)));
        Assert.assertFalse(this.newWith(1L, 0L, 31L, 32L).allSatisfy(LongPredicates.greaterThan(0L)));
        Assert.assertTrue(this.newWith(1L, 2L, 31L, 32L).allSatisfy(LongPredicates.greaterThan(0L)));
        Assert.assertFalse(this.newWith(32L).allSatisfy(LongPredicates.equal(33L)));
        Assert.assertFalse(this.newWith(-32L+100).allSatisfy(LongPredicates.equal(33L+100)));
        LongIterable iterable = this.newWith(0L, 1L, 2L);
        Assert.assertFalse(iterable.allSatisfy(value -> 3L < value));
        Assert.assertTrue(iterable.allSatisfy(LongPredicates.lessThan(3L)));

        LongIterable iterable1 = this.classUnderTest();
        int size = iterable1.size();
        Assert.assertEquals(size == 0, iterable1.allSatisfy(LongPredicates.greaterThan(3L)));
        Assert.assertEquals(size < 3, iterable1.allSatisfy(LongPredicates.lessThan(3L)));
    }

    @Test
    public void noneSatisfy()
    {
        Assert.assertFalse(this.newWith(1L, 0L, 2L).noneSatisfy(LongPredicates.greaterThan(0L)));
        Assert.assertFalse(this.newWith(1L, 0L, 2L).noneSatisfy(LongPredicates.equal(0L)));
        Assert.assertTrue(this.newWith(1L, 2L, 3L).noneSatisfy(LongPredicates.greaterThan(3L)));
        Assert.assertFalse(this.newWith(1L, 0L, 31L, 32L).noneSatisfy(LongPredicates.greaterThan(0L)));
        Assert.assertFalse(this.newWith(1L, 0L, 31L, 32L).noneSatisfy(LongPredicates.greaterThan(0L)));
        Assert.assertTrue(this.newWith(1L, 2L, 31L, 32L).noneSatisfy(LongPredicates.lessThan(0L)));
        Assert.assertFalse(this.newWith(32L).noneSatisfy(LongPredicates.greaterThan(0L)));
        LongIterable iterable = this.newWith(0L, 1L, 2L);
        Assert.assertFalse(iterable.noneSatisfy(value -> 1L < value));
        Assert.assertTrue(iterable.noneSatisfy(LongPredicates.greaterThan(3L)));

        LongIterable iterable1 = this.classUnderTest();
        int size = iterable1.size();
        Assert.assertEquals(size <= 3, iterable1.noneSatisfy(LongPredicates.greaterThan(3L)));
        Assert.assertEquals(size == 0, iterable1.noneSatisfy(LongPredicates.lessThan(3L)));
    }

    @Test
    public void collect()
    {
        LongToObjectFunction<Long> function = parameter -> parameter - 1;
        Assert.assertEquals(this.newObjectCollectionWith(0L, 1L, 2L), this.newWith(1L, 2L, 3L).collect(function));
        LongIterable iterable = this.newWith(1L, 2L, 2L, 3L, 3L, 3L);
        Assert.assertEquals(this.newObjectCollectionWith(0L, 1L, 1L, 2L, 2L, 2L), iterable.collect(function));
        Assert.assertEquals(this.newObjectCollectionWith(), this.newWith().collect(function));
        Assert.assertEquals(this.newObjectCollectionWith(2L), this.newWith(3L).collect(function));
    }

    @Test
    public void select()
    {
        LongIterable iterable = this.classUnderTest();
        int size = iterable.size();
        Verify.assertSize(size >= 3 ? 3 : size, iterable.select(LongPredicates.lessThan(4L)));
        Verify.assertSize(size >= 2 ? 2 : size, iterable.select(LongPredicates.lessThan(3L)));
        LongIterable iterable1 = this.newWith(0L, 1L, 2L, 2L, 3L, 3L, 3L);
        Assert.assertEquals(this.newMutableCollectionWith(0L, 1L), iterable1.select(LongPredicates.lessThan(2L)));
        Assert.assertEquals(this.newMutableCollectionWith(2L, 2L, 3L, 3L, 3L), iterable1.select(LongPredicates.greaterThan(1L)));
    }

    @Test
    public void reject()
    {
        LongIterable iterable = this.classUnderTest();
        int size = iterable.size();
        Verify.assertSize(size <= 3 ? 0 : size - 3, iterable.reject(LongPredicates.lessThan(4L)));
        Verify.assertSize(size <= 2 ? 0 : size - 2, iterable.reject(LongPredicates.lessThan(3L)));
        LongIterable iterable1 = this.newWith(0L, 1L, 2L, 2L, 3L, 3L, 3L);
        Assert.assertEquals(this.newMutableCollectionWith(2L, 2L, 3L, 3L, 3L), iterable1.reject(LongPredicates.lessThan(2L)));
        Assert.assertEquals(this.newMutableCollectionWith(0L, 1L), iterable1.reject(LongPredicates.greaterThan(1L)));
    }

    @Test
    public void detectIfNone()
    {
        LongIterable iterable = this.classUnderTest();
        int size = iterable.size();
        Assert.assertEquals(size >= 4 ? 4L : 0L, iterable.detectIfNone(LongPredicates.equal(4L), 0L));
        Assert.assertEquals(size >= 2 ? 2L : 0L, iterable.detectIfNone(LongPredicates.equal(2L), 0L));
        Assert.assertEquals(size > 0 ? 1L : 0L, iterable.detectIfNone(LongPredicates.lessThan(2L), 0L));
        Assert.assertEquals(size > 3 ? 4L : 0L, iterable.detectIfNone(LongPredicates.greaterThan(3L), 0L));

        LongIterable iterable1 = this.newWith(0L, 1L, 2L, 2L, 3L, 3L, 3L);
        Assert.assertEquals(0L, iterable1.detectIfNone(LongPredicates.lessThan(1L), 4L));
        Assert.assertEquals(3L, iterable1.detectIfNone(LongPredicates.greaterThan(2L), 4L));
        Assert.assertEquals(4L, iterable1.detectIfNone(LongPredicates.greaterThan(4L), 4L));
    }

    @Test
    public void max()
    {
        Assert.assertEquals(9L+100, this.newWith(-1L+100, -2L+100, 9L+100).max());
        Assert.assertEquals(-1L+100, this.newWith(-1L+100, -2L+100, -9L+100).max());
        Assert.assertEquals(32L, this.newWith(1L, 0L, 9L, 30L, 31L, 32L).max());
        Assert.assertEquals(32L, this.newWith(1L, 0L, 9L, 30L, 31L, 32L).max());
        Assert.assertEquals(31L, this.newWith(31L, 0L, 30L).max());
        Assert.assertEquals(39L, this.newWith(32L, 39L, 35L).max());
        Assert.assertEquals(this.classUnderTest().size(), this.classUnderTest().max());
    }

    @Test(expected = NoSuchElementException.class)
    public void max_throws_emptyCollection()
    {
        this.newWith().max();
    }

    @Test
    public void min()
    {
        Assert.assertEquals(-2L+100, this.newWith(-1L+100, -2L+100, 9L+100).min());
        Assert.assertEquals(0L, this.newWith(1L, 0L, 9L, 30L, 31L, 32L).min());
        Assert.assertEquals(0L, this.newWith(0L, 9L, 30L, 31L, 32L).min());
        Assert.assertEquals(31L, this.newWith(31L, 32L, 33L).min());
        Assert.assertEquals(32L, this.newWith(32L, 39L, 35L).min());
        Assert.assertEquals(1L, this.classUnderTest().min());
    }

    @Test(expected = NoSuchElementException.class)
    public void min_throws_emptyCollection()
    {
        this.newWith().min();
    }

    @Test
    public void minIfEmpty()
    {
        Assert.assertEquals(5L, this.newWith().minIfEmpty(5L));
        Assert.assertEquals(0L, this.newWith().minIfEmpty(0L));
        Assert.assertEquals(0L, this.newWith(1L, 0L, 9L, 7L).minIfEmpty(5L));
        int size = this.classUnderTest().size();
        Assert.assertEquals(size == 0 ? 5L : 1L, this.classUnderTest().minIfEmpty(5L));
    }

    @Test
    public void maxIfEmpty()
    {
        Assert.assertEquals(5L, this.newWith().maxIfEmpty(5L));
        Assert.assertEquals(0L, this.newWith().maxIfEmpty(0L));
        Assert.assertEquals(9L, this.newWith(1L, 0L, 9L, 7L).maxIfEmpty(5L));
        int size = this.classUnderTest().size();
        Assert.assertEquals(size == 0 ? 5L : size, this.classUnderTest().maxIfEmpty(5L));
    }

    @Test
    public void sum()
    {
        int size = this.classUnderTest().size();
        long sum = (long) ((size * (size + 1)) / 2);
        Assert.assertEquals(sum, this.classUnderTest().sum());
        Assert.assertEquals(10L, this.newWith(0L, 1L, 2L, 3L, 4L).sum());
        Assert.assertEquals(93L, this.newWith(30L, 31L, 32L).sum());
    }

    @Test
    public void average()
    {
        int size = this.classUnderTest().size();
        long sum = (long) ((size * (size + 1)) / 2);
        double average = sum / size;
        Assert.assertEquals(average, this.classUnderTest().average(), 0.0);
        Assert.assertEquals(2.5, this.newWith(1L, 2L, 3L, 4L).average(), 0.0);
        Assert.assertEquals(2.5, this.newWith(1L, 2L, 3L, 4L).average(), 0.0);
        Assert.assertEquals(31.0, this.newWith(30L, 30L, 31L, 31L, 32L, 32L).average(), 0.0);
    }

    @Test(expected = ArithmeticException.class)
    public void averageThrowsOnEmpty()
    {
        this.newWith().average();
    }

    @Test
    public void median()
    {
        Assert.assertEquals(1.0, this.newWith(1L).median(), 0.0);
        Assert.assertEquals(2.5, this.newWith(1L, 2L, 3L, 4L).median(), 0.0);
        Assert.assertEquals(3.0, this.newWith(1L, 2L, 3L, 4L, 5L).median(), 0.0);
        Assert.assertEquals(31.0, this.newWith(30L, 30L, 31L, 31L, 32L).median(), 0.0);
        Assert.assertEquals(30.5, this.newWith(1L, 30L, 30L, 31L, 31L, 32L).median(), 0.0);
    }

    @Test(expected = ArithmeticException.class)
    public void medianThrowsOnEmpty()
    {
        this.newWith().median();
    }

    @Test
    public void toArray()
    {
        Assert.assertEquals(this.classUnderTest().size(), this.classUnderTest().toArray().length);
        LongIterable iterable = this.newWith(1L, 2L);
        Assert.assertTrue(Arrays.equals(new long[]{1L, 2L}, iterable.toArray())
                || Arrays.equals(new long[]{2L, 1L}, iterable.toArray()));
        Assert.assertTrue(Arrays.equals(new long[]{0L, 1L}, this.newWith(0L, 1L).toArray())
                || Arrays.equals(new long[]{1L, 0L}, this.newWith(0L, 1L).toArray()));
        Assert.assertTrue(Arrays.equals(new long[]{1L, 31L}, this.newWith(1L, 31L).toArray())
                || Arrays.equals(new long[]{31L, 1L}, this.newWith(1L, 31L).toArray()));
        Assert.assertTrue(Arrays.equals(new long[]{31L, 35L}, this.newWith(31L, 35L).toArray())
                || Arrays.equals(new long[]{35L, 31L}, this.newWith(31L, 35L).toArray()));
        Assert.assertArrayEquals(new long[]{}, this.newWith().toArray());
        Assert.assertArrayEquals(new long[]{32L}, this.newWith(32L).toArray());
    }

    @Test
    public void toSortedArray()
    {
        LongIterable iterable = this.classUnderTest();
        int size = iterable.size();
        long[] array = new long[size];
        for (int i = 0; i < size; i++)
        {
            array[i] = i + 1;
        }

        Assert.assertArrayEquals(array, iterable.toSortedArray());
        Assert.assertArrayEquals(new long[]{1L, 3L, 7L, 9L},
                this.newWith(3L, 1L, 9L, 7L).toSortedArray());
    }

    @Test
    public void testEquals()
    {
        LongIterable iterable1 = this.newWith(1L, 2L, 3L, 4L);
        LongIterable iterable2 = this.newWith(1L, 2L, 3L, 4L);
        LongIterable iterable3 = this.newWith(5L, 6L, 7L, 8L);
        LongIterable iterable4 = this.newWith(5L, 6L, 7L);
        LongIterable iterable5 = this.newWith(0L, 1L, 1L, 2L, 2L, 2L);
        LongIterable iterable6 = this.newWith(1L, 31L, 32L);
        LongIterable iterable7 = this.newWith(35L, 31L, 1L);
        LongIterable iterable8 = this.newWith(32L, 31L, 1L, 50L);
        LongIterable iterable9 = this.newWith(0L, 1L, 2L);
        LongIterable iterable10 = this.newWith(0L, 1L, 3L);
        LongIterable iterable11 = this.newWith(3L, 1L, 2L);
        LongIterable iterable12 = this.newWith(3L);

        Verify.assertEqualsAndHashCode(iterable1, iterable2);
        Verify.assertPostSerializedEqualsAndHashCode(iterable1);
        Verify.assertPostSerializedEqualsAndHashCode(iterable12);
        Verify.assertPostSerializedEqualsAndHashCode(iterable5);
        Verify.assertPostSerializedEqualsAndHashCode(iterable6);
        Assert.assertNotEquals(iterable12, iterable11);
        Assert.assertNotEquals(iterable1, iterable3);
        Assert.assertNotEquals(iterable1, iterable4);
        Assert.assertNotEquals(iterable6, iterable7);
        Assert.assertNotEquals(iterable6, iterable8);
        Assert.assertNotEquals(iterable9, iterable10);
        Assert.assertNotEquals(iterable9, iterable11);
        Assert.assertNotEquals(this.newWith(), this.newWith(100L));
    }

    @Test
    public void testHashCode()
    {
        Assert.assertEquals(this.newObjectCollectionWith(1L, 2L, 3L).hashCode(), this.newWith(1L, 2L, 3L).hashCode());
        Assert.assertEquals(this.newObjectCollectionWith(0L, 1L, 31L).hashCode(), this.newWith(0L, 1L, 31L).hashCode());
        Assert.assertEquals(this.newObjectCollectionWith(32L).hashCode(), this.newWith(32L).hashCode());
        Assert.assertNotEquals(this.newObjectCollectionWith(32L).hashCode(), this.newWith(0L).hashCode());
        Assert.assertEquals(this.newObjectCollectionWith(31L, 32L, 50L).hashCode(), this.newWith(31L, 32L, 50L).hashCode());
        Assert.assertEquals(this.newObjectCollectionWith(32L, 50L, 60L).hashCode(), this.newWith(32L, 50L, 60L).hashCode());
        Assert.assertEquals(this.newObjectCollectionWith().hashCode(), this.newWith().hashCode());
    }

    @Test
    public void testToString()
    {
        Assert.assertEquals("[]", this.newWith().toString());
        Assert.assertEquals("[1]", this.newWith(1L).toString());
        Assert.assertEquals("[31]", this.newWith(31L).toString());
        Assert.assertEquals("[32]", this.newWith(32L).toString());

        LongIterable iterable = this.newWith(1L, 2L);
        Assert.assertTrue("[1, 2]".equals(iterable.toString())
                || "[2, 1]".equals(iterable.toString()));

        LongIterable iterable1 = this.newWith(0L, 31L);
        Assert.assertTrue(
                iterable1.toString(),
                iterable1.toString().equals("[0, 31]")
                        || iterable1.toString().equals("[31, 0]"));

        LongIterable iterable2 = this.newWith(31L, 32L);
        Assert.assertTrue(
                iterable2.toString(),
                iterable2.toString().equals("[31, 32]")
                        || iterable2.toString().equals("[32, 31]"));

        LongIterable iterable3 = this.newWith(32L, 33L);
        Assert.assertTrue(
                iterable3.toString(),
                iterable3.toString().equals("[32, 33]")
                        || iterable3.toString().equals("[33, 32]"));

        LongIterable iterable4 = this.newWith(0L, 1L);
        Assert.assertTrue(
                iterable4.toString(),
                iterable4.toString().equals("[0, 1]")
                        || iterable4.toString().equals("[1, 0]"));
    }

    @Test
    public void makeString()
    {
        LongIterable iterable = this.classUnderTest();
        Assert.assertEquals("1", this.newWith(1L).makeString("/"));
        Assert.assertEquals("31", this.newWith(31L).makeString());
        Assert.assertEquals("32", this.newWith(32L).makeString());
        Assert.assertEquals(iterable.toString(), iterable.makeString("[", ", ", "]"));
        Assert.assertEquals("", this.newWith().makeString());
        Assert.assertEquals("", this.newWith().makeString("/"));
        Assert.assertEquals("[]", this.newWith().makeString("[", ", ", "]"));

        LongIterable iterable1 = this.newWith(0L, 31L);
        Assert.assertTrue(
                iterable1.makeString(),
                iterable1.makeString().equals("0, 31")
                        || iterable1.makeString().equals("31, 0"));

        LongIterable iterable2 = this.newWith(31L, 32L);
        Assert.assertTrue(
                iterable2.makeString("[", "/", "]"),
                iterable2.makeString("[", "/", "]").equals("[31/32]")
                        || iterable2.makeString("[", "/", "]").equals("[32/31]"));

        LongIterable iterable3 = this.newWith(32L, 33L);
        Assert.assertTrue(
                iterable3.makeString("/"),
                iterable3.makeString("/").equals("32/33")
                        || iterable3.makeString("/").equals("33/32"));

        LongIterable iterable4 = this.newWith(1L, 2L);
        Assert.assertTrue("1, 2".equals(iterable4.makeString())
                || "2, 1".equals(iterable4.makeString()));
        Assert.assertTrue("1/2".equals(iterable4.makeString("/"))
                || "2/1".equals(iterable4.makeString("/")));
        Assert.assertTrue("[1/2]".equals(iterable4.makeString("[", "/", "]"))
                || "[2/1]".equals(iterable4.makeString("[", "/", "]")));

        LongIterable iterable5 = this.newWith(0L, 1L);
        Assert.assertTrue(
                iterable5.makeString(),
                iterable5.makeString().equals("0, 1")
                        || iterable5.makeString().equals("1, 0"));
        Assert.assertTrue(
                iterable5.makeString("[", "/", "]"),
                iterable5.makeString("[", "/", "]").equals("[0/1]")
                        || iterable5.makeString("[", "/", "]").equals("[1/0]"));
        Assert.assertTrue(
                iterable5.makeString("/"),
                iterable5.makeString("/").equals("0/1")
                        || iterable5.makeString("/").equals("1/0"));
    }

    @Test
    public void appendString()
    {
        StringBuilder appendable = new StringBuilder();
        this.newWith().appendString(appendable);
        Assert.assertEquals("", appendable.toString());
        this.newWith().appendString(appendable, "/");
        Assert.assertEquals("", appendable.toString());
        this.newWith().appendString(appendable, "[", ", ", "]");
        Assert.assertEquals("[]", appendable.toString());
        StringBuilder appendable1 = new StringBuilder();
        this.newWith(1L).appendString(appendable1);
        Assert.assertEquals("1", appendable1.toString());
        StringBuilder appendable2 = new StringBuilder();

        LongIterable iterable = this.newWith(1L, 2L);
        iterable.appendString(appendable2);
        Assert.assertTrue("1, 2".equals(appendable2.toString())
                || "2, 1".equals(appendable2.toString()));
        StringBuilder appendable3 = new StringBuilder();
        iterable.appendString(appendable3, "/");
        Assert.assertTrue("1/2".equals(appendable3.toString())
                || "2/1".equals(appendable3.toString()));
        StringBuilder appendable4 = new StringBuilder();
        iterable.appendString(appendable4, "[", ", ", "]");
        Assert.assertEquals(iterable.toString(), appendable4.toString());

        StringBuilder appendable5 = new StringBuilder();
        this.newWith(31L).appendString(appendable5);
        Assert.assertEquals("31", appendable5.toString());

        StringBuilder appendable6 = new StringBuilder();
        this.newWith(32L).appendString(appendable6);
        Assert.assertEquals("32", appendable6.toString());

        StringBuilder appendable7 = new StringBuilder();
        LongIterable iterable1 = this.newWith(0L, 31L);
        iterable1.appendString(appendable7);
        Assert.assertTrue(appendable7.toString(), "0, 31".equals(appendable7.toString())
                || "31, 0".equals(appendable7.toString()));

        StringBuilder appendable8 = new StringBuilder();
        LongIterable iterable2 = this.newWith(31L, 32L);
        iterable2.appendString(appendable8, "/");
        Assert.assertTrue(appendable8.toString(), "31/32".equals(appendable8.toString())
                || "32/31".equals(appendable8.toString()));

        StringBuilder appendable9 = new StringBuilder();
        LongIterable iterable4 = this.newWith(32L, 33L);
        iterable4.appendString(appendable9, "[", "/", "]");
        Assert.assertTrue(appendable9.toString(), "[32/33]".equals(appendable9.toString())
                || "[33/32]".equals(appendable9.toString()));

        StringBuilder appendable10 = new StringBuilder();
        LongIterable iterable5 = this.newWith(0L, 1L);
        iterable5.appendString(appendable10);
        Assert.assertTrue(appendable10.toString(), "0, 1".equals(appendable10.toString())
                || "1, 0".equals(appendable10.toString()));
        StringBuilder appendable11 = new StringBuilder();
        iterable5.appendString(appendable11, "/");
        Assert.assertTrue(appendable11.toString(), "0/1".equals(appendable11.toString())
                || "1/0".equals(appendable11.toString()));
        StringBuilder appendable12 = new StringBuilder();
        iterable5.appendString(appendable12, "[", "/", "]");
        Assert.assertTrue(appendable12.toString(), "[0/1]".equals(appendable12.toString())
                || "[1/0]".equals(appendable12.toString()));
    }

    @Test
    public void toList()
    {
        LongIterable iterable = this.newWith(31L, 32L);
        Assert.assertTrue(LongArrayList.newListWith(31L, 32L).equals(iterable.toList())
                || LongArrayList.newListWith(32L, 31L).equals(iterable.toList()));
        Assert.assertEquals(LongArrayList.newListWith(0L), this.newWith(0L).toList());
        Assert.assertEquals(LongArrayList.newListWith(31L), this.newWith(31L).toList());
        Assert.assertEquals(LongArrayList.newListWith(32L), this.newWith(32L).toList());
        Assert.assertEquals(new LongArrayList(), this.newWith().toList());
    }

    @Test
    public void toSortedList()
    {
        Assert.assertEquals(LongArrayList.newListWith(), this.newWith().toSortedList());
        Assert.assertEquals(LongArrayList.newListWith(1L), this.newWith(1L).toSortedList());
        Assert.assertEquals(LongArrayList.newListWith(0L, 1L, 31L), this.newWith(0L, 31L, 1L).toSortedList());
        Assert.assertEquals(LongArrayList.newListWith(0L, 1L, 31L, 32L), this.newWith(0L, 31L, 32L, 1L).toSortedList());
    }

    @Test
    public void toSet()
    {
        Assert.assertEquals(LongHashSet.newSetWith(), this.newWith().toSet());
        Assert.assertEquals(LongHashSet.newSetWith(1L), this.newWith(1L).toSet());
        Assert.assertEquals(LongHashSet.newSetWith(1L, 2L, 3L), this.newWith(1L, 2L, 3L).toSet());
        Assert.assertEquals(LongHashSet.newSetWith(0L, 1L, 31L), this.newWith(0L, 1L, 31L).toSet());
        Assert.assertEquals(LongHashSet.newSetWith(0L, 1L, 31L, 32L), this.newWith(0L, 1L, 31L, 32L).toSet());
        Assert.assertEquals(LongHashSet.newSetWith(1L, 2L, 3L), this.newWith(1L, 2L, 2L, 3L, 3L, 3L).toSet());
    }

    @Test
    public void toBag()
    {
        Assert.assertEquals(new LongHashBag(), this.newWith().toBag());
        Assert.assertEquals(LongHashBag.newBagWith(1L), this.newWith(1L).toBag());
        Assert.assertEquals(LongHashBag.newBagWith(1L, 2L, 3L), this.newWith(1L, 2L, 3L).toBag());
        Assert.assertEquals(LongHashBag.newBagWith(1L, 2L, 2L, 3L, 3L, 3L), this.newWith(1L, 2L, 2L, 3L, 3L, 3L).toBag());
        Assert.assertEquals(LongHashBag.newBagWith(0L, 1L, 31L, 32L), this.newWith(0L, 1L, 31L, 32L).toBag());
    }

    @Test
    public void asLazy()
    {
        LongIterable iterable = this.classUnderTest();
        Assert.assertEquals(iterable.toBag(), iterable.asLazy().toBag());
        Verify.assertInstanceOf(LazyLongIterable.class, iterable.asLazy());

        LongIterable iterable1 = this.newWith(1L, 2L, 2L, 3L, 3L, 3L);
        Assert.assertEquals(iterable1.toBag(), iterable1.asLazy().toBag());
        Verify.assertInstanceOf(LazyLongIterable.class, iterable1.asLazy());

        LongIterable iterable2 = this.newWith(1L, 2L, 2L, 3L, 3L, 3L);
        Assert.assertEquals(iterable2.toBag(), iterable2.asLazy().toBag());
        Verify.assertInstanceOf(LazyLongIterable.class, iterable2.asLazy());

        LongIterable iterable3 = this.newWith();
        Assert.assertEquals(iterable3.toBag(), iterable3.asLazy().toBag());
        Verify.assertInstanceOf(LazyLongIterable.class, iterable3.asLazy());

        LongIterable iterable4 = this.newWith(1L);
        Assert.assertEquals(iterable4.toBag(), iterable4.asLazy().toBag());
        Verify.assertInstanceOf(LazyLongIterable.class, iterable4.asLazy());
    }

    @Test
    public void injectInto()
    {
        LongIterable iterable1 = this.newWith(0L, 2L, 31L);
        Long sum1 = iterable1.injectInto(Long.valueOf(0L), (Long result, long value) -> Long.valueOf((long) (result + value + 1)));
        Assert.assertEquals(Long.valueOf(36L), sum1);

        LongIterable iterable2 = this.newWith(1L, 2L, 31L);
        Long sum2 = iterable2.injectInto(Long.valueOf(0L), (Long result, long value) -> Long.valueOf((long) (result + value + 1)));
        Assert.assertEquals(Long.valueOf(37L), sum2);

        LongIterable iterable3 = this.newWith(0L, 1L, 2L, 31L);
        Long sum3 = iterable3.injectInto(Long.valueOf(0L), (Long result, long value) -> Long.valueOf((long) (result + value + 1)));
        Assert.assertEquals(Long.valueOf(38L), sum3);
    }
}
