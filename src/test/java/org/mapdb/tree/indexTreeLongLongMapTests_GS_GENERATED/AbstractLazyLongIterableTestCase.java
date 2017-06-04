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
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.impl.bag.mutable.primitive.LongHashBag;
import org.eclipse.collections.impl.block.factory.primitive.LongPredicates;
import org.eclipse.collections.impl.factory.primitive.*;
import org.eclipse.collections.impl.lazy.primitive.LazyLongIterableAdapter;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.eclipse.collections.impl.test.Verify;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.NoSuchElementException;

/**
 * Abstract JUnit test for {@link LazyLongIterable}.
 * This file was automatically generated from template file abstractLazyPrimitiveIterableTestCase.stg.
 */
public abstract class AbstractLazyLongIterableTestCase
{
    protected abstract LazyLongIterable classUnderTest();

    protected abstract LazyLongIterable getEmptyIterable();

    protected abstract LazyLongIterable newWith(long element1, long element2);

    @Test
    public void longIterator()
    {
        long sum = 0L;
        for (LongIterator iterator = this.classUnderTest().longIterator(); iterator.hasNext(); )
        {
            sum += iterator.next();
        }
        Assert.assertEquals(6L, sum);
    }

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

    @Test
    public void forEach()
    {
        long[] sum = new long[1];
        this.classUnderTest().forEach(each -> sum[0] += each);
        Assert.assertEquals(6L, sum[0]);
    }

    @Test
    public void size()
    {
        Verify.assertSize(3, this.classUnderTest());
    }

    @Test
    public void isEmpty()
    {
        Verify.assertEmpty(this.getEmptyIterable());
        Verify.assertNotEmpty(this.classUnderTest());
    }

    @Test
    public void notEmpty()
    {
        Assert.assertFalse(this.getEmptyIterable().notEmpty());
        Assert.assertTrue(this.classUnderTest().notEmpty());
    }

    @Test
    public void count()
    {
        Assert.assertEquals(1L, this.classUnderTest().count(LongPredicates.lessThan(2L)));
        Assert.assertEquals(0L, this.classUnderTest().count(LongPredicates.lessThan(0L)));
        Assert.assertEquals(2L, this.newWith(0L, 1L).count(LongPredicates.lessThan(2L)));
        Assert.assertEquals(2L, this.newWith(32L, 33L).count(LongPredicates.lessThan(34L)));
        Assert.assertEquals(0L, this.newWith(32L, 33L).count(LongPredicates.lessThan(0L)));
    }

    @Test
    public void anySatisfy()
    {
        Assert.assertTrue(this.classUnderTest().anySatisfy(LongPredicates.lessThan(2L)));
        Assert.assertFalse(this.classUnderTest().anySatisfy(LongPredicates.greaterThan(4L)));
        Assert.assertTrue(this.newWith(0L, 1L).anySatisfy(LongPredicates.lessThan(2L)));
        Assert.assertFalse(this.newWith(0L, 1L).anySatisfy(LongPredicates.lessThan(0L)));
        Assert.assertFalse(this.newWith(32L, 33L).anySatisfy(LongPredicates.lessThan(0L)));
        Assert.assertTrue(this.newWith(32L, 33L).anySatisfy(LongPredicates.lessThan(33L)));
    }

    @Test
    public void allSatisfy()
    {
        Assert.assertTrue(this.classUnderTest().allSatisfy(LongPredicates.greaterThan(0L)));
        Assert.assertFalse(this.classUnderTest().allSatisfy(LongPredicates.lessThan(2L)));
        Assert.assertFalse(this.classUnderTest().allSatisfy(LongPredicates.lessThan(1L)));
        Assert.assertTrue(this.classUnderTest().allSatisfy(LongPredicates.lessThan(4L)));
        Assert.assertTrue(this.newWith(0L, 1L).allSatisfy(LongPredicates.lessThan(2L)));
        Assert.assertFalse(this.newWith(0L, 1L).allSatisfy(LongPredicates.lessThan(1L)));
        Assert.assertFalse(this.newWith(0L, 1L).allSatisfy(LongPredicates.lessThan(0L)));
        Assert.assertFalse(this.newWith(32L, 33L).allSatisfy(LongPredicates.lessThan(1L)));
        Assert.assertFalse(this.newWith(32L, 33L).allSatisfy(LongPredicates.lessThan(33L)));
        Assert.assertTrue(this.newWith(32L, 33L).allSatisfy(LongPredicates.lessThan(34L)));
    }

    @Test
    public void noneSatisfy()
    {
        Assert.assertTrue(this.classUnderTest().noneSatisfy(LongPredicates.lessThan(0L)));
        Assert.assertFalse(this.classUnderTest().noneSatisfy(LongPredicates.lessThan(2L)));
        Assert.assertTrue(this.classUnderTest().noneSatisfy(LongPredicates.lessThan(1L)));
        Assert.assertTrue(this.classUnderTest().noneSatisfy(LongPredicates.greaterThan(4L)));
        Assert.assertFalse(this.newWith(0L, 1L).noneSatisfy(LongPredicates.lessThan(2L)));
        Assert.assertTrue(this.newWith(0L, 1L).noneSatisfy(LongPredicates.lessThan(0L)));
        Assert.assertTrue(this.newWith(32L, 33L).noneSatisfy(LongPredicates.lessThan(0L)));
        Assert.assertFalse(this.newWith(32L, 33L).noneSatisfy(LongPredicates.lessThan(33L)));
    }

    @Test
    public void select()
    {
        Verify.assertSize(2, this.classUnderTest().select(LongPredicates.greaterThan(1L)));
        Verify.assertEmpty(this.classUnderTest().select(LongPredicates.lessThan(0L)));
        Verify.assertSize(2, this.newWith(0L, 1L).select(LongPredicates.lessThan(2L)));
        Verify.assertEmpty(this.newWith(32L, 33L).select(LongPredicates.lessThan(2L)));
        Verify.assertSize(2, this.newWith(32L, 33L).select(LongPredicates.lessThan(34L)));
    }

    @Test
    public void reject()
    {
        Verify.assertSize(1, this.classUnderTest().reject(LongPredicates.greaterThan(1L)));
        Verify.assertEmpty(this.classUnderTest().reject(LongPredicates.greaterThan(0L)));
        Verify.assertEmpty(this.newWith(0L, 1L).reject(LongPredicates.lessThan(2L)));
        Verify.assertEmpty(this.newWith(32L, 33L).reject(LongPredicates.lessThan(34L)));
        Verify.assertSize(2, this.newWith(32L, 33L).reject(LongPredicates.lessThan(2L)));
    }

    @Test
    public void detectIfNone()
    {
        Assert.assertEquals(1L, this.classUnderTest().detectIfNone(LongPredicates.lessThan(4L), 0L));
        Assert.assertEquals(0L, this.classUnderTest().detectIfNone(LongPredicates.greaterThan(3L), 0L));
        Assert.assertEquals(0L, this.newWith(0L, 1L).detectIfNone(LongPredicates.lessThan(2L), 1L));
        Assert.assertEquals(33L, this.newWith(32L, 33L).detectIfNone(LongPredicates.equal(33L), 1L));
        Assert.assertEquals(32L, this.newWith(0L, 1L).detectIfNone(LongPredicates.equal(33L), 32L));
        Assert.assertEquals(32L, this.newWith(34L, 35L).detectIfNone(LongPredicates.equal(33L), 32L));
    }

    @Test
    public void collect()
    {
        Verify.assertIterableSize(3, this.classUnderTest().collect(String::valueOf));
    }

    @Test
    public void lazyCollectPrimitives()
    {
        Assert.assertEquals(BooleanLists.immutable.of(false, true, false), this.classUnderTest().collectBoolean(e -> e % 2 == 0).toList());
        Assert.assertEquals(CharLists.immutable.of((char) 2, (char) 3, (char) 4), this.classUnderTest().asLazy().collectChar(e -> (char) (e + 1)).toList());
        Assert.assertEquals(ByteLists.immutable.of((byte) 2, (byte) 3, (byte) 4), this.classUnderTest().asLazy().collectByte(e -> (byte) (e + 1)).toList());
        Assert.assertEquals(ShortLists.immutable.of((short) 2, (short) 3, (short) 4), this.classUnderTest().asLazy().collectShort(e -> (short) (e + 1)).toList());
        Assert.assertEquals(IntLists.immutable.of(2, 3, 4), this.classUnderTest().asLazy().collectInt(e -> (int) (e + 1)).toList());
        Assert.assertEquals(FloatLists.immutable.of(2.0f, 3.0f, 4.0f), this.classUnderTest().asLazy().collectFloat(e -> (float) (e + 1)).toList());
        Assert.assertEquals(LongLists.immutable.of(2L, 3L, 4L), this.classUnderTest().asLazy().collectLong(e -> (long) (e + 1)).toList());
        Assert.assertEquals(DoubleLists.immutable.of(2.0, 3.0, 4.0), this.classUnderTest().asLazy().collectDouble(e -> (double) (e + 1)).toList());
    }

    @Test
    public void sum()
    {
        Assert.assertEquals(6L, this.classUnderTest().sum());
        Assert.assertEquals(1L, this.newWith(0L, 1L).sum());
        Assert.assertEquals(33L, this.newWith(0L, 33L).sum());
    }

    @Test(expected = NoSuchElementException.class)
    public void max_throws_emptyIterable()
    {
        this.getEmptyIterable().max();
    }

    @Test(expected = NoSuchElementException.class)
    public void min_throws_emptyIterable()
    {
        this.getEmptyIterable().min();
    }

    @Test
    public void max()
    {
        Assert.assertEquals(3L, this.classUnderTest().max());
        Assert.assertEquals(33L, this.newWith(33L, 0L).max());
        Assert.assertEquals(100L, this.newWith(100L, 1L).max());
        Assert.assertEquals(2L, this.newWith(1L, 2L).max());
    }

    @Test
    public void min()
    {
        Assert.assertEquals(1L, this.classUnderTest().min());
        Assert.assertEquals(0L, this.newWith(33L, 0L).min());
        Assert.assertEquals(1L, this.newWith(100L, 1L).min());
        Assert.assertEquals(1L, this.newWith(2L, 1L).min());
    }

    @Test
    public void minIfEmpty()
    {
        Assert.assertEquals(5L, this.getEmptyIterable().minIfEmpty(5L));
        Assert.assertEquals(1L, this.classUnderTest().minIfEmpty(0L));
        Assert.assertEquals(
                0L,
                this.classUnderTest().select(LongPredicates.lessThan(0L)).minIfEmpty(0L));
    }

    @Test
    public void maxIfEmpty()
    {
        Assert.assertEquals(5L, this.getEmptyIterable().maxIfEmpty(5L));
        Assert.assertEquals(3L, this.classUnderTest().maxIfEmpty(0L));
        Assert.assertEquals(
                0L,
                this.classUnderTest().select(LongPredicates.lessThan(0L)).maxIfEmpty(0L));
    }

    @Test(expected = NoSuchElementException.class)
    public void maxThrowsOnEmpty()
    {
        new LazyLongIterableAdapter(new LongArrayList()).max();
    }

    @Test(expected = NoSuchElementException.class)
    public void minThrowsOnEmpty()
    {
        new LazyLongIterableAdapter(new LongArrayList()).min();
    }

    @Test
    public void average()
    {
        Assert.assertEquals(2.0d, this.classUnderTest().average(), 0.0);
    }

    @Test(expected = ArithmeticException.class)
    public void averageThrowsOnEmpty()
    {
        this.getEmptyIterable().average();
    }

    @Test
    public void median()
    {
        Assert.assertEquals(2.0d, this.classUnderTest().median(), 0.0);
        Assert.assertEquals(16.0d, this.newWith(1L, 31L).median(), 0.0);
    }

    @Test(expected = ArithmeticException.class)
    public void medianThrowsOnEmpty()
    {
        this.getEmptyIterable().median();
    }

    @Test
    public void toArray()
    {
        Assert.assertTrue(Arrays.equals(new long[]{0L, 1L}, this.newWith(0L, 1L).toArray())
                || Arrays.equals(new long[]{1L, 0L}, this.newWith(0L, 1L).toArray()));
        Assert.assertTrue(Arrays.equals(new long[]{1L, 31L}, this.newWith(1L, 31L).toArray())
                || Arrays.equals(new long[]{31L, 1L}, this.newWith(1L, 31L).toArray()));
        Assert.assertTrue(Arrays.equals(new long[]{31L, 35L}, this.newWith(31L, 35L).toArray())
                || Arrays.equals(new long[]{35L, 31L}, this.newWith(31L, 35L).toArray()));
    }

    @Test
    public void contains()
    {
        Assert.assertTrue(this.classUnderTest().contains(1L));
        Assert.assertTrue(this.classUnderTest().contains(2L));
        Assert.assertTrue(this.classUnderTest().contains(3L));
        Assert.assertFalse(this.classUnderTest().contains(4L));
    }

    @Test
    public void containsAllArray()
    {
        Assert.assertTrue(this.classUnderTest().containsAll(1L));
        Assert.assertTrue(this.classUnderTest().containsAll(2L));
        Assert.assertTrue(this.classUnderTest().containsAll(1L, 2L));
        Assert.assertTrue(this.classUnderTest().containsAll(1L, 2L, 3L));
        Assert.assertFalse(this.classUnderTest().containsAll(1L, 2L, 3L, 4L));
        Assert.assertFalse(this.classUnderTest().containsAll(4L, 5L, 6L));
    }

    @Test
    public void containsAllIterable()
    {
        Assert.assertTrue(this.classUnderTest().containsAll(LongArrayList.newListWith(1L)));
        Assert.assertTrue(this.classUnderTest().containsAll(LongArrayList.newListWith(2L)));
        Assert.assertTrue(this.classUnderTest().containsAll(LongArrayList.newListWith(1L, 2L)));
        Assert.assertTrue(this.classUnderTest().containsAll(LongArrayList.newListWith(1L, 2L, 3L)));
        Assert.assertFalse(this.classUnderTest().containsAll(LongArrayList.newListWith(1L, 2L, 3L, 4L)));
        Assert.assertFalse(this.classUnderTest().containsAll(LongArrayList.newListWith(4L, 5L, 6L)));
    }

    @Test
    public void testToString()
    {
        LazyLongIterable iterable = this.newWith(1L, 2L);
        Assert.assertTrue("[1, 2]".equals(iterable.toString())
                || "[2, 1]".equals(iterable.toString()));

        LazyLongIterable iterable1 = this.newWith(0L, 31L);
        Assert.assertTrue(
                iterable1.toString(),
                iterable1.toString().equals("[0, 31]")
                        || iterable1.toString().equals("[31, 0]"));

        LazyLongIterable iterable2 = this.newWith(31L, 32L);
        Assert.assertTrue(
                iterable2.toString(),
                iterable2.toString().equals("[31, 32]")
                        || iterable2.toString().equals("[32, 31]"));

        LazyLongIterable iterable3 = this.newWith(32L, 33L);
        Assert.assertTrue(
                iterable3.toString(),
                iterable3.toString().equals("[32, 33]")
                        || iterable3.toString().equals("[33, 32]"));

        LazyLongIterable iterable4 = this.newWith(0L, 1L);
        Assert.assertTrue(
                iterable4.toString(),
                iterable4.toString().equals("[0, 1]")
                        || iterable4.toString().equals("[1, 0]"));
    }

    @Test
    public void makeString()
    {
        LazyLongIterable iterable1 = this.newWith(0L, 31L);
        Assert.assertTrue(
                iterable1.makeString(),
                iterable1.makeString().equals("0, 31")
                        || iterable1.makeString().equals("31, 0"));

        LazyLongIterable iterable2 = this.newWith(31L, 32L);
        Assert.assertTrue(
                iterable2.makeString("[", "/", "]"),
                iterable2.makeString("[", "/", "]").equals("[31/32]")
                        || iterable2.makeString("[", "/", "]").equals("[32/31]"));

        LazyLongIterable iterable3 = this.newWith(32L, 33L);
        Assert.assertTrue(
                iterable3.makeString("/"),
                iterable3.makeString("/").equals("32/33")
                        || iterable3.makeString("/").equals("33/32"));

        LazyLongIterable iterable4 = this.newWith(1L, 2L);
        Assert.assertTrue("1, 2".equals(iterable4.makeString())
                || "2, 1".equals(iterable4.makeString()));
        Assert.assertTrue("1/2".equals(iterable4.makeString("/"))
                || "2/1".equals(iterable4.makeString("/")));
        Assert.assertTrue("[1/2]".equals(iterable4.makeString("[", "/", "]"))
                || "[2/1]".equals(iterable4.makeString("[", "/", "]")));

        LazyLongIterable iterable5 = this.newWith(0L, 1L);
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
        StringBuilder appendable2 = new StringBuilder();
        LazyLongIterable iterable = this.newWith(1L, 2L);
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

        StringBuilder appendable7 = new StringBuilder();
        LazyLongIterable iterable1 = this.newWith(0L, 31L);
        iterable1.appendString(appendable7);
        Assert.assertTrue(appendable7.toString(), "0, 31".equals(appendable7.toString())
                || "31, 0".equals(appendable7.toString()));

        StringBuilder appendable8 = new StringBuilder();
        LazyLongIterable iterable2 = this.newWith(31L, 32L);
        iterable2.appendString(appendable8, "/");
        Assert.assertTrue(appendable8.toString(), "31/32".equals(appendable8.toString())
                || "32/31".equals(appendable8.toString()));

        StringBuilder appendable9 = new StringBuilder();
        LazyLongIterable iterable4 = this.newWith(32L, 33L);
        iterable4.appendString(appendable9, "[", "/", "]");
        Assert.assertTrue(appendable9.toString(), "[32/33]".equals(appendable9.toString())
                || "[33/32]".equals(appendable9.toString()));

        StringBuilder appendable10 = new StringBuilder();
        LazyLongIterable iterable5 = this.newWith(0L, 1L);
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
        LazyLongIterable iterable = this.newWith(31L, 32L);
        Assert.assertTrue(LongArrayList.newListWith(31L, 32L).equals(iterable.toList())
                || LongArrayList.newListWith(32L, 31L).equals(iterable.toList()));
    }

    @Test
    public void toSortedArray()
    {
        Assert.assertArrayEquals(new long[]{1, 2, 3}, this.classUnderTest().toSortedArray());
    }

    @Test
    public void toSortedList()
    {
        Assert.assertEquals(LongArrayList.newListWith(1L, 2L, 3L), this.classUnderTest().toSortedList());
    }

    @Test
    public void toSet()
    {
        Assert.assertEquals(LongHashSet.newSetWith(1L, 2L, 3L), this.classUnderTest().toSet());
    }

    @Test
    public void toBag()
    {
        Assert.assertEquals(LongHashBag.newBagWith(1L, 2L, 3L), this.classUnderTest().toBag());
    }

    @Test
    public void asLazy()
    {
        LazyLongIterable iterable = this.classUnderTest();
        Assert.assertEquals(iterable.toSet(), iterable.asLazy().toSet());
        Verify.assertInstanceOf(LazyLongIterable.class, iterable.asLazy());
        Assert.assertSame(iterable, iterable.asLazy());
    }
}
