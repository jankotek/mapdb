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

import org.eclipse.collections.api.LongIterable;
import org.eclipse.collections.api.block.function.primitive.LongToObjectFunction;
import org.eclipse.collections.api.collection.primitive.MutableLongCollection;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.map.primitive.MutableLongLongMap;
import org.eclipse.collections.impl.block.factory.primitive.LongPredicates;
import org.eclipse.collections.impl.collection.mutable.primitive.*;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;
import org.eclipse.collections.impl.test.Verify;
import org.junit.*;

import java.util.NoSuchElementException;

/**
 * JUnit test for {@link MutableLongLongMap#values()}.
 * This file was automatically generated from template file primitivePrimitiveHashMapValuesTest.stg.
 */
public abstract class LongLongHashMapValuesTest extends AbstractMutableLongCollectionTestCase
{
//    @Override
//    protected MutableLongCollection classUnderTest()
//    {
//        return MutableLongLongMap.newWithKeysValues(1L, 1L, 2L, 2L, 3L, 3L).values();
//    }
//
//    @Override
//    protected MutableLongCollection newWith(long... elements)
//    {
//        MutableLongLongMap map = new MutableLongLongMap();
//        for (int i = 0; i < elements.length; i++)
//        {
//            map.put(i, elements[i]);
//        }
//        return map.values();
//    }
//
    @Override
    protected MutableLongCollection newMutableCollectionWith(long... elements)
    {
        return this.newWith(elements);
    }

    @Override
    protected MutableList<Long> newObjectCollectionWith(Long... elements)
    {
        return FastList.newListWith(elements);
    }

    @Override
    @Test
    public void longIterator()
    {
        MutableLongCollection bag = this.newWith(0L, 1L, 2L, 3L);
        LongArrayList list = LongArrayList.newListWith(0L, 1L, 2L, 3L);
        LongIterator iterator = bag.longIterator();
        for (int i = 0; i < 4; i++)
        {
            Assert.assertTrue(iterator.hasNext());
            Assert.assertTrue(list.remove(iterator.next()));
        }
        Verify.assertEmpty(list);
        Assert.assertFalse(iterator.hasNext());

        Verify.assertThrows(NoSuchElementException.class, iterator::next);
    }

    @Override
    @Test(expected = UnsupportedOperationException.class)
    public void addAllIterable()
    {
        this.classUnderTest().addAll(new LongArrayList());
    }

    @Override
    @Test(expected = UnsupportedOperationException.class)
    public void add()
    {
        this.classUnderTest().add(0L);
    }


    @Override
    @Test(expected = UnsupportedOperationException.class)
    public void addAllArray()
    {
        this.classUnderTest().addAll(0L, 1L);
    }

    @Override
    @Test(expected = UnsupportedOperationException.class)
    public void with()
    {
        this.classUnderTest().with(0L);
    }

    @Override
    @Test(expected = UnsupportedOperationException.class)
    public void without()
    {
        this.classUnderTest().without(0L);
    }

    @Override
    @Test(expected = UnsupportedOperationException.class)
    public void withAll()
    {
        this.classUnderTest().withAll(new LongArrayList());
    }

    @Override
    @Test(expected = UnsupportedOperationException.class)
    public void withoutAll()
    {
        this.classUnderTest().withoutAll(new LongArrayList());
    }

    @Override
    @Test
    public void remove()
    {
        MutableLongLongMap map =  newWithKeysValues(1L, 1L, 2L, 2L, 3L, 3L);
        MutableLongCollection collection = map.values();
        Assert.assertTrue(collection.remove(3L));
        Assert.assertFalse(collection.contains(3L));
        Assert.assertTrue(collection.contains(1L));
        Assert.assertTrue(collection.contains(2L));
        Assert.assertFalse(map.contains(3L));
        Assert.assertTrue(map.contains(1L));
        Assert.assertTrue(map.contains(2L));
    }

    protected abstract MutableLongLongMap newWithKeysValues(long... args);

    @Override
    @Test
    public void asSynchronized()
    {
        MutableLongCollection collection = this.classUnderTest();
        Verify.assertInstanceOf(SynchronizedLongCollection.class, collection.asSynchronized());
        Assert.assertTrue(collection.asSynchronized().containsAll(this.classUnderTest()));
    }

    @Override
    @Test
    public void asUnmodifiable()
    {
        MutableLongCollection collection = this.classUnderTest();
        Verify.assertInstanceOf(UnmodifiableLongCollection.class, collection.asUnmodifiable());
        Assert.assertTrue(collection.asUnmodifiable().containsAll(this.classUnderTest()));
    }

    @Override
    @Test
    public void removeAll()
    {
        Assert.assertFalse(this.newWith().removeAll());
        Assert.assertFalse(this.newWith().removeAll(1L));

        MutableLongLongMap map = newWithKeysValues(1L, 1L, 2L, 2L, 3L, 3L);
        MutableLongCollection collection = map.values();
        Assert.assertFalse(collection.removeAll());

        Assert.assertTrue(collection.removeAll(1L, 5L));
        Assert.assertFalse(collection.contains(1L));
        Assert.assertTrue(collection.contains(2L));
        Assert.assertTrue(collection.contains(3L));
        Assert.assertFalse(map.contains(1L));
        Assert.assertTrue(map.contains(2L));
        Assert.assertTrue(map.contains(3L));

        Assert.assertTrue(collection.removeAll(3L, 2L));
        Assert.assertTrue(collection.isEmpty());
        Assert.assertFalse(collection.contains(1L));
        Assert.assertFalse(collection.contains(2L));
        Assert.assertFalse(collection.contains(3L));
        Assert.assertFalse(map.contains(1L));
        Assert.assertFalse(map.contains(2L));
        Assert.assertFalse(map.contains(3L));
        Assert.assertTrue(map.isEmpty());
    }

    @Override
    @Test
    public void removeAll_iterable()
    {
        Assert.assertFalse(this.newWith().removeAll(new LongArrayList()));
        Assert.assertFalse(this.newWith().removeAll(LongArrayList.newListWith(1L)));

        MutableLongLongMap map = newWithKeysValues(1L, 1L, 2L, 2L, 3L, 3L);
        MutableLongCollection collection = map.values();
        Assert.assertFalse(collection.removeAll());

        Assert.assertTrue(collection.removeAll(LongArrayList.newListWith(1L, 5L)));
        Assert.assertFalse(collection.contains(1L));
        Assert.assertTrue(collection.contains(2L));
        Assert.assertTrue(collection.contains(3L));
        Assert.assertFalse(map.contains(1L));
        Assert.assertTrue(map.contains(2L));
        Assert.assertTrue(map.contains(3L));

        Assert.assertTrue(collection.removeAll(LongArrayList.newListWith(3L, 2L)));
        Assert.assertTrue(collection.isEmpty());
        Assert.assertFalse(collection.contains(1L));
        Assert.assertFalse(collection.contains(2L));
        Assert.assertFalse(collection.contains(3L));
        Assert.assertFalse(map.contains(1L));
        Assert.assertFalse(map.contains(2L));
        Assert.assertFalse(map.contains(3L));
        Assert.assertTrue(map.isEmpty());
    }

    @Override
    @Test
    public void retainAll()
    {
        Assert.assertFalse(this.newWith().retainAll());
        Assert.assertFalse(this.newWith().retainAll(1L));

        MutableLongLongMap map = newWithKeysValues(0L, 0L, 1L, 1L, 2L, 2L, 3L, 3L);
        MutableLongCollection collection = map.values();
        Assert.assertFalse(collection.retainAll(0L, 1L, 2L, 3L));

        Assert.assertTrue(collection.retainAll(0L, 2L, 3L, 5L));
        Assert.assertTrue(collection.contains(0L));
        Assert.assertFalse(collection.contains(1L));
        Assert.assertTrue(collection.contains(2L));
        Assert.assertTrue(collection.contains(3L));
        Assert.assertFalse(collection.contains(5L));
        Assert.assertTrue(map.contains(0L));
        Assert.assertFalse(map.contains(1L));
        Assert.assertTrue(map.contains(2L));
        Assert.assertTrue(map.contains(3L));
        Assert.assertFalse(map.contains(5L));

        Assert.assertTrue(collection.retainAll(2L, 3L, 5L));
        Assert.assertFalse(collection.contains(0L));
        Assert.assertFalse(collection.contains(1L));
        Assert.assertTrue(collection.contains(2L));
        Assert.assertTrue(collection.contains(3L));
        Assert.assertFalse(collection.contains(5L));
        Assert.assertFalse(map.contains(0L));
        Assert.assertFalse(map.contains(1L));
        Assert.assertTrue(map.contains(2L));
        Assert.assertTrue(map.contains(3L));
        Assert.assertFalse(map.contains(5L));

        Assert.assertTrue(collection.retainAll(3L, 5L));
        Assert.assertFalse(collection.contains(0L));
        Assert.assertFalse(collection.contains(1L));
        Assert.assertFalse(collection.contains(2L));
        Assert.assertTrue(collection.contains(3L));
        Assert.assertFalse(collection.contains(5L));
        Assert.assertFalse(map.contains(0L));
        Assert.assertFalse(map.contains(1L));
        Assert.assertFalse(map.contains(2L));
        Assert.assertTrue(map.contains(3L));
        Assert.assertFalse(map.contains(5L));

        Assert.assertTrue(collection.retainAll(0L, 0L, 1L));
        Assert.assertTrue(collection.isEmpty());
        Assert.assertFalse(collection.contains(0L));
        Assert.assertFalse(collection.contains(1L));
        Assert.assertFalse(collection.contains(2L));
        Assert.assertFalse(collection.contains(3L));
        Assert.assertFalse(collection.contains(5L));
        Assert.assertFalse(map.contains(0L));
        Assert.assertFalse(map.contains(1L));
        Assert.assertFalse(map.contains(2L));
        Assert.assertFalse(map.contains(3L));
        Assert.assertFalse(map.contains(5L));
        Assert.assertTrue(map.isEmpty());
    }

    @Override
    @Test
    public void retainAll_iterable()
    {
        Assert.assertFalse(this.newWith().retainAll(new LongArrayList()));
        Assert.assertFalse(this.newWith().retainAll(LongArrayList.newListWith(1L)));

        MutableLongLongMap map = newWithKeysValues(0L, 0L, 1L, 1L, 2L, 2L, 3L, 3L);
        MutableLongCollection collection = map.values();
        Assert.assertFalse(collection.retainAll(LongArrayList.newListWith(0L, 1L, 2L, 3L)));

        Assert.assertTrue(collection.retainAll(LongArrayList.newListWith(0L, 2L, 3L, 5L)));
        Assert.assertTrue(collection.contains(0L));
        Assert.assertFalse(collection.contains(1L));
        Assert.assertTrue(collection.contains(2L));
        Assert.assertTrue(collection.contains(3L));
        Assert.assertFalse(collection.contains(5L));
        Assert.assertTrue(map.contains(0L));
        Assert.assertFalse(map.contains(1L));
        Assert.assertTrue(map.contains(2L));
        Assert.assertTrue(map.contains(3L));
        Assert.assertFalse(map.contains(5L));

        Assert.assertTrue(collection.retainAll(LongArrayList.newListWith(2L, 3L, 5L)));
        Assert.assertFalse(collection.contains(0L));
        Assert.assertFalse(collection.contains(1L));
        Assert.assertTrue(collection.contains(2L));
        Assert.assertTrue(collection.contains(3L));
        Assert.assertFalse(collection.contains(5L));
        Assert.assertFalse(map.contains(0L));
        Assert.assertFalse(map.contains(1L));
        Assert.assertTrue(map.contains(2L));
        Assert.assertTrue(map.contains(3L));
        Assert.assertFalse(map.contains(5L));

        Assert.assertTrue(collection.retainAll(LongArrayList.newListWith(3L, 5L)));
        Assert.assertFalse(collection.contains(0L));
        Assert.assertFalse(collection.contains(1L));
        Assert.assertFalse(collection.contains(2L));
        Assert.assertTrue(collection.contains(3L));
        Assert.assertFalse(collection.contains(5L));
        Assert.assertFalse(map.contains(0L));
        Assert.assertFalse(map.contains(1L));
        Assert.assertFalse(map.contains(2L));
        Assert.assertTrue(map.contains(3L));
        Assert.assertFalse(map.contains(5L));

        Assert.assertTrue(collection.retainAll(LongArrayList.newListWith(0L, 0L, 1L)));
        Assert.assertTrue(collection.isEmpty());
        Assert.assertFalse(collection.contains(0L));
        Assert.assertFalse(collection.contains(1L));
        Assert.assertFalse(collection.contains(2L));
        Assert.assertFalse(collection.contains(3L));
        Assert.assertFalse(collection.contains(5L));
        Assert.assertFalse(map.contains(0L));
        Assert.assertFalse(map.contains(1L));
        Assert.assertFalse(map.contains(2L));
        Assert.assertFalse(map.contains(3L));
        Assert.assertFalse(map.contains(5L));
        Assert.assertTrue(map.isEmpty());
    }

    @Override
    @Test
    public void clear()
    {
        MutableLongCollection emptyCollection = this.newWith();
        emptyCollection.clear();
        Verify.assertSize(0, emptyCollection);

        MutableLongLongMap map = newWithKeysValues(1L, 1L, 2L, 2L, 3L, 3L);
        MutableLongCollection collection = map.values();
        collection.clear();
        Verify.assertEmpty(collection);
        Verify.assertEmpty(map);
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

        MutableLongCollection collection2 = this.newWith(0L, 1L, 2L);
        collection2.clear();
        Verify.assertSize(0, collection2);
    }

    @Override
    @Test
    public void contains()
    {
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

        Assert.assertFalse(collection.contains(29L));
        Assert.assertFalse(collection.contains(49L));
    }

    @Override
    @Test
    public void reject()
    {
        LongIterable iterable = this.classUnderTest();
        Verify.assertSize(0, iterable.reject(LongPredicates.lessThan(4L)));
        Verify.assertSize(1, iterable.reject(LongPredicates.lessThan(3L)));
    }

    @Override
    @Test
    public void select()
    {
        LongIterable iterable = this.classUnderTest();
        Verify.assertSize(3, iterable.select(LongPredicates.lessThan(4L)));
        Verify.assertSize(2, iterable.select(LongPredicates.lessThan(3L)));
    }

    @Override
    @Test
    public void collect()
    {
        LongToObjectFunction<Long> function = (long parameter) -> parameter - 1;
        Assert.assertEquals(this.newObjectCollectionWith(0L, 1L, 2L).toBag(), this.newWith(1L, 2L, 3L).collect(function).toBag());
        LongIterable iterable = this.newWith(1L, 2L, 3L);
        Assert.assertEquals(this.newObjectCollectionWith(0L, 1L, 2L).toBag(), iterable.collect(function).toBag());
        Assert.assertArrayEquals(this.newObjectCollectionWith().toArray(), this.newWith().collect(function).toArray());
        Assert.assertArrayEquals(this.newObjectCollectionWith(2L).toArray(), this.newWith(3L).collect(function).toArray());
    }

    @Override
    @Test
    public void makeString()
    {
        Assert.assertEquals("1", this.newWith(1L).makeString("/"));
        Assert.assertEquals("31", this.newWith(31L).makeString());
        Assert.assertEquals("32", this.newWith(32L).makeString());
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

    @Override
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

    @Override
    @Test
    public void testEquals()
    {
        //Testing equals() is not applicable for MutableLongCollection.
    }

    @Override
    public void testToString()
    {
        //Testing toString() is not applicable for MutableLongCollection.
    }

    @Override
    public void testHashCode()
    {
        //Testing hashCode() is not applicable for MutableLongCollection.
    }

    @Override
    public void newCollection()
    {
        //Testing newCollection() is not applicable for MutableLongCollection.
    }
}
