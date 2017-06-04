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

import org.eclipse.collections.api.*;
import org.eclipse.collections.api.bag.MutableBag;
import org.eclipse.collections.api.bag.sorted.MutableSortedBag;
import org.eclipse.collections.api.block.function.Function;
import org.eclipse.collections.api.block.function.Function0;
import org.eclipse.collections.api.block.function.Function2;
import org.eclipse.collections.api.block.procedure.Procedure2;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.map.MapIterable;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.api.map.primitive.LongLongMap;
import org.eclipse.collections.api.map.sorted.MutableSortedMap;
import org.eclipse.collections.api.multimap.Multimap;
import org.eclipse.collections.api.partition.PartitionIterable;
import org.eclipse.collections.api.set.MutableSet;
import org.eclipse.collections.api.set.sorted.MutableSortedSet;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.api.tuple.primitive.LongLongPair;
import org.eclipse.collections.impl.bag.mutable.HashBag;
import org.eclipse.collections.impl.bag.mutable.primitive.*;
import org.eclipse.collections.impl.bag.sorted.mutable.TreeBag;
import org.eclipse.collections.impl.block.factory.Comparators;
import org.eclipse.collections.impl.block.factory.Functions0;
import org.eclipse.collections.impl.block.factory.Predicates;
import org.eclipse.collections.impl.block.factory.Predicates2;
import org.eclipse.collections.impl.block.procedure.CollectionAddProcedure;
import org.eclipse.collections.impl.factory.Bags;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.list.Interval;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.eclipse.collections.impl.map.sorted.mutable.TreeSortedMap;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;
import org.eclipse.collections.impl.set.sorted.mutable.TreeSortedSet;
import org.eclipse.collections.impl.test.Verify;
import org.eclipse.collections.impl.tuple.Tuples;
import org.eclipse.collections.impl.tuple.primitive.PrimitiveTuples;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Abstract JUnit test for {@link LongLongMap#keyValuesView()}.
 * This file was automatically generated from template file abstractPrimitivePrimitiveMapKeyValuesViewTestCase.stg.
 */
public abstract class AbstractLongLongMapKeyValuesViewTestCase
{
    public abstract LongLongMap newWithKeysValues(long key1, long value1, long key2, long value2, long key3, long value3);

    public abstract LongLongMap newWithKeysValues(long key1, long value1, long key2, long value2);

    public abstract LongLongMap newWithKeysValues(long key1, long value1);

    public abstract LongLongMap newEmpty();

    public RichIterable<LongLongPair> newWith()
    {
        return this.newEmpty().keyValuesView();
    }

    public RichIterable<LongLongPair> newWith(long key1, long value1)
    {
        return this.newWithKeysValues(key1, value1).keyValuesView();
    }

    public RichIterable<LongLongPair> newWith(long key1, long value1, long key2, long value2)
    {
        return this.newWithKeysValues(key1, value1, key2, value2).keyValuesView();
    }

    public RichIterable<LongLongPair> newWith(long key1, long value1, long key2, long value2, long key3, long value3)
    {
        return this.newWithKeysValues(key1, value1, key2, value2, key3, value3).keyValuesView();
    }

    @Test
    public void containsAllIterable()
    {
        RichIterable<LongLongPair> collection = this.newWith(1L, 2L, 2L, 3L, 3L, 4L);
        Assert.assertTrue(collection.containsAllIterable(FastList.newListWith(PrimitiveTuples.pair(1L, 2L), PrimitiveTuples.pair(2L, 3L))));
        Assert.assertFalse(collection.containsAllIterable(FastList.newListWith(PrimitiveTuples.pair(1L, 2L), PrimitiveTuples.pair(1L, 5L))));
    }

    @Test
    public void containsAllArray()
    {
        RichIterable<LongLongPair> collection = this.newWith(1L, 2L, 2L, 3L, 3L, 4L);
        Assert.assertTrue(collection.containsAllArguments(PrimitiveTuples.pair(1L, 2L), PrimitiveTuples.pair(2L, 3L)));
        Assert.assertFalse(collection.containsAllArguments(PrimitiveTuples.pair(1L, 2L), PrimitiveTuples.pair(1L, 5L)));
    }

    @Test
    public void forEach()
    {
        MutableList<LongLongPair> result = Lists.mutable.of();
        RichIterable<LongLongPair> collection = this.newWith(1L, 2L, 2L, 3L, 3L, 4L);
        collection.forEach(CollectionAddProcedure.on(result));
        Verify.assertSize(3, result);
        Verify.assertContainsAll(result, PrimitiveTuples.pair(1L, 2L), PrimitiveTuples.pair(2L, 3L), PrimitiveTuples.pair(3L, 4L));

        MutableList<LongLongPair> result2 = Lists.mutable.of();
        RichIterable<LongLongPair> collection2 = this.newWith(0L, 2L, 2L, 3L, 3L, 4L);
        collection2.forEach(CollectionAddProcedure.on(result2));
        Verify.assertSize(3, result2);
        Verify.assertContainsAll(result2, PrimitiveTuples.pair(0L, 2L), PrimitiveTuples.pair(2L, 3L), PrimitiveTuples.pair(3L, 4L));
    }

    @Test
    public void forEachWith()
    {
        MutableBag<LongLongPair> result = Bags.mutable.of();
        MutableBag<Integer> result2 = Bags.mutable.of();
        RichIterable<LongLongPair> collection = this.newWith(1L, 0L, 2L, 3L, 3L, 4L);
        collection.forEachWith((LongLongPair argument1, Integer argument2) ->
            {
                result.add(argument1);
                result2.add(argument2);
            }, 0);

        Assert.assertEquals(Bags.immutable.of(PrimitiveTuples.pair(1L, 0L), PrimitiveTuples.pair(2L, 3L), PrimitiveTuples.pair(3L, 4L)), result);
        Assert.assertEquals(Bags.immutable.of(0, 0, 0), result2);

        MutableBag<LongLongPair> result3 = Bags.mutable.of();
        MutableBag<Integer> result4 = Bags.mutable.of();
        RichIterable<LongLongPair> collection2 = this.newWith(2L, 5L, 6L, 3L, 3L, 4L);
        collection2.forEachWith((LongLongPair argument1, Integer argument2) ->
            {
                result3.add(argument1);
                result4.add(argument2);
            }, 0);

        Assert.assertEquals(Bags.immutable.of(PrimitiveTuples.pair(2L, 5L), PrimitiveTuples.pair(6L, 3L), PrimitiveTuples.pair(3L, 4L)), result3);
        Assert.assertEquals(Bags.immutable.of(0, 0, 0), result4);
    }

    @Test
    public void forEachWithIndex()
    {
        MutableBag<LongLongPair> elements = Bags.mutable.of();
        MutableBag<Integer> indexes = Bags.mutable.of();
        RichIterable<LongLongPair> collection = this.newWith(2L, 2L, 6L, 3L, 3L, 4L);
        collection.forEachWithIndex((LongLongPair object, int index) ->
            {
                elements.add(object);
                indexes.add(index);
            });
        Assert.assertEquals(Bags.mutable.of(PrimitiveTuples.pair(2L, 2L), PrimitiveTuples.pair(6L, 3L), PrimitiveTuples.pair(3L, 4L)), elements);
        Assert.assertEquals(Bags.mutable.of(0, 1, 2), indexes);

        MutableBag<LongLongPair> elements2 = Bags.mutable.of();
        MutableBag<Integer> indexes2 = Bags.mutable.of();
        RichIterable<LongLongPair> collection2 = this.newWith(0L, 1L, 2L, 3L, 3L, 4L);
        collection2.forEachWithIndex((LongLongPair object, int index) ->
            {
                elements2.add(object);
                indexes2.add(index);
            });
        Assert.assertEquals(Bags.mutable.of(PrimitiveTuples.pair(0L, 1L), PrimitiveTuples.pair(2L, 3L), PrimitiveTuples.pair(3L, 4L)), elements2);
        Assert.assertEquals(Bags.mutable.of(0, 1, 2), indexes2);
    }

    @Test
    public void select()
    {
        MutableList<LongLongPair> result = this.newWith(1L, 2L, 2L, 3L, 3L, 4L).select(PrimitiveTuples.pair(2L, 3L)::equals).toList();
        Verify.assertContains(PrimitiveTuples.pair(2L, 3L), result);
        Verify.assertNotContains(PrimitiveTuples.pair(1L, 2L), result);
        Verify.assertNotContains(PrimitiveTuples.pair(3L, 4L), result);
    }

    @Test
    public void selectWith()
    {
        MutableList<LongLongPair> result = this.newWith(1L, 2L, 2L, 3L, 3L, 4L).selectWith(Object::equals, PrimitiveTuples.pair(2L, 3L)).toList();
        Verify.assertContains(PrimitiveTuples.pair(2L, 3L), result);
        Verify.assertNotContains(PrimitiveTuples.pair(1L, 2L), result);
        Verify.assertNotContains(PrimitiveTuples.pair(3L, 4L), result);
    }

    @Test
    public void selectWith_target()
    {
        HashBag<LongLongPair> result = this.newWith(1L, 2L, 2L, 3L, 3L, 4L).selectWith(Predicates2.notEqual(), PrimitiveTuples.pair(2L, 3L), HashBag.<LongLongPair>newBag());
        Assert.assertEquals(Bags.immutable.of(PrimitiveTuples.pair(1L, 2L), PrimitiveTuples.pair(3L, 4L)), result);
    }

    @Test
    public void reject()
    {
        MutableList<LongLongPair> result = this.newWith(1L, 2L, 2L, 3L, 3L, 4L).reject(Predicates.notEqual(PrimitiveTuples.pair(2L, 3L))).toList();
        Verify.assertContains(PrimitiveTuples.pair(2L, 3L), result);
        Verify.assertNotContains(PrimitiveTuples.pair(1L, 2L), result);
        Verify.assertNotContains(PrimitiveTuples.pair(3L, 4L), result);
    }

    @Test
    public void rejectWith()
    {
        MutableList<LongLongPair> result = this.newWith(1L, 2L, 2L, 3L, 3L, 4L).rejectWith(Predicates2.notEqual(), PrimitiveTuples.pair(2L, 3L)).toList();
        Verify.assertContains(PrimitiveTuples.pair(2L, 3L), result);
        Verify.assertNotContains(PrimitiveTuples.pair(1L, 2L), result);
        Verify.assertNotContains(PrimitiveTuples.pair(3L, 4L), result);
    }

    @Test
    public void rejectWith_target()
    {
        HashBag<LongLongPair> result = this.newWith(1L, 2L, 2L, 3L, 3L, 4L).rejectWith(Object::equals, PrimitiveTuples.pair(2L, 3L), HashBag.<LongLongPair>newBag());
        Assert.assertEquals(Bags.immutable.of(PrimitiveTuples.pair(1L, 2L), PrimitiveTuples.pair(3L, 4L)), result);
    }

    @Test
    public void selectInstancesOf()
    {
        RichIterable<LongLongPair> pairs = this.newWith(1L, 2L, 2L, 3L, 3L, 4L);
        Verify.assertIterableEmpty(pairs.selectInstancesOf(Integer.class));
        Verify.assertContainsAll(pairs.selectInstancesOf(LongLongPair.class), PrimitiveTuples.pair(1L, 2L), PrimitiveTuples.pair(3L, 4L), PrimitiveTuples.pair(2L, 3L));
    }

    @Test
    public void collect()
    {
        RichIterable<Integer> result1 =
            this.newWith(1L, 2L, 2L, 3L, 3L, 4L).collect((LongLongPair object) -> (int) object.getOne());
        Assert.assertEquals(Bags.immutable.of(1, 2, 3), result1.toBag());
        RichIterable<Long> result2 =
            this.newWith(1L, 2L, 2L, 3L, 3L, 4L).collect((LongLongPair object) -> (long) object.getTwo());
        Assert.assertEquals(Bags.immutable.of(2L, 3L, 4L), result2.toBag());
    }

    @Test
    public void collectBoolean()
    {
        BooleanIterable result = this.newWith(1L, 2L, 2L, 3L, 3L, 4L).collectBoolean((LongLongPair each) -> (each.getOne() % 2) == 0);
        Assert.assertEquals(BooleanHashBag.newBagWith(true, false, false), result.toBag());
    }

    @Test
    public void collectByte()
    {
        ByteIterable result = this.newWith(1L, 2L, 2L, 3L, 3L, 4L).collectByte((LongLongPair anObject) -> (byte) anObject.getOne());
        Assert.assertEquals(ByteHashBag.newBagWith((byte) 1, (byte) 2, (byte) 3), result.toBag());
    }

    @Test
    public void collectChar()
    {
        CharIterable result = this.newWith(1L, 2L, 2L, 3L, 3L, 4L).collectChar((LongLongPair anObject) -> (char) anObject.getOne());
        Assert.assertEquals(CharHashBag.newBagWith((char) 1, (char) 2, (char) 3), result.toBag());
    }

    @Test
    public void collectDouble()
    {
        DoubleIterable result = this.newWith(1L, 2L, 2L, 3L, 3L, 4L).collectDouble((LongLongPair anObject) -> (double) anObject.getOne());
        Assert.assertEquals(DoubleHashBag.newBagWith(1.0, 2.0, 3.0), result.toBag());
    }

    @Test
    public void collectFloat()
    {
        FloatIterable result = this.newWith(1L, 2L, 2L, 3L, 3L, 4L).collectFloat((LongLongPair anObject) -> (float) anObject.getOne());
        Assert.assertEquals(FloatHashBag.newBagWith(1.0f, 2.0f, 3.0f), result.toBag());
    }

    @Test
    public void collectInt()
    {
        IntIterable result = this.newWith(1L, 2L, 2L, 3L, 3L, 4L).collectInt((LongLongPair anObject) -> (int) anObject.getOne());
        Assert.assertEquals(IntHashBag.newBagWith(1, 2, 3), result.toBag());
    }

    @Test
    public void collectLong()
    {
        LongIterable result = this.newWith(1L, 2L, 2L, 3L, 3L, 4L).collectLong((LongLongPair anObject) -> (long) anObject.getOne());
        Assert.assertEquals(LongHashBag.newBagWith(1L, 2L, 3L), result.toBag());
    }

    @Test
    public void collectShort()
    {
        ShortIterable result = this.newWith(1L, 2L, 2L, 3L, 3L, 4L).collectShort((LongLongPair anObject) -> (short) anObject.getOne());
        Assert.assertEquals(ShortHashBag.newBagWith((short) 1, (short) 2, (short) 3), result.toBag());
    }

    @Test
    public void flatCollect()
    {
        RichIterable<LongLongPair> collection = this.newWith(1L, 2L, 2L, 3L, 3L, 4L);
        Function<LongLongPair, MutableList<String>> function = (LongLongPair object) -> FastList.newListWith(String.valueOf(object));

        Verify.assertListsEqual(
                FastList.newListWith("1:2", "2:3", "3:4"),
                collection.flatCollect(function).toSortedList());

        Verify.assertSetsEqual(
                UnifiedSet.newSetWith("1:2", "2:3", "3:4"),
                collection.flatCollect(function, UnifiedSet.<String>newSet()));
    }

    @Test
    public void detect()
    {
        Assert.assertEquals(PrimitiveTuples.pair(2L, 3L), this.newWith(1L, 2L, 2L, 3L, 3L, 4L).detect(PrimitiveTuples.pair(2L, 3L)::equals));
        Assert.assertNull(this.newWith(1L, 2L, 2L, 3L, 3L, 4L).detect(PrimitiveTuples.pair(2L, 4L)::equals));
    }

    @Test(expected = NoSuchElementException.class)
    public void min_empty_throws()
    {
        this.newWith().min(Comparators.naturalOrder());
    }

    @Test(expected = NoSuchElementException.class)
    public void max_empty_throws()
    {
        this.newWith().max(Comparators.naturalOrder());
    }

    @Test
    public void min()
    {
        Assert.assertEquals(PrimitiveTuples.pair(1L, 2L), this.newWith(1L, 2L, 2L, 3L, 3L, 4L).min(Comparators.naturalOrder()));
    }

    @Test
    public void max()
    {
        Assert.assertEquals(PrimitiveTuples.pair(3L, 4L), this.newWith(1L, 2L, 2L, 3L, 3L, 4L).max(Comparators.naturalOrder()));
    }

    @Test
    public void min_without_comparator()
    {
        Assert.assertEquals(PrimitiveTuples.pair(1L, 2L), this.newWith(1L, 2L, 2L, 3L, 3L, 4L).min(Comparators.naturalOrder()));
    }

    @Test
    public void max_without_comparator()
    {
        Assert.assertEquals(PrimitiveTuples.pair(3L, 4L), this.newWith(1L, 2L, 2L, 3L, 3L, 4L).max(Comparators.naturalOrder()));
    }

    @Test
    public void minBy()
    {
        Assert.assertEquals(PrimitiveTuples.pair(2L, 3L), this.newWith(1L, 2L, 2L, 3L, 3L, 4L).minBy((LongLongPair object) -> (int) object.getOne() & 1));
    }

    @Test
    public void maxBy()
    {
        Assert.assertEquals(PrimitiveTuples.pair(1L, 2L), this.newWith(1L, 2L, 2L, 3L, 4L, 5L).maxBy((LongLongPair object) -> (int) object.getOne() & 1));
    }

    @Test
    public void detectWith()
    {
        Assert.assertEquals(PrimitiveTuples.pair(2L, 3L), this.newWith(1L, 2L, 2L, 3L, 3L, 4L).detectWith(Object::equals, PrimitiveTuples.pair(2L, 3L)));
        Assert.assertNull(this.newWith(1L, 2L, 2L, 3L, 3L, 4L).detectWith(Object::equals, PrimitiveTuples.pair(2, 4L)));
    }

    @Test
    public void detectIfNone()
    {
        Function0<LongLongPair> function = Functions0.value(PrimitiveTuples.pair(5L, 6L));
        Assert.assertEquals(PrimitiveTuples.pair(2L, 3L), this.newWith(1L, 2L, 2L, 3L, 3L, 4L).detectIfNone(PrimitiveTuples.pair(2L, 3L)::equals, function));
        Assert.assertEquals(PrimitiveTuples.pair(5L, 6L), this.newWith(1L, 2L, 2L, 3L, 3L, 4L).detectIfNone(PrimitiveTuples.pair(2, 4L)::equals, function));
    }

    @Test
    public void detectWithIfNoneBlock()
    {
        Function0<LongLongPair> function = Functions0.value(PrimitiveTuples.pair(5L, 6L));
        Assert.assertEquals(
            PrimitiveTuples.pair(2L, 3L),
            this.newWith(1L, 2L, 2L, 3L, 3L, 4L).detectWithIfNone(
                    Object::equals,
                    PrimitiveTuples.pair(2L, 3L),
                    function));
        Assert.assertEquals(
            PrimitiveTuples.pair(5L, 6L),
            this.newWith(1L, 2L, 2L, 3L, 3L, 4L).detectWithIfNone(
                    Object::equals,
                    PrimitiveTuples.pair(2, 4L),
                    function));
    }

    @Test
    public void allSatisfy()
    {
        Assert.assertTrue(this.newWith(1L, 2L, 2L, 3L, 3L, 4L).allSatisfy(LongLongPair.class::isInstance));
        Assert.assertFalse(this.newWith(1L, 2L, 2L, 3L, 3L, 4L).allSatisfy(PrimitiveTuples.pair(2L, 3L)::equals));
    }

    @Test
    public void allSatisfyWith()
    {
        Assert.assertTrue(this.newWith(1L, 2L, 2L, 3L, 3L, 4L).allSatisfyWith(Predicates2.instanceOf(), LongLongPair.class));
        Assert.assertFalse(this.newWith(1L, 2L, 2L, 3L, 3L, 4L).allSatisfyWith(Object::equals, PrimitiveTuples.pair(2L, 3L)));
    }

    @Test
    public void noneSatisfy()
    {
        Assert.assertTrue(this.newWith(1L, 2L, 2L, 3L, 3L, 4L).noneSatisfy(Boolean.class::isInstance));
        Assert.assertFalse(this.newWith(1L, 2L, 2L, 3L, 3L, 4L).noneSatisfy(PrimitiveTuples.pair(2L, 3L)::equals));
    }

    @Test
    public void noneSatisfyWith()
    {
        Assert.assertTrue(this.newWith(1L, 2L, 2L, 3L, 3L, 4L).noneSatisfyWith(Predicates2.instanceOf(), Boolean.class));
        Assert.assertFalse(this.newWith(1L, 2L, 2L, 3L, 3L, 4L).noneSatisfyWith(Object::equals, PrimitiveTuples.pair(2L, 3L)));
    }

    @Test
    public void anySatisfy()
    {
        Assert.assertTrue(this.newWith(1L, 2L, 2L, 3L, 3L, 4L).anySatisfy(PrimitiveTuples.pair(2L, 3L)::equals));
        Assert.assertFalse(this.newWith(1L, 2L, 2L, 3L, 3L, 4L).anySatisfy(PrimitiveTuples.pair(2L, 5L)::equals));
    }

    @Test
    public void anySatisfyWith()
    {
        Assert.assertTrue(this.newWith(1L, 2L, 2L, 3L, 3L, 4L).anySatisfyWith(Object::equals, PrimitiveTuples.pair(2L, 3L)));
        Assert.assertFalse(this.newWith(1L, 2L, 2L, 3L, 3L, 4L).anySatisfyWith(Object::equals, PrimitiveTuples.pair(2L, 5L)));
    }

    @Test
    public void count()
    {
        Assert.assertEquals(0, this.newWith(1L, 2L, 2L, 3L, 3L, 4L).count(Boolean.class::isInstance));
        Assert.assertEquals(3, this.newWith(1L, 2L, 2L, 3L, 3L, 4L).count(LongLongPair.class::isInstance));
        Assert.assertEquals(1, this.newWith(1L, 2L, 2L, 3L, 3L, 4L).count(PrimitiveTuples.pair(2L, 3L)::equals));
    }

    @Test
    public void countWith()
    {
        Assert.assertEquals(0, this.newWith(1L, 2L, 2L, 3L, 3L, 4L).countWith(Predicates2.instanceOf(), Boolean.class));
        Assert.assertEquals(3, this.newWith(1L, 2L, 2L, 3L, 3L, 4L).countWith(Predicates2.instanceOf(), LongLongPair.class));
        Assert.assertEquals(1, this.newWith(1L, 2L, 2L, 3L, 3L, 4L).countWith(Object::equals, PrimitiveTuples.pair(2L, 3L)));
    }

    @Test
    public void collectIf()
    {
        Verify.assertContainsAll(
                this.newWith(1L, 2L, 2L, 3L, 3L, 4L).collectIf(
                        LongLongPair.class::isInstance,
                        String::valueOf),
                "1:2", "2:3", "3:4");
        Verify.assertContainsAll(
                this.newWith(1L, 2L, 2L, 3L, 3L, 4L).collectIf(
                        LongLongPair.class::isInstance,
                        String::valueOf,
                        UnifiedSet.<String>newSet()),
                "1:2", "2:3", "3:4");
    }

    @Test
    public void collectWith()
    {
        Assert.assertEquals(
                Bags.mutable.of(4L, 6L, 8L),
                this.newWith(1L, 2L, 2L, 3L, 3L, 4L).collectWith((LongLongPair argument1, Long argument2) -> (long) (argument1.getOne() + argument1.getTwo() + argument2), 1L).toBag());
    }

    @Test
    public void collectWith_target()
    {
        Assert.assertEquals(
                Bags.mutable.of(4L, 6L, 8L),
                this.newWith(1L, 2L, 2L, 3L, 3L, 4L).collectWith((LongLongPair argument1, Long argument2) -> (long) (argument1.getOne() + argument1.getTwo() + argument2), 1L, HashBag.<Long>newBag()));
    }

    @Test
    public void getFirst()
    {
        LongLongPair first = this.newWith(1L, 2L, 2L, 3L, 3L, 4L).getFirst();
        Assert.assertTrue(PrimitiveTuples.pair(1L, 2L).equals(first)
                || PrimitiveTuples.pair(2L, 3L).equals(first)
                || PrimitiveTuples.pair(3L, 4L).equals(first));
        Assert.assertEquals(PrimitiveTuples.pair(1L, 2L), this.newWith(1L, 2L).getFirst());
    }

    @Test
    public void getLast()
    {
        LongLongPair last = this.newWith(1L, 2L, 2L, 3L, 3L, 4L).getLast();
        Assert.assertTrue(PrimitiveTuples.pair(1L, 2L).equals(last)
                || PrimitiveTuples.pair(2L, 3L).equals(last)
                || PrimitiveTuples.pair(3L, 4L).equals(last));
        Assert.assertEquals(PrimitiveTuples.pair(1L, 2L), this.newWith(1L, 2L).getLast());
    }

    @Test
    public void isEmpty()
    {
        Verify.assertIterableEmpty(this.newWith());
        Verify.assertIterableNotEmpty(this.newWith(1L, 2L));
        Assert.assertTrue(this.newWith(1L, 2L).notEmpty());
    }

    @Test
    public void iterator()
    {
        RichIterable<LongLongPair> objects = this.newWith(1L, 2L, 0L, 3L, 3L, 4L);
        MutableBag<LongLongPair> actual = Bags.mutable.of();
        Iterator<LongLongPair> iterator = objects.iterator();
        for (int i = objects.size(); i-- > 0; )
        {
            Assert.assertTrue(iterator.hasNext());
            actual.add(iterator.next());
        }
        Assert.assertFalse(iterator.hasNext());
        Assert.assertEquals(objects.toBag(), actual);
    }

    @Test
    public void iterator_no_sentinels()
    {
        RichIterable<LongLongPair> objects = this.newWith(2L, 3L, 4L, 3L, 3L, 4L);
        MutableBag<LongLongPair> actual = Bags.mutable.of();
        Iterator<LongLongPair> iterator = objects.iterator();
        for (int i = objects.size(); i-- > 0; )
        {
            Assert.assertTrue(iterator.hasNext());
            actual.add(iterator.next());
        }
        Assert.assertFalse(iterator.hasNext());
        Assert.assertEquals(objects.toBag(), actual);
    }

    @Test(expected = NoSuchElementException.class)
    public void iterator_next_throws()
    {
        RichIterable<LongLongPair> objects = this.newWith(1L, 2L, 2L, 3L, 3L, 4L);
        Iterator<LongLongPair> iterator = objects.iterator();
        for (int i = objects.size(); i-- > 0; )
        {
            Assert.assertTrue(iterator.hasNext());
            iterator.next();
        }
        Assert.assertFalse(iterator.hasNext());
        iterator.next();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void iterator_remove_throws()
    {
        RichIterable<LongLongPair> objects = this.newWith(1L, 2L, 2L, 3L, 3L, 4L);
        Iterator<LongLongPair> iterator = objects.iterator();
        iterator.remove();
    }

    @Test
    public void injectInto()
    {
        RichIterable<LongLongPair> objects = this.newWith(1L, 2L, 2L, 3L, 3L, 4L);
        Long result = objects.injectInto(1L, (Long argument1, LongLongPair argument2) -> (long) (argument1 + argument2.getOne() + argument2.getTwo()));
        Assert.assertEquals(Long.valueOf(16), result);
    }

    @Test
    public void injectIntoInt()
    {
        RichIterable<LongLongPair> objects = this.newWith(1L, 2L, 2L, 3L, 3L, 4L);
        int result = objects.injectInto(1, (int intParameter, LongLongPair argument2) -> (int) (intParameter + argument2.getOne() + argument2.getTwo()));
        Assert.assertEquals(16, result);
    }

    @Test
    public void injectIntoLong()
    {
        RichIterable<LongLongPair> objects = this.newWith(1L, 2L, 2L, 3L, 3L, 4L);
        long result = objects.injectInto(1L, (long parameter, LongLongPair argument2) -> (long) (parameter + argument2.getOne() + argument2.getTwo()));
        Assert.assertEquals(16, result);
    }

    @Test
    public void injectIntoDouble()
    {
        RichIterable<LongLongPair> objects = this.newWith(1L, 2L, 2L, 3L, 3L, 4L);
        double result = objects.injectInto(1.0, (double parameter, LongLongPair argument2) -> (double) (parameter + argument2.getOne() + argument2.getTwo()));
        Assert.assertEquals(16.0, result, 0.0);
    }

    @Test
    public void injectIntoFloat()
    {
        RichIterable<LongLongPair> objects = this.newWith(1L, 2L, 2L, 3L, 3L, 4L);
        float result = objects.injectInto(1.0f, (float parameter, LongLongPair argument2) -> (float) (parameter + argument2.getOne() + argument2.getTwo()));
        Assert.assertEquals(16.0, result, 0.0);
    }

    @Test
    public void sumFloat()
    {
        RichIterable<LongLongPair> objects = this.newWith(1L, 2L, 2L, 3L, 3L, 4L);
        double actual = objects.sumOfFloat((LongLongPair each) -> (float) (each.getOne() + each.getTwo()));
        Assert.assertEquals(15.0, actual, 0.0);
    }

    @Test
    public void sumDouble()
    {
        RichIterable<LongLongPair> objects = this.newWith(1L, 2L, 2L, 3L, 3L, 4L);
        double actual = objects.sumOfDouble((LongLongPair each) -> (double) (each.getOne() + each.getTwo()));
        Assert.assertEquals(15.0, actual, 0.0);
    }

    @Test
    public void sumInteger()
    {
        RichIterable<LongLongPair> objects = this.newWith(1L, 2L, 2L, 3L, 3L, 4L);
        long actual = objects.sumOfInt((LongLongPair each) -> (int) (each.getOne() + each.getTwo()));
        Assert.assertEquals(15, actual);
    }

    @Test
    public void sumLong()
    {
        RichIterable<LongLongPair> objects = this.newWith(1L, 2L, 2L, 3L, 3L, 4L);
        long actual = objects.sumOfLong((LongLongPair each) -> (long) (each.getOne() + each.getTwo()));
        Assert.assertEquals(15, actual);
    }

    @Test
    public void toArray()
    {
        RichIterable<LongLongPair> objects = this.newWith(1L, 2L, 2L, 3L, 3L, 4L);
        Object[] array = objects.toArray();
        Verify.assertSize(3, array);
        LongLongPair[] array2 = objects.toArray(new LongLongPair[3]);
        Verify.assertSize(3, array2);
    }

    @Test
    public void partition()
    {
        PartitionIterable<LongLongPair> result = this.newWith(1L, 2L, 2L, 3L, 3L, 4L).partition(PrimitiveTuples.pair(2L, 3L)::equals);
        Verify.assertContains(PrimitiveTuples.pair(2L, 3L), result.getSelected().toList());
        Verify.assertIterableSize(1, result.getSelected());
        Verify.assertContains(PrimitiveTuples.pair(1L, 2L), result.getRejected().toList());
        Verify.assertContains(PrimitiveTuples.pair(3L, 4L), result.getRejected().toList());
        Verify.assertIterableSize(2, result.getRejected());
    }

    @Test
    public void toList()
    {
        MutableList<LongLongPair> list = this.newWith(1L, 2L, 2L, 3L, 3L, 4L).toList();
        Verify.assertContainsAll(list, PrimitiveTuples.pair(1L, 2L), PrimitiveTuples.pair(2L, 3L), PrimitiveTuples.pair(3L, 4L));
    }

    @Test
    public void toBag()
    {
        MutableBag<LongLongPair> bag = this.newWith(1L, 2L, 2L, 3L, 3L, 4L).toBag();
        Verify.assertContainsAll(bag, PrimitiveTuples.pair(1L, 2L), PrimitiveTuples.pair(2L, 3L), PrimitiveTuples.pair(3L, 4L));
    }

    @Test
    public void toSortedList_natural_ordering()
    {
        RichIterable<LongLongPair> pairs = this.newWith(2L, 3L, 1L, 2L, 3L, 4L);
        MutableList<LongLongPair> list = pairs.toSortedList();
        Assert.assertEquals(Lists.mutable.of(PrimitiveTuples.pair(1L, 2L), PrimitiveTuples.pair(2L, 3L), PrimitiveTuples.pair(3L, 4L)), list);
    }

    @Test
    public void toSortedList_with_comparator()
    {
        RichIterable<LongLongPair> pairs = this.newWith(2L, 3L, 1L, 2L, 3L, 4L);
        MutableList<LongLongPair> list = pairs.toSortedList(Comparators.reverseNaturalOrder());
        Assert.assertEquals(Lists.mutable.of(PrimitiveTuples.pair(3L, 4L), PrimitiveTuples.pair(2L, 3L), PrimitiveTuples.pair(1L, 2L)), list);
    }

    @Test
    public void toSortedListBy()
    {
        RichIterable<LongLongPair> pairs = this.newWith(2L, 3L, 1L, 2L, 3L, 4L);
        MutableList<LongLongPair> list = pairs.toSortedListBy(String::valueOf);
        Assert.assertEquals(Lists.mutable.of(PrimitiveTuples.pair(1L, 2L), PrimitiveTuples.pair(2L, 3L), PrimitiveTuples.pair(3L, 4L)), list);
    }

    @Test
    public void toSortedBag_natural_ordering()
    {
        RichIterable<LongLongPair> pairs = this.newWith(2L, 3L, 1L, 2L, 3L, 4L);
        MutableSortedBag<LongLongPair> bag = pairs.toSortedBag();
        Verify.assertSortedBagsEqual(TreeBag.newBagWith(PrimitiveTuples.pair(1L, 2L), PrimitiveTuples.pair(2L, 3L), PrimitiveTuples.pair(3L, 4L)), bag);
    }

    @Test
    public void toSortedBag_with_comparator()
    {
        RichIterable<LongLongPair> pairs = this.newWith(2L, 3L, 1L, 2L, 3L, 4L);
        MutableSortedBag<LongLongPair> bag = pairs.toSortedBag(Comparators.reverseNaturalOrder());
        Verify.assertSortedBagsEqual(TreeBag.newBagWith(Comparators.reverseNaturalOrder(), PrimitiveTuples.pair(3L, 4L), PrimitiveTuples.pair(2L, 3L), PrimitiveTuples.pair(1L, 2L)), bag);
    }

    @Test
    public void toSortedBagBy()
    {
        RichIterable<LongLongPair> pairs = this.newWith(2L, 3L, 1L, 2L, 3L, 4L);
        MutableSortedBag<LongLongPair> bag = pairs.toSortedBagBy(String::valueOf);
        Verify.assertSortedBagsEqual(TreeBag.newBagWith(PrimitiveTuples.pair(1L, 2L), PrimitiveTuples.pair(2L, 3L), PrimitiveTuples.pair(3L, 4L)), bag);
    }

    @Test
    public void toSortedSet_natural_ordering()
    {
        RichIterable<LongLongPair> pairs = this.newWith(2L, 3L, 1L, 2L, 3L, 4L);
        MutableSortedSet<LongLongPair> set = pairs.toSortedSet();
        Verify.assertSortedSetsEqual(TreeSortedSet.newSetWith(PrimitiveTuples.pair(1L, 2L), PrimitiveTuples.pair(2L, 3L), PrimitiveTuples.pair(3L, 4L)), set);
    }

    @Test
    public void toSortedSet_with_comparator()
    {
        RichIterable<LongLongPair> pairs = this.newWith(2L, 3L, 1L, 2L, 3L, 4L);
        MutableSortedSet<LongLongPair> set = pairs.toSortedSet(Comparators.reverseNaturalOrder());
        Verify.assertSortedSetsEqual(TreeSortedSet.newSetWith(Comparators.reverseNaturalOrder(),
                PrimitiveTuples.pair(3L, 4L),
                PrimitiveTuples.pair(2L, 3L),
                PrimitiveTuples.pair(1L, 2L)),
                set);
    }

    @Test
    public void toSortedSetBy()
    {
        RichIterable<LongLongPair> pairs = this.newWith(2L, 3L, 1L, 2L, 3L, 4L);
        MutableSortedSet<LongLongPair> set = pairs.toSortedSetBy(String::valueOf);
        Verify.assertSortedSetsEqual(TreeSortedSet.newSetWith(PrimitiveTuples.pair(1L, 2L), PrimitiveTuples.pair(2L, 3L), PrimitiveTuples.pair(3L, 4L)), set);
    }

    @Test
    public void toSet()
    {
        RichIterable<LongLongPair> pairs = this.newWith(1L, 2L, 2L, 3L, 3L, 4L);
        MutableSet<LongLongPair> set = pairs.toSet();
        Verify.assertContainsAll(set, PrimitiveTuples.pair(1L, 2L), PrimitiveTuples.pair(2L, 3L), PrimitiveTuples.pair(3L, 4L));
    }

    @Test
    public void toMap()
    {
        RichIterable<LongLongPair> pairs = this.newWith(1L, 2L, 2L, 3L, 3L, 4L);
        MutableMap<String, String> map =
                pairs.toMap(String::valueOf, String::valueOf);
        Assert.assertEquals(UnifiedMap.newWithKeysValues("1:2", "1:2", "2:3", "2:3", "3:4", "3:4"), map);
    }

    @Test
    public void toSortedMap()
    {
        RichIterable<LongLongPair> pairs = this.newWith(1L, 2L, 2L, 3L, 3L, 4L);
        MutableSortedMap<String, String> map =
                pairs.toSortedMap(String::valueOf, String::valueOf);
        Assert.assertEquals(TreeSortedMap.newMapWith("1:2", "1:2", "2:3", "2:3", "3:4", "3:4"), map);
    }

    @Test
    public void toSortedMap_with_comparator()
    {
        RichIterable<LongLongPair> pairs = this.newWith(1L, 2L, 2L, 3L, 3L, 4L);
        MutableSortedMap<String, String> map =
                pairs.toSortedMap(Comparators.reverseNaturalOrder(), String::valueOf, String::valueOf);
        Assert.assertEquals(TreeSortedMap.newMapWith(Comparators.reverseNaturalOrder(), "1:2", "1:2", "2:3", "2:3", "3:4", "3:4"), map);
    }

    @Test
    public void testToString()
    {
        RichIterable<LongLongPair> collection = this.newWith(1L, 2L, 2L, 3L);
        Assert.assertTrue("[1:2, 2:3]".equals(collection.toString())
                || "[2:3, 1:2]".equals(collection.toString()));
    }

    @Test
    public void makeString()
    {
        RichIterable<LongLongPair> collection = this.newWith(1L, 2L, 2L, 3L, 3L, 4L);
        Assert.assertEquals(collection.toString(), '[' + collection.makeString() + ']');
    }

    @Test
    public void makeStringWithSeparator()
    {
        RichIterable<LongLongPair> collection = this.newWith(1L, 2L, 2L, 3L, 3L, 4L);
        Assert.assertEquals(collection.toString(), '[' + collection.makeString(", ") + ']');
    }

    @Test
    public void makeStringWithSeparatorAndStartAndEnd()
    {
        RichIterable<LongLongPair> collection = this.newWith(1L, 2L, 2L, 3L, 3L, 4L);
        Assert.assertEquals(collection.toString(), collection.makeString("[", ", ", "]"));
    }

    @Test
    public void appendString()
    {
        RichIterable<LongLongPair> collection = this.newWith(1L, 2L, 2L, 3L, 3L, 4L);
        Appendable builder = new StringBuilder();
        collection.appendString(builder);
        Assert.assertEquals(collection.toString(), '[' + builder.toString() + ']');
    }

    @Test
    public void appendStringWithSeparator()
    {
        RichIterable<LongLongPair> collection = this.newWith(1L, 2L, 2L, 3L, 3L, 4L);
        Appendable builder = new StringBuilder();
        collection.appendString(builder, ", ");
        Assert.assertEquals(collection.toString(), '[' + builder.toString() + ']');
    }

    @Test
    public void appendStringWithSeparatorAndStartAndEnd()
    {
        RichIterable<LongLongPair> collection = this.newWith(1L, 2L, 2L, 3L, 3L, 4L);
        Appendable builder = new StringBuilder();
        collection.appendString(builder, "[", ", ", "]");
        Assert.assertEquals(collection.toString(), builder.toString());
    }

    @Test
    public void groupBy()
    {
        RichIterable<LongLongPair> collection = this.newWith(1L, 2L, 2L, 3L, 3L, 4L);
        Function<LongLongPair, Boolean> function = (LongLongPair object) -> PrimitiveTuples.pair(1L, 2L).equals(object);

        Multimap<Boolean, LongLongPair> multimap = collection.groupBy(function);
        Assert.assertEquals(3, multimap.size());
        Assert.assertTrue(multimap.containsKeyAndValue(Boolean.TRUE, PrimitiveTuples.pair(1L, 2L)));
        Assert.assertTrue(multimap.containsKeyAndValue(Boolean.FALSE, PrimitiveTuples.pair(2L, 3L)));
        Assert.assertTrue(multimap.containsKeyAndValue(Boolean.FALSE, PrimitiveTuples.pair(3L, 4L)));
    }

    @Test
    public void groupByEach()
    {
        RichIterable<LongLongPair> collection = this.newWith(1L, 2L, 2L, 3L, 3L, 4L);
        Function<LongLongPair, MutableList<Boolean>> function = (LongLongPair object) -> Lists.mutable.of(PrimitiveTuples.pair(1L, 2L).equals(object));

        Multimap<Boolean, LongLongPair> multimap = collection.groupByEach(function);
        Assert.assertEquals(3, multimap.size());
        Assert.assertTrue(multimap.containsKeyAndValue(Boolean.TRUE, PrimitiveTuples.pair(1L, 2L)));
        Assert.assertTrue(multimap.containsKeyAndValue(Boolean.FALSE, PrimitiveTuples.pair(2L, 3L)));
        Assert.assertTrue(multimap.containsKeyAndValue(Boolean.FALSE, PrimitiveTuples.pair(3L, 4L)));
    }

    @Test
    public void zip()
    {
        RichIterable<LongLongPair> collection = this.newWith(1L, 2L, 2L, 3L);
        RichIterable<Pair<LongLongPair, Integer>> result = collection.zip(Interval.oneTo(5));

        Assert.assertTrue(Bags.mutable.of(Tuples.pair(PrimitiveTuples.pair(1L, 2L), 1), Tuples.pair(PrimitiveTuples.pair(2L, 3L), 2)).equals(result.toBag())
                || Bags.mutable.of(Tuples.pair(PrimitiveTuples.pair(2L, 3L), 1), Tuples.pair(PrimitiveTuples.pair(1L, 2L), 2)).equals(result.toBag()));
    }

    @Test
    public void zipWithIndex()
    {
        RichIterable<LongLongPair> collection = this.newWith(1L, 2L, 2L, 3L);
        RichIterable<Pair<LongLongPair, Integer>> result = collection.zipWithIndex();
        Assert.assertTrue(Bags.mutable.of(Tuples.pair(PrimitiveTuples.pair(1L, 2L), 0), Tuples.pair(PrimitiveTuples.pair(2L, 3L), 1)).equals(result.toBag())
                || Bags.mutable.of(Tuples.pair(PrimitiveTuples.pair(2L, 3L), 0), Tuples.pair(PrimitiveTuples.pair(1L, 2L), 1)).equals(result.toBag()));
    }

    @Test
    public void chunk()
    {
        RichIterable<LongLongPair> collection = this.newWith(1L, 2L, 2L, 3L, 3L, 4L);
        Assert.assertEquals(Bags.immutable.of(FastList.newListWith(PrimitiveTuples.pair(1L, 2L)),
                FastList.newListWith(PrimitiveTuples.pair(2L, 3L)),
                FastList.newListWith(PrimitiveTuples.pair(3L, 4L))),
                collection.chunk(1).toBag());
    }

    @Test(expected = IllegalArgumentException.class)
    public void chunk_zero_throws()
    {
        RichIterable<LongLongPair> collection = this.newWith(1L, 2L, 2L, 3L, 3L, 4L);
        collection.chunk(0);
    }

    @Test
    public void chunk_large_size()
    {
        RichIterable<LongLongPair> collection = this.newWith(1L, 2L, 2L, 3L, 3L, 4L);
        Verify.assertIterableSize(3, collection.chunk(10).getFirst());
    }

    @Test
    public void empty()
    {
        Verify.assertIterableEmpty(this.newWith());
        Assert.assertTrue(this.newWith().isEmpty());
        Assert.assertFalse(this.newWith().notEmpty());
    }

    @Test
    public void notEmpty()
    {
        RichIterable<LongLongPair> notEmpty = this.newWith(1L, 2L);
        Verify.assertIterableNotEmpty(notEmpty);
    }

    @Test
    public void aggregateByMutating()
    {
        Function0<AtomicInteger> valueCreator = Functions0.zeroAtomicInteger();
        Procedure2<AtomicInteger, LongLongPair> sumAggregator = (AtomicInteger aggregate, LongLongPair value) -> aggregate.addAndGet((int) value.getOne());
        RichIterable<LongLongPair> collection = this.newWith(1L, 2L, 2L, 3L, 3L, 4L);
        MapIterable<String, AtomicInteger> aggregation = collection.aggregateInPlaceBy(String::valueOf, valueCreator, sumAggregator);
        Assert.assertEquals(3, aggregation.get("3:4").intValue());
        Assert.assertEquals(2, aggregation.get("2:3").intValue());
        Assert.assertEquals(1, aggregation.get("1:2").intValue());
    }

    @Test
    public void aggregateByNonMutating()
    {
        Function0<Integer> valueCreator = Functions0.value(0);
        Function2<Integer, LongLongPair, Integer> sumAggregator = (Integer aggregate, LongLongPair value) -> (int) (aggregate + value.getOne());
        RichIterable<LongLongPair> collection = this.newWith(1L, 1L, 1L, 2L, 2L, 3L);
        MapIterable<String, Integer> aggregation = collection.aggregateBy(String::valueOf, valueCreator, sumAggregator);
        Assert.assertEquals(2, aggregation.get("2:3").intValue());
        Assert.assertEquals(1, aggregation.get("1:2").intValue());
    }
}
