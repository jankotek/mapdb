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
import org.eclipse.collections.api.block.function.primitive.LongToObjectFunction;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.map.primitive.*;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.api.tuple.primitive.LongLongPair;
import org.eclipse.collections.impl.bag.mutable.primitive.LongHashBag;
import org.eclipse.collections.impl.block.factory.primitive.LongPredicates;
import org.eclipse.collections.impl.factory.Bags;
import org.eclipse.collections.impl.factory.primitive.*;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.eclipse.collections.impl.test.Verify;
import org.eclipse.collections.impl.tuple.primitive.PrimitiveTuples;
import org.junit.*;
import org.mapdb.TT;

import java.util.*;

/**
 * This file was automatically generated from template file abstractPrimitivePrimitiveMapTestCase.stg.
 */
public abstract class AbstractLongLongMapTestCase
{
    protected final LongLongMap map = this.classUnderTest();

    protected abstract LongLongMap classUnderTest();

    protected abstract LongLongMap newWithKeysValues(long key1, long value1);

    protected abstract LongLongMap newWithKeysValues(long key1, long value1, long key2, long value2);

    protected abstract LongLongMap newWithKeysValues(long key1, long value1, long key2, long value2, long key3, long value3);

    protected abstract LongLongMap newWithKeysValues(long key1, long value1, long key2, long value2, long key3, long value3, long key4, long value4);

    protected abstract LongLongMap getEmptyMap();

    @Test
    public void keySet()
    {
        Verify.assertEmpty(this.getEmptyMap().keySet());
        Assert.assertEquals(LongHashSet.newSetWith(0L), this.newWithKeysValues(0L, 0L).keySet());
        Assert.assertEquals(LongHashSet.newSetWith(0L, 31L, 32L),
                this.newWithKeysValues(0L, 0L, 31L, 31L, 32L, 32L).keySet());
    }

    @Test
    public void values()
    {
        Verify.assertEmpty(this.getEmptyMap().values());

        LongLongMap map = this.newWithKeysValues(0L, 0L);
        Verify.assertSize(1, map.values());
        Assert.assertTrue(map.values().contains(0L));

        LongLongMap map1 = this.newWithKeysValues(0L, 0L, 31L, 31L, 32L, 32L);
        Verify.assertSize(3, map1.values());
        Assert.assertTrue(map1.values().contains(0L));
        Assert.assertTrue(map1.values().contains(31L));
        Assert.assertTrue(map1.values().contains(32L));
    }

    @Test
    public void get()
    {
        Assert.assertEquals(0L, this.map.get(0L));
        Assert.assertEquals(31L, this.map.get(31L));
        Assert.assertEquals(32L, this.map.get(32L));

        Assert.assertEquals(0L, this.map.get(1L));
        Assert.assertEquals(0L, this.map.get(33L));
    }

    @Test
    public void getIfAbsent()
    {
        Assert.assertEquals(0L, this.map.getIfAbsent(0L, 5L));
        Assert.assertEquals(31L, this.map.getIfAbsent(31L, 5L));
        Assert.assertEquals(32L, this.map.getIfAbsent(32L, 5L));
    }

    @Test
    public void getOrThrow()
    {
        Assert.assertEquals(0L, this.map.getOrThrow(0L));
        Assert.assertEquals(31L, this.map.getOrThrow(31L));
        Assert.assertEquals(32L, this.map.getOrThrow(32L));

        Verify.assertThrows(IllegalStateException.class, () -> this.map.getOrThrow(1L));
        Verify.assertThrows(IllegalStateException.class, () -> this.map.getOrThrow(33L));
    }

    @Test
    public void containsKey()
    {
        Assert.assertTrue(this.map.containsKey(0L));
        Assert.assertTrue(this.map.containsKey(31L));
        Assert.assertTrue(this.map.containsKey(32L));
        Assert.assertFalse(this.map.containsKey(1L));
        Assert.assertFalse(this.map.containsKey(5L));
        Assert.assertFalse(this.map.containsKey(35L));
    }

    @Test
    public void containsValue()
    {
        Assert.assertTrue(this.map.containsValue(0L));
        Assert.assertTrue(this.map.containsValue(31L));
        Assert.assertTrue(this.map.containsValue(32L));
    }

    @Test
    public void contains()
    {
        Assert.assertTrue(this.map.contains(0L));
        Assert.assertTrue(this.map.contains(31L));
        Assert.assertTrue(this.map.contains(32L));
    }

    @Test
    public void containsAll()
    {
        Assert.assertTrue(this.map.containsAll(0L, 31L, 32L));
        Assert.assertFalse(this.map.containsAll(0L, 31L, 35L));
        Assert.assertTrue(this.map.containsAll());
    }

    @Test
    public void containsAll_Iterable()
    {
        Assert.assertTrue(this.map.containsAll(LongArrayList.newListWith(0L, 31L, 32L)));
        Assert.assertFalse(this.map.containsAll(LongArrayList.newListWith(0L, 31L, 35L)));
        Assert.assertTrue(this.map.containsAll(new LongArrayList()));
    }

    @Test
    public void size()
    {
        Assert.assertEquals(0, this.getEmptyMap().size());
        Assert.assertEquals(1, this.newWithKeysValues(0L, 0L).size());
        Assert.assertEquals(1, this.newWithKeysValues(1L, 1L).size());

        Assert.assertEquals(2, this.newWithKeysValues(1L, 1L, 5L, 5L).size());
        Assert.assertEquals(2, this.newWithKeysValues(0L, 0L, 5L, 5L).size());
        Assert.assertEquals(3, this.newWithKeysValues(1L, 1L, 0L, 0L, 5L, 5L).size());
        Assert.assertEquals(2, this.newWithKeysValues(6L, 6L, 5L, 5L).size());
    }

    @Test
    public void isEmpty()
    {
        Assert.assertTrue(this.getEmptyMap().isEmpty());
        Assert.assertFalse(this.map.isEmpty());
        Assert.assertFalse(this.newWithKeysValues(1L, 1L).isEmpty());
        Assert.assertFalse(this.newWithKeysValues(0L, 0L).isEmpty());
        Assert.assertFalse(this.newWithKeysValues(50L, 50L).isEmpty());
    }

    @Test
    public void notEmpty()
    {
        Assert.assertFalse(this.getEmptyMap().notEmpty());
        Assert.assertTrue(this.map.notEmpty());
        Assert.assertTrue(this.newWithKeysValues(1L, 1L).notEmpty());
        Assert.assertTrue(this.newWithKeysValues(0L, 0L).notEmpty());
        Assert.assertTrue(this.newWithKeysValues(50L, 50L).notEmpty());
    }

    @Test
    public void testEquals()
    {
        LongLongMap map1 = this.newWithKeysValues(0L, 0L, 1L, 1L, 32L, 32L);
        LongLongMap map2 = this.newWithKeysValues(32L, 32L, 0L, 0L, 1L, 1L);
        LongLongMap map3 = this.newWithKeysValues(0L, 0L, 1L, 2L, 32L, 32L);
        LongLongMap map4 = this.newWithKeysValues(0L, 1L, 1L, 1L, 32L, 32L);
        LongLongMap map5 = this.newWithKeysValues(0L, 0L, 1L, 1L, 32L, 33L);
        LongLongMap map6 = this.newWithKeysValues(50L, 0L, 60L, 1L, 70L, 33L);
        LongLongMap map7 = this.newWithKeysValues(50L, 0L, 60L, 1L);
        LongLongMap map8 = this.newWithKeysValues(0L, 0L, 1L, 1L);
        LongLongMap map9 = this.newWithKeysValues(0L, 0L);

        Verify.assertEqualsAndHashCode(map1, map2);
//        Verify.assertPostSerializedEqualsAndHashCode(map1);
//        Verify.assertPostSerializedEqualsAndHashCode(map6);
//        Verify.assertPostSerializedEqualsAndHashCode(map8);
//        Verify.assertPostSerializedEqualsAndHashCode(this.getEmptyMap());
        Assert.assertNotEquals(map1, map3);
        Assert.assertNotEquals(this.getEmptyMap(), map3);
        Assert.assertNotEquals(map9, this.getEmptyMap());
        Assert.assertNotEquals(this.getEmptyMap(), map9);
        Assert.assertNotEquals(LongArrayList.newListWith(0L), map9);
        Assert.assertNotEquals(map1, map4);
        Assert.assertNotEquals(map1, map5);
        Assert.assertNotEquals(map7, map6);
        Assert.assertNotEquals(map7, map8);

        Assert.assertEquals(map1, LongLongMaps.mutable.ofAll(map1));
        Assert.assertEquals(map1, LongLongMaps.immutable.ofAll(map1));
    }

//    @Test
//    public void testHashCode()
//    {
//        Assert.assertEquals(
//                UnifiedMap.newWithKeysValues(0L, 0L, 1L, 1L, 32L, 32L).hashCode(),
//                this.newWithKeysValues(32L, 32L, 0L, 0L, 1L, 1L).hashCode());
//        Assert.assertEquals(
//                UnifiedMap.newWithKeysValues(50L, 0L, 60L, 1L, 70L, 33L).hashCode(),
//                this.newWithKeysValues(50L, 0L, 60L, 1L, 70L, 33L).hashCode());
//        Assert.assertEquals(UnifiedMap.newMap().hashCode(), this.getEmptyMap().hashCode());
//        Assert.assertEquals(UnifiedMap.newWithKeysValues(1L, 2L).hashCode(), this.newWithKeysValues(1L, 2L).hashCode());
//    }

    @Test
    public void testToString()
    {
        Assert.assertEquals("{}", this.getEmptyMap().toString());
        Assert.assertEquals("{0=0}", this.newWithKeysValues(0L, 0L).toString());
        Assert.assertEquals("{1=1}", this.newWithKeysValues(1L, 1L).toString());
        Assert.assertEquals("{5=5}", this.newWithKeysValues(5L, 5L).toString());

        LongLongMap map1 = this.newWithKeysValues(0L, 0L, 1L, 1L);
        Assert.assertTrue(
                map1.toString(),
                "{0=0, 1=1}".equals(map1.toString())
                        || "{1=1, 0=0}".equals(map1.toString()));

        LongLongMap map2 = this.newWithKeysValues(1L, 1L, 32L, 32L);
        Assert.assertTrue(
                map2.toString(),
                "{1=1, 32=32}".equals(map2.toString())
                        || "{32=32, 1=1}".equals(map2.toString()));

        LongLongMap map3 = this.newWithKeysValues(0L, 0L, 32L, 32L);
        Assert.assertTrue(
                map3.toString(),
                "{0=0, 32=32}".equals(map3.toString())
                        || "{32=32, 0=0}".equals(map3.toString()));

        LongLongMap map4 = this.newWithKeysValues(32L, 32L, 33L, 33L);
        Assert.assertTrue(
                map4.toString(),
                "{32=32, 33=33}".equals(map4.toString())
                        || "{33=33, 32=32}".equals(map4.toString()));
    }

    @Test
    public void forEach()
    {
        LongLongMap map0 = this.newWithKeysValues(0L, 1L, 3L, 4L);
        long[] sum0 = new long[1];
        map0.forEach(each -> sum0[0] += each);
        Assert.assertEquals(5L, sum0[0]);

        LongLongMap map1 = this.newWithKeysValues(1L, 2L, 3L, 4L);
        long[] sum1 = new long[1];
        map1.forEach(each -> sum1[0] += each);
        Assert.assertEquals(6L, sum1[0]);

        LongLongMap map01 = this.newWithKeysValues(0L, 1L, 1L, 2L);
        long[] sum01 = new long[1];
        map01.forEach(each -> sum01[0] += each);
        Assert.assertEquals(3L, sum01[0]);

        LongLongMap map = this.newWithKeysValues(3L, 4L, 4L, 5L);
        long[] sum = new long[1];
        map.forEach(each -> sum[0] += each);
        Assert.assertEquals(9L, sum[0]);

        LongLongMap map2 = this.getEmptyMap();
        long[] sum2 = new long[1];
        map2.forEach(each -> sum2[0] += each);
        Assert.assertEquals(0L, sum2[0]);

        LongLongMap map3 = this.newWithKeysValues(1L, 2L);
        long[] sum3 = new long[1];
        map3.forEach(each -> sum3[0] += each);
        Assert.assertEquals(2L, sum3[0]);
    }

    @Test
    public void forEachValue()
    {
        LongLongMap map0 = this.newWithKeysValues(0L, 1L, 3L, 4L);
        long[] sum0 = new long[1];
        map0.forEachValue(each -> sum0[0] += each);
        Assert.assertEquals(5L, sum0[0]);

        LongLongMap map1 = this.newWithKeysValues(1L, 2L, 3L, 4L);
        long[] sum1 = new long[1];
        map1.forEachValue(each -> sum1[0] += each);
        Assert.assertEquals(6L, sum1[0]);

        LongLongMap map01 = this.newWithKeysValues(0L, 1L, 1L, 2L);
        long[] sum01 = new long[1];
        map01.forEachValue(each -> sum01[0] += each);
        Assert.assertEquals(3L, sum01[0]);

        LongLongMap map = this.newWithKeysValues(3L, 4L, 4L, 5L);
        long[] sum = new long[1];
        map.forEachValue(each -> sum[0] += each);
        Assert.assertEquals(9L, sum[0]);

        LongLongMap map2 = this.getEmptyMap();
        long[] sum2 = new long[1];
        map2.forEachValue(each -> sum2[0] += each);
        Assert.assertEquals(0L, sum2[0]);

        LongLongMap map3 = this.newWithKeysValues(1L, 2L);
        long[] sum3 = new long[1];
        map3.forEachValue(each -> sum3[0] += each);
        Assert.assertEquals(2L, sum3[0]);
    }

    @Test
    public void forEachKey()
    {
        LongLongMap map0 = this.newWithKeysValues(0L, 1L, 3L, 4L);
        long[] sum0 = new long[1];
        map0.forEachKey(each -> sum0[0] += each);
        Assert.assertEquals(3L, sum0[0]);

        LongLongMap map1 = this.newWithKeysValues(1L, 2L, 3L, 4L);
        long[] sum1 = new long[1];
        map1.forEachKey(each -> sum1[0] += each);
        Assert.assertEquals(4L, sum1[0]);

        LongLongMap map01 = this.newWithKeysValues(0L, 1L, 1L, 2L);
        long[] sum01 = new long[1];
        map01.forEachKey(each -> sum01[0] += each);
        Assert.assertEquals(1L, sum01[0]);

        LongLongMap map = this.newWithKeysValues(3L, 4L, 4L, 5L);
        long[] sum = new long[1];
        map.forEachKey(each -> sum[0] += each);
        Assert.assertEquals(7L, sum[0]);

        LongLongMap map2 = this.getEmptyMap();
        long[] sum2 = new long[1];
        map2.forEachKey(each -> sum2[0] += each);
        Assert.assertEquals(0L, sum2[0]);

        LongLongMap map3 = this.newWithKeysValues(1L, 1L);
        long[] sum3 = new long[1];
        map3.forEachKey(each -> sum3[0] += each);
        Assert.assertEquals(1L, sum3[0]);
    }

    @Test
    public void forEachKeyValue()
    {
        LongLongMap map0 = this.newWithKeysValues(0L, 1L, 3L, 4L);
        long[] sumKey0 = new long[1];
        long[] sumValue0 = new long[1];
        map0.forEachKeyValue((long eachKey, long eachValue) ->
        {
            sumKey0[0] += eachKey;
            sumValue0[0] += eachValue;
        });
        Assert.assertEquals(3L, sumKey0[0]);
        Assert.assertEquals(5L, sumValue0[0]);

        LongLongMap map1 = this.newWithKeysValues(1L, 2L, 3L, 4L);
        long[] sumKey1 = new long[1];
        long[] sumValue1 = new long[1];
        map1.forEachKeyValue((long eachKey, long eachValue) ->
        {
            sumKey1[0] += eachKey;
            sumValue1[0] += eachValue;
        });
        Assert.assertEquals(4L, sumKey1[0]);
        Assert.assertEquals(6L, sumValue1[0]);

        LongLongMap map01 = this.newWithKeysValues(0L, 1L, 1L, 2L);
        long[] sumKey01 = new long[1];
        long[] sumValue01 = new long[1];
        map01.forEachKeyValue((long eachKey, long eachValue) ->
        {
            sumKey01[0] += eachKey;
            sumValue01[0] += eachValue;
        });
        Assert.assertEquals(1L, sumKey01[0]);
        Assert.assertEquals(3L, sumValue01[0]);

        LongLongMap map = this.newWithKeysValues(3L, 4L, 4L, 5L);
        long[] sumKey = new long[1];
        long[] sumValue = new long[1];
        map.forEachKeyValue((long eachKey, long eachValue) ->
        {
            sumKey[0] += eachKey;
            sumValue[0] += eachValue;
        });
        Assert.assertEquals(7L, sumKey[0]);
        Assert.assertEquals(9L, sumValue[0]);

        LongLongMap map2 = this.getEmptyMap();
        long[] sumKey2 = new long[1];
        long[] sumValue2 = new long[1];
        map2.forEachKeyValue((long eachKey, long eachValue) ->
        {
            sumKey2[0] += eachKey;
            sumValue2[0] += eachValue;
        });
        Assert.assertEquals(0L, sumKey2[0]);
        Assert.assertEquals(0L, sumValue2[0]);

        LongLongMap map3 = this.newWithKeysValues(3L, 5L);
        long[] sumKey3 = new long[1];
        long[] sumValue3 = new long[1];
        map3.forEachKeyValue((long eachKey, long eachValue) ->
        {
            sumKey3[0] += eachKey;
            sumValue3[0] += eachValue;
        });
        Assert.assertEquals(3L, sumKey3[0]);
        Assert.assertEquals(5L, sumValue3[0]);
    }

    @Test
    public void makeString()
    {
        Assert.assertEquals("", this.getEmptyMap().makeString());
        Assert.assertEquals("", this.getEmptyMap().makeString(", "));
        Assert.assertEquals("[]", this.getEmptyMap().makeString("[", "/", "]"));
        Assert.assertEquals("0", this.newWithKeysValues(0L, 0L).makeString());
        Assert.assertEquals("0", this.newWithKeysValues(0L, 0L).makeString(", "));
        Assert.assertEquals("[0]", this.newWithKeysValues(0L, 0L).makeString("[", "/", "]"));
        Assert.assertEquals("1", this.newWithKeysValues(1L, 1L).makeString());
        Assert.assertEquals("5", this.newWithKeysValues(5L, 5L).makeString());

        LongLongMap map1 = this.newWithKeysValues(0L, 0L, 1L, 1L);
        Assert.assertTrue(
                map1.makeString(),
                "0, 1".equals(map1.makeString())
                        || "1, 0".equals(map1.makeString()));

        LongLongMap map2 = this.newWithKeysValues(1L, 1L, 32L, 32L);
        Assert.assertTrue(
                map2.makeString("[", "/", "]"),
                "[1/32]".equals(map2.makeString("[", "/", "]"))
                        || "[32/1]".equals(map2.makeString("[", "/", "]")));

        LongLongMap map3 = this.newWithKeysValues(0L, 0L, 32L, 32L);
        Assert.assertTrue(
                map3.makeString("~"),
                "0~32".equals(map3.makeString("~"))
                        || "32~0".equals(map3.makeString("~")));

        LongLongMap map4 = this.newWithKeysValues(32L, 32L, 33L, 33L);
        Assert.assertTrue(
                map4.makeString("[", ", ", "]"),
                "[32, 33]".equals(map4.makeString("[", ", ", "]"))
                        || "[33, 32]".equals(map4.makeString("[", ", ", "]")));
    }

    @Test
    public void appendString()
    {
        Appendable appendable = new StringBuilder();
        this.getEmptyMap().appendString(appendable);
        Assert.assertEquals("", appendable.toString());

        this.getEmptyMap().appendString(appendable, "/");
        Assert.assertEquals("", appendable.toString());

        this.getEmptyMap().appendString(appendable, "{", "/", "}");
        Assert.assertEquals("{}", appendable.toString());

        Appendable appendable0 = new StringBuilder();
        this.newWithKeysValues(0L, 0L).appendString(appendable0);
        Assert.assertEquals("0", appendable0.toString());

        Appendable appendable01 = new StringBuilder();
        this.newWithKeysValues(0L, 0L).appendString(appendable01, "/");
        Assert.assertEquals("0", appendable01.toString());

        Appendable appendable02 = new StringBuilder();
        this.newWithKeysValues(0L, 0L).appendString(appendable02, "{", "/", "}");
        Assert.assertEquals("{0}", appendable02.toString());

        Appendable appendable1 = new StringBuilder();
        this.newWithKeysValues(1L, 1L).appendString(appendable1);
        Assert.assertEquals("1", appendable1.toString());

        Appendable appendable2 = new StringBuilder();
        this.newWithKeysValues(5L, 5L).appendString(appendable2);
        Assert.assertEquals("5", appendable2.toString());

        Appendable appendable3 = new StringBuilder();
        LongLongMap map1 = this.newWithKeysValues(0L, 0L, 1L, 1L);
        map1.appendString(appendable3);
        Assert.assertTrue(
                appendable3.toString(),
                "0, 1".equals(appendable3.toString())
                        || "1, 0".equals(appendable3.toString()));

        Appendable appendable4 = new StringBuilder();
        LongLongMap map2 = this.newWithKeysValues(1L, 1L, 32L, 32L);
        map2.appendString(appendable4, "[", "/", "]");
        Assert.assertTrue(
                appendable4.toString(),
                "[1/32]".equals(appendable4.toString())
                        || "[32/1]".equals(appendable4.toString()));

        Appendable appendable5 = new StringBuilder();
        LongLongMap map3 = this.newWithKeysValues(1L, 1L, 32L, 32L);
        map3.appendString(appendable5, "[", "/", "]");
        Assert.assertTrue(
                appendable5.toString(),
                "[1/32]".equals(appendable5.toString())
                        || "[32/1]".equals(appendable5.toString()));

        Appendable appendable6 = new StringBuilder();
        map1.appendString(appendable6, "/");
        Assert.assertTrue(
                appendable6.toString(),
                "0/1".equals(appendable6.toString())
                        || "1/0".equals(appendable6.toString()));
    }

    @Test
    public void select()
    {
        LongLongMap map = this.newWithKeysValues(0L, 0L, 1L, 1L, 2L, 2L, 3L, 3L);
        LongLongMap actual1 = map.select((long key, long value) -> key == 1L || value == 3L);
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(1L, 1L, 3L, 3L), actual1);
        LongLongMap actual2 = map.select((long key, long value) -> key == 0L || value == 2L);
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(0L, 0L, 2L, 2L), actual2);
    }

    @Test
    public void reject()
    {
        LongLongMap map = this.newWithKeysValues(0L, 0L, 1L, 1L, 2L, 2L, 3L, 3L);
        LongLongMap actual1 = map.reject((long key, long value) -> key == 1L || value == 3L);
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(0L, 0L, 2L, 2L), actual1);
        LongLongMap actual2 = map.reject((long key, long value)-> key == 0L || value == 2L);
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(1L, 1L, 3L, 3L), actual2);
    }


    @Test
    public void select_value()
    {
        LongLongMap map = this.newWithKeysValues(0L, 0L, 1L, 1L, 2L, 2L, 3L, 3L);
        LongIterable actual1 = map.select(LongPredicates.greaterThan(1L));
        Assert.assertEquals(LongBags.immutable.with(2L, 3L), actual1);
        LongIterable actual2 = map.select(LongPredicates.lessThan(2L));
        Assert.assertEquals(LongBags.immutable.with(0L, 1L), actual2);
    }

    @Test
    public void reject_value()
    {
        LongLongMap map = this.newWithKeysValues(0L, 0L, 1L, 1L, 2L, 2L, 3L, 3L);
        LongIterable actual1 = map.reject(LongPredicates.lessThan(2L));
        Assert.assertEquals(LongBags.immutable.with(2L, 3L), actual1);
        LongIterable actual2 = map.reject(LongPredicates.greaterThan(1L));
        Assert.assertEquals(LongBags.immutable.with(0L, 1L), actual2);
    }

    @Test
    public void collect()
    {
        LongLongMap map = this.newWithKeysValues(0L, 0L, 1L, 1L, 2L, 2L, 3L, 3L);

        LongToObjectFunction<Long> function = (parameter) -> parameter + 1;
        Assert.assertEquals(Bags.immutable.with(1L, 2L, 3L, 4L), map.collect(function));
        Assert.assertEquals(Bags.immutable.empty(), this.getEmptyMap().collect(function));
        Assert.assertEquals(Bags.immutable.with(2L), this.newWithKeysValues(1L, 1L).collect(function));
    }

    @Test
    public void count()
    {
        LongLongMap map = this.newWithKeysValues(0L, 0L, 1L, 1L, 2L, 2L, 3L, 3L);
        Assert.assertEquals(2, map.count(LongPredicates.greaterThan(1L)));
        Assert.assertEquals(2, map.count(LongPredicates.lessThan(2L)));
    }

    @Test
    public void detectIfNone_value()
    {
        LongLongMap map = this.newWithKeysValues(0L, 0L, 1L, 1L, 2L, 2L, 3L, 3L);
        long resultNotFound = map.detectIfNone(LongPredicates.greaterThan(5L), 5L);
        Assert.assertEquals(5L, resultNotFound);

        Assert.assertEquals(5L, this.getEmptyMap().detectIfNone(LongPredicates.equal(0L), 5L));
        Assert.assertEquals(5L, this.newWithKeysValues(1L, 1L).detectIfNone(LongPredicates.equal(0L), 5L));
        Assert.assertEquals(1L, this.newWithKeysValues(1L, 1L).detectIfNone(LongPredicates.equal(1L), 5L));
        Assert.assertEquals(0L, map.detectIfNone(LongPredicates.equal(0L), 5L));
        Assert.assertEquals(1L, map.detectIfNone(LongPredicates.equal(1L), 5L));
        Assert.assertEquals(2L, map.detectIfNone(LongPredicates.equal(2L), 5L));
    }

    @Test
    public void anySatisfy()
    {
        LongLongMap map = this.newWithKeysValues(0L, 0L, 1L, 1L, 2L, 2L, 3L, 3L);
        Assert.assertFalse(this.getEmptyMap().anySatisfy(LongPredicates.equal(0L)));
        Assert.assertFalse(this.newWithKeysValues(1L, 1L).anySatisfy(LongPredicates.equal(0L)));
        Assert.assertTrue(this.newWithKeysValues(1L, 1L).anySatisfy(LongPredicates.equal(1L)));
        Assert.assertTrue(map.anySatisfy(LongPredicates.equal(0L)));
        Assert.assertTrue(map.anySatisfy(LongPredicates.equal(1L)));
        Assert.assertTrue(map.anySatisfy(LongPredicates.equal(2L)));
        Assert.assertFalse(map.anySatisfy(LongPredicates.greaterThan(5L)));
    }

    @Test
    public void allSatisfy()
    {
        LongLongMap map = this.newWithKeysValues(0L, 0L, 1L, 1L, 2L, 2L, 3L, 3L);
        Assert.assertTrue(this.getEmptyMap().allSatisfy(LongPredicates.equal(0L)));
        Assert.assertFalse(this.newWithKeysValues(1L, 1L).allSatisfy(LongPredicates.equal(0L)));
        Assert.assertTrue(this.newWithKeysValues(1L, 1L).allSatisfy(LongPredicates.equal(1L)));
        Assert.assertFalse(map.allSatisfy(LongPredicates.equal(0L)));
        Assert.assertFalse(map.allSatisfy(LongPredicates.equal(1L)));
        Assert.assertFalse(map.allSatisfy(LongPredicates.equal(2L)));
        Assert.assertTrue(map.allSatisfy(LongPredicates.lessThan(5L)));
        LongLongMap map1 = this.newWithKeysValues(2L, 2L, 3L, 3L);
        Assert.assertFalse(map1.allSatisfy(LongPredicates.equal(0L)));
    }

    @Test
    public void noneSatisfy()
    {
        LongLongMap map = this.newWithKeysValues(0L, 0L, 1L, 1L, 2L, 2L, 3L, 3L);
        Assert.assertTrue(this.getEmptyMap().noneSatisfy(LongPredicates.equal(0L)));
        Assert.assertTrue(this.newWithKeysValues(1L, 1L).noneSatisfy(LongPredicates.equal(0L)));
        Assert.assertFalse(this.newWithKeysValues(1L, 1L).noneSatisfy(LongPredicates.equal(1L)));
        Assert.assertFalse(map.noneSatisfy(LongPredicates.equal(0L)));
        Assert.assertFalse(map.noneSatisfy(LongPredicates.equal(1L)));
        Assert.assertFalse(map.noneSatisfy(LongPredicates.equal(2L)));
        Assert.assertTrue(map.noneSatisfy(LongPredicates.lessThan(0L)));
    }

    @Test
    public void max()
    {
        LongLongMap map = this.newWithKeysValues(0L, 0L, 1L, 1L, 2L, 2L, 3L, 3L);
        Assert.assertEquals(3L, map.max());
        Assert.assertEquals(3L, this.newWithKeysValues(3L, 3L).max());
    }

    @Test
    public void min()
    {
        LongLongMap map = this.newWithKeysValues(1L, 1L, 2L, 2L, 3L, 3L, 0L, 0L);
        Assert.assertEquals(0L, map.min());
        Assert.assertEquals(3L, this.newWithKeysValues(3L, 3L).min());
    }

    @Test(expected = NoSuchElementException.class)
    public void max_empty_throws()
    {
        this.getEmptyMap().max();
    }

    @Test(expected = NoSuchElementException.class)
    public void min_empty_throws()
    {
        this.getEmptyMap().min();
    }

    @Test
    public void minIfEmpty()
    {
        Assert.assertEquals(5L, this.getEmptyMap().minIfEmpty(5L));
        Assert.assertEquals(0L, this.getEmptyMap().minIfEmpty(0L));
        LongLongMap map = this.newWithKeysValues(1L, 1L, 0L, 0L, 9L, 9L, 7L, 7L);
        Assert.assertEquals(0L, map.minIfEmpty(5L));
        Assert.assertEquals(3L, this.newWithKeysValues(3L, 3L).maxIfEmpty(5L));
    }

    @Test
    public void maxIfEmpty()
    {
        Assert.assertEquals(5L, this.getEmptyMap().maxIfEmpty(5L));
        Assert.assertEquals(0L, this.getEmptyMap().maxIfEmpty(0L));
        LongLongMap map = this.newWithKeysValues(1L, 1L, 0L, 0L, 9L, 9L, 7L, 7L);
        Assert.assertEquals(9L, map.maxIfEmpty(5L));
        Assert.assertEquals(3L, this.newWithKeysValues(3L, 3L).minIfEmpty(5L));
    }

    @Test
    public void sum()
    {
        LongLongMap map = this.newWithKeysValues(0L, 0L, 1L, 1L, 2L, 2L, 3L, 3L);
        Assert.assertEquals(6L, map.sum());
        LongLongMap map2 = this.newWithKeysValues(2L, 2L, 3L, 3L, 4L, 4L);
        Assert.assertEquals(9L, map2.sum());
        LongLongMap map3 = this.newWithKeysValues(2L, 2L);
        Assert.assertEquals(2L, map3.sum());
    }

    @Test
    public void average()
    {
        LongLongMap map = this.newWithKeysValues(0L, 0L, 1L, 1L, 2L, 2L, 3L, 3L);
        Assert.assertEquals(1.5, map.average(), 0.0);
        LongLongMap map1 = this.newWithKeysValues(1L, 1L);
        Assert.assertEquals(1.0, map1.average(), 0.0);
    }

    @Test(expected = ArithmeticException.class)
    public void averageThrowsOnEmpty()
    {
        this.getEmptyMap().average();
    }

    @Test
    public void median()
    {
        LongLongMap map = this.newWithKeysValues(0L, 0L, 1L, 1L, 2L, 2L, 3L, 3L);
        Assert.assertEquals(1.5, map.median(), 0.0);
        LongLongMap map2 = this.newWithKeysValues(0L, 0L, 1L, 1L, 2L, 2L);
        Assert.assertEquals(1.0, map2.median(), 0.0);
        LongLongMap map3 = this.newWithKeysValues(1L, 1L);
        Assert.assertEquals(1.0, map3.median(), 0.0);
    }

    @Test(expected = ArithmeticException.class)
    public void medianThrowsOnEmpty()
    {
        this.getEmptyMap().median();
    }

    @Test
    public void toList()
    {
        Assert.assertEquals(LongArrayList.newListWith(0L), this.newWithKeysValues(0L, 0L).toList());
        Assert.assertEquals(LongArrayList.newListWith(1L), this.newWithKeysValues(1L, 1L).toList());
        Assert.assertEquals(LongArrayList.newListWith(2L), this.newWithKeysValues(2L, 2L).toList());
        Assert.assertTrue(this.newWithKeysValues(2L, 2L, 3L, 3L).toList().equals(LongArrayList.newListWith(2L, 3L))
                || this.newWithKeysValues(2L, 2L, 3L, 3L).toList().equals(LongArrayList.newListWith(3L, 2L)));
    }

    @Test
    public void toSortedList()
    {
        LongLongMap map = this.newWithKeysValues(0L, 0L, 1L, 1L, 2L, 2L, 3L, 3L);
        Assert.assertEquals(LongArrayList.newListWith(0L, 1L, 2L, 3L), map.toSortedList());
        Assert.assertEquals(LongArrayList.newListWith(), this.getEmptyMap().toSortedList());
        Assert.assertEquals(LongArrayList.newListWith(1L), this.newWithKeysValues(1L, 1L).toSortedList());
    }

    @Test
    public void toSet()
    {
        LongLongMap map = this.newWithKeysValues(0L, 0L, 1L, 1L, 2L, 2L, 3L, 3L);
        Assert.assertEquals(LongHashSet.newSetWith(0L, 1L, 2L, 3L), map.toSet());
        Assert.assertEquals(LongHashSet.newSetWith(), this.getEmptyMap().toSet());
        Assert.assertEquals(LongHashSet.newSetWith(1L), this.newWithKeysValues(1L, 1L).toSet());
    }

    @Test
    public void toBag()
    {
        LongLongMap map = this.newWithKeysValues(0L, 0L, 1L, 1L, 2L, 2L, 3L, 3L);
        Assert.assertEquals(LongHashBag.newBagWith(0L, 1L, 2L, 3L), map.toBag());
        Assert.assertEquals(LongHashBag.newBagWith(), this.getEmptyMap().toBag());
        Assert.assertEquals(LongHashBag.newBagWith(1L), this.newWithKeysValues(1L, 1L).toBag());
    }

    @Test
    public void longIterator()
    {
        MutableLongSet expected = LongHashSet.newSetWith(0L, 31L, 32L);
        MutableLongSet actual = LongHashSet.newSetWith();

        LongIterator iterator = this.map.longIterator();
        Assert.assertTrue(iterator.hasNext());
        actual.add(iterator.next());
        Assert.assertTrue(iterator.hasNext());
        actual.add(iterator.next());
        Assert.assertTrue(iterator.hasNext());
        actual.add(iterator.next());
        Assert.assertFalse(iterator.hasNext());

        Assert.assertEquals(expected, actual);
        Verify.assertThrows(NoSuchElementException.class, iterator::next);
        Verify.assertThrows(NoSuchElementException.class, () -> this.getEmptyMap().longIterator().next());
    }

    @Test
    public void asLazy()
    {
        LazyLongIterable lazy = this.map.asLazy();
        Assert.assertTrue(lazy.toList().containsAll(0L, 31L, 32L));
    }

    @Test
    public void keysView()
    {
        Assert.assertEquals(LongArrayList.newListWith(0L, 31L, 32L), this.map.keysView().toSortedList());
    }

    @Test
    public void keyValuesView()
    {
        if(TT.shortTest())
            return;

        MutableBag<LongLongPair> expected = Bags.mutable.of();
        this.map.forEachKeyValue((long key, long value) -> expected.add(PrimitiveTuples.pair(key, value)));
        Assert.assertEquals(expected, this.map.keyValuesView().toBag());
    }

    @Test
    public void toSortedArray()
    {
        Assert.assertTrue(Arrays.equals(new long[]{0L, 31L, 32L}, this.map.toSortedArray()));
    }

    @Test
    public void toArray()
    {
        LongLongMap map = this.newWithKeysValues(1L, 1L, 2L, 2L);
        long[] array = map.toArray();
        Assert.assertTrue(Arrays.equals(new long[]{1L, 2L}, array)
                || Arrays.equals(new long[]{2L, 1L}, array));
        Assert.assertEquals(0, this.getEmptyMap().toArray().length);
        Assert.assertTrue(Arrays.equals(new long[]{1L}, this.newWithKeysValues(1L, 1L).toArray()));
    }

    @Test
    public void toImmutable()
    {
        Assert.assertEquals(this.classUnderTest(), this.classUnderTest().toImmutable());
        Verify.assertInstanceOf(ImmutableLongLongMap.class, this.classUnderTest().toImmutable());
    }
}
