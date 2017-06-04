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

import org.eclipse.collections.api.block.function.primitive.LongFunction;
import org.eclipse.collections.api.block.function.primitive.LongFunction0;
import org.eclipse.collections.api.block.function.primitive.LongToLongFunction;
import org.eclipse.collections.api.iterator.MutableLongIterator;
import org.eclipse.collections.api.map.primitive.MutableLongLongMap;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.SynchronizedLongLongMap;
import org.eclipse.collections.impl.map.mutable.primitive.UnmodifiableLongLongMap;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.eclipse.collections.impl.test.Verify;
import org.junit.Assert;
import org.junit.Test;

import java.util.NoSuchElementException;

/**
 * This file was automatically generated from template file abstractMutablePrimitivePrimitiveMapTestCase.stg.
 */
public abstract class AbstractMutableLongLongMapTestCase extends AbstractLongLongMapTestCase
{
    @Override
    protected abstract MutableLongLongMap classUnderTest();

    @Override
    protected abstract MutableLongLongMap newWithKeysValues(long key1, long value1);

    @Override
    protected abstract MutableLongLongMap newWithKeysValues(long key1, long value1, long key2, long value2);

    @Override
    protected abstract MutableLongLongMap newWithKeysValues(long key1, long value1, long key2, long value2, long key3, long value3);

    @Override
    protected abstract MutableLongLongMap newWithKeysValues(long key1, long value1, long key2, long value2, long key3, long value3, long key4, long value4);

    @Override
    protected abstract MutableLongLongMap getEmptyMap();

    @Override
    @Test
    public void get()
    {
        super.get();
        MutableLongLongMap map1 = this.classUnderTest();
        map1.put(0L, 1L);
        Assert.assertEquals(1L, map1.get(0L));

        map1.put(0L, 0L);
        Assert.assertEquals(0L, map1.get(0L));

        map1.put(5L, 5L);
        Assert.assertEquals(5L, map1.get(5L));

        map1.put(35L, 35L);
        Assert.assertEquals(35L, map1.get(35L));
    }

    @Override
    @Test
    public void getOrThrow()
    {
        super.getOrThrow();
        MutableLongLongMap map1 = this.classUnderTest();
        map1.removeKey(0L);
        Verify.assertThrows(IllegalStateException.class, () -> map1.getOrThrow(0L));
        map1.put(0L, 1L);
        Assert.assertEquals(1L, map1.getOrThrow(0L));

        map1.put(1L, 1L);
        Assert.assertEquals(1L, map1.getOrThrow(1L));

        map1.put(5L, 5L);
        Assert.assertEquals(5L, map1.getOrThrow(5L));

        map1.put(35L, 35L);
        Assert.assertEquals(35L, map1.getOrThrow(35L));
    }

    @Override
    @Test
    public void getIfAbsent()
    {
        super.getIfAbsent();
        MutableLongLongMap map1 = this.classUnderTest();
        map1.removeKey(0L);
        Assert.assertEquals(5L, map1.getIfAbsent(0L, 5L));

        Assert.assertEquals(6L, map1.getIfAbsent(1L, 6L));
        Assert.assertEquals(6L, map1.getIfAbsent(33L, 6L));

        map1.put(0L, 1L);
        Assert.assertEquals(1L, map1.getIfAbsent(0L, 5L));

        map1.put(1L, 1L);
        Assert.assertEquals(1L, map1.getIfAbsent(1L, 5L));

        map1.put(5L, 5L);
        Assert.assertEquals(5L, map1.getIfAbsent(5L, 6L));

        map1.put(35L, 35L);
        Assert.assertEquals(35L, map1.getIfAbsent(35L, 5L));
    }

    @Override
    @Test
    public void containsKey()
    {
        super.containsKey();
        MutableLongLongMap map1 = this.classUnderTest();
        map1.removeKey(0L);
        Assert.assertFalse(map1.containsKey(0L));
        Assert.assertEquals(0L, map1.get(0L));
        map1.removeKey(0L);
        Assert.assertFalse(map1.containsKey(0L));
        Assert.assertEquals(0L, map1.get(0L));

        map1.removeKey(1L);
        Assert.assertFalse(map1.containsKey(1L));
        Assert.assertEquals(0L, map1.get(1L));

        map1.removeKey(31L);
        Assert.assertFalse(map1.containsKey(31L));
        Assert.assertEquals(0L, map1.get(31L));

        map1.removeKey(32L);
        Assert.assertFalse(map1.containsKey(32L));
        Assert.assertEquals(0L, map1.get(32L));
    }

    @Override
    @Test
    public void containsValue()
    {
        super.containsValue();
        MutableLongLongMap map1 = this.classUnderTest();

        map1.put(35L, 35L);
        Assert.assertTrue(map1.containsValue(35L));

        map1.removeKey(0L);
        Assert.assertFalse(map1.containsValue(0L));
    }

    @Override
    @Test
    public void contains()
    {
        super.contains();
        MutableLongLongMap map1 = this.classUnderTest();

        map1.put(35L, 35L);
        Assert.assertTrue(map1.contains(35L));

        map1.removeKey(0L);
        Assert.assertFalse(map1.contains(0L));
    }

    @Override
    @Test
    public void size()
    {
        super.size();
        MutableLongLongMap hashMap1 = this.newWithKeysValues(1L, 1L, 0L, 0L);
        Assert.assertEquals(2, hashMap1.size());
        hashMap1.removeKey(1L);
        Assert.assertEquals(1, hashMap1.size());
        hashMap1.removeKey(0L);
        Assert.assertEquals(0, hashMap1.size());

        MutableLongLongMap hashMap = this.newWithKeysValues(6L, 6L, 5L, 5L);
        hashMap.removeKey(5L);
        Assert.assertEquals(1, hashMap.size());
    }

    protected static LongArrayList generateCollisions()
    {
        LongArrayList collisions = new LongArrayList();
        LongLongHashMap hashMap = new LongLongHashMap();
        for (long each = 2L; collisions.size() <= 10; each++)
        {
//            if (hashMap.spreadAndMask(each) == hashMap.spreadAndMask(2L))
            {
                collisions.add(each);
            }
        }
        return collisions;
    }

    @Test
    public void clear()
    {
        MutableLongLongMap map1 = this.classUnderTest();
        map1.clear();
        Assert.assertEquals(new LongLongHashMap(), map1);

        map1.put(1L, 0L);
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(1L, 0L), map1);
        map1.clear();
        Assert.assertEquals(new LongLongHashMap(), map1);

        map1.put(33L, 0L);
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(33L, 0L), map1);
        map1.clear();
        Assert.assertEquals(new LongLongHashMap(), map1);
    }

    @Test
    public void removeKey()
    {
        MutableLongLongMap map0 = this.newWithKeysValues(0L, 0L, 1L, 1L);
        map0.removeKey(1L);
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(0L, 0L), map0);
        map0.removeKey(0L);
        Assert.assertEquals(new LongLongHashMap(), map0);

        MutableLongLongMap map1 = this.newWithKeysValues(0L, 0L, 1L, 1L);
        map1.removeKey(0L);
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(1L, 1L), map1);
        map1.removeKey(1L);
        Assert.assertEquals(new LongLongHashMap(), map1);

        MutableLongLongMap map2 = this.classUnderTest();
        map2.removeKey(5L);
        map2.removeKey(50L);
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(0L, 0L, 31L, 31L, 32L, 32L), map2);
        map2.removeKey(0L);
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(31L, 31L, 32L, 32L), map2);
        map2.removeKey(31L);
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(32L, 32L), map2);
        map2.removeKey(32L);
        Assert.assertEquals(new LongLongHashMap(), map2);
        map2.removeKey(0L);
        map2.removeKey(31L);
        map2.removeKey(32L);
        Assert.assertEquals(new LongLongHashMap(), map2);
        Verify.assertEmpty(map2);

        map2.put(AbstractMutableLongLongMapTestCase.generateCollisions().get(0), 1L);
        map2.put(AbstractMutableLongLongMapTestCase.generateCollisions().get(1), 2L);

        Assert.assertEquals(1L, map2.get(AbstractMutableLongLongMapTestCase.generateCollisions().get(0)));
        map2.removeKey(AbstractMutableLongLongMapTestCase.generateCollisions().get(0));
        Assert.assertEquals(0L, map2.get(AbstractMutableLongLongMapTestCase.generateCollisions().get(0)));

        Assert.assertEquals(2L, map2.get(AbstractMutableLongLongMapTestCase.generateCollisions().get(1)));
        map2.removeKey(AbstractMutableLongLongMapTestCase.generateCollisions().get(1));
        Assert.assertEquals(0L, map2.get(AbstractMutableLongLongMapTestCase.generateCollisions().get(1)));
    }

    @Test
    public void remove()
    {
        MutableLongLongMap map0 = this.newWithKeysValues(0L, 0L, 1L, 1L);
        map0.remove(1L);
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(0L, 0L), map0);
        map0.remove(0L);
        Assert.assertEquals(new LongLongHashMap(), map0);

        MutableLongLongMap map1 = this.newWithKeysValues(0L, 0L, 1L, 1L);
        map1.remove(0L);
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(1L, 1L), map1);
        map1.remove(1L);
        Assert.assertEquals(new LongLongHashMap(), map1);

        MutableLongLongMap map2 = this.classUnderTest();
        map2.remove(5L);
        map2.remove(50L);
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(0L, 0L, 31L, 31L, 32L, 32L), map2);
        map2.remove(0L);
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(31L, 31L, 32L, 32L), map2);
        map2.remove(31L);
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(32L, 32L), map2);
        map2.remove(32L);
        Assert.assertEquals(new LongLongHashMap(), map2);
        map2.remove(0L);
        map2.remove(31L);
        map2.remove(32L);
        Assert.assertEquals(new LongLongHashMap(), map2);
        Verify.assertEmpty(map2);

        map2.put(AbstractMutableLongLongMapTestCase.generateCollisions().get(0), 1L);
        map2.put(AbstractMutableLongLongMapTestCase.generateCollisions().get(1), 2L);

        Assert.assertEquals(1L, map2.get(AbstractMutableLongLongMapTestCase.generateCollisions().get(0)));
        map2.remove(AbstractMutableLongLongMapTestCase.generateCollisions().get(0));
        Assert.assertEquals(0L, map2.get(AbstractMutableLongLongMapTestCase.generateCollisions().get(0)));

        Assert.assertEquals(2L, map2.get(AbstractMutableLongLongMapTestCase.generateCollisions().get(1)));
        map2.remove(AbstractMutableLongLongMapTestCase.generateCollisions().get(1));
        Assert.assertEquals(0L, map2.get(AbstractMutableLongLongMapTestCase.generateCollisions().get(1)));
    }

    @Test
    public void removeKeyIfAbsent()
    {
        MutableLongLongMap map0 = this.newWithKeysValues(0L, 0L, 1L, 1L);
        Assert.assertEquals(1L, map0.removeKeyIfAbsent(1L, 100L));
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(0L, 0L), map0);
        Assert.assertEquals(0L, map0.removeKeyIfAbsent(0L, 100L));
        Assert.assertEquals(new LongLongHashMap(), map0);
        Assert.assertEquals(100L, map0.removeKeyIfAbsent(1L, 100L));
        Assert.assertEquals(100L, map0.removeKeyIfAbsent(0L, 100L));

        MutableLongLongMap map1 = this.newWithKeysValues(0L, 0L, 1L, 1L);
        Assert.assertEquals(0L, map1.removeKeyIfAbsent(0L, 100L));
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(1L, 1L), map1);
        Assert.assertEquals(1L, map1.removeKeyIfAbsent(1L, 100L));
        Assert.assertEquals(new LongLongHashMap(), map1);
        Assert.assertEquals(100L, map1.removeKeyIfAbsent(0L, 100L));
        Assert.assertEquals(100L, map1.removeKeyIfAbsent(1L, 100L));

        MutableLongLongMap map2 = this.classUnderTest();
        Assert.assertEquals(100L, map2.removeKeyIfAbsent(5L, 100L));
        Assert.assertEquals(100L, map2.removeKeyIfAbsent(50L, 100L));
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(0L, 0L, 31L, 31L, 32L, 32L), map2);
        Assert.assertEquals(0L, map2.removeKeyIfAbsent(0L, 100L));
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(31L, 31L, 32L, 32L), map2);
        Assert.assertEquals(31L, map2.removeKeyIfAbsent(31L, 100L));
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(32L, 32L), map2);
        Assert.assertEquals(32L, map2.removeKeyIfAbsent(32L, 100L));
        Assert.assertEquals(new LongLongHashMap(), map2);
        Assert.assertEquals(100L, map2.removeKeyIfAbsent(0L, 100L));
        Assert.assertEquals(100L, map2.removeKeyIfAbsent(31L, 100L));
        Assert.assertEquals(100L, map2.removeKeyIfAbsent(32L, 100L));
        Assert.assertEquals(new LongLongHashMap(), map2);
        Verify.assertEmpty(map2);

        map2.put(AbstractMutableLongLongMapTestCase.generateCollisions().get(0), 1L);
        map2.put(AbstractMutableLongLongMapTestCase.generateCollisions().get(1), 2L);

        Assert.assertEquals(1L, map2.get(AbstractMutableLongLongMapTestCase.generateCollisions().get(0)));
        Assert.assertEquals(1L, map2.removeKeyIfAbsent(AbstractMutableLongLongMapTestCase.generateCollisions().get(0), 100L));
        Assert.assertEquals(0L, map2.get(AbstractMutableLongLongMapTestCase.generateCollisions().get(0)));

        Assert.assertEquals(2L, map2.get(AbstractMutableLongLongMapTestCase.generateCollisions().get(1)));
        Assert.assertEquals(2L, map2.removeKeyIfAbsent(AbstractMutableLongLongMapTestCase.generateCollisions().get(1), 100L));
        Assert.assertEquals(0L, map2.get(AbstractMutableLongLongMapTestCase.generateCollisions().get(1)));
    }

    @Test
    public void put()
    {
        MutableLongLongMap map1 = this.classUnderTest();
        map1.put(0L, 1L);
        map1.put(31L, 32L);
        map1.put(32L, 33L);
        LongLongHashMap expected = LongLongHashMap.newWithKeysValues(0L, 1L, 31L, 32L, 32L, 33L);
        Assert.assertEquals(expected, map1);

        map1.put(1L, 2L);
        expected.put(1L, 2L);
        Assert.assertEquals(expected, map1);

        map1.put(33L, 34L);
        expected.put(33L, 34L);
        Assert.assertEquals(expected, map1);

        map1.put(30L, 31L);
        expected.put(30L, 31L);
        Assert.assertEquals(expected, map1);
    }

    @Test
    public void addToValue()
    {
        MutableLongLongMap map1 = this.getEmptyMap();
        Assert.assertEquals(1L, map1.addToValue(0L, 1L));
        Assert.assertEquals(32L, map1.addToValue(31L, 32L));
        Assert.assertEquals(3L, map1.addToValue(1L, 3L));
        Assert.assertEquals(11L, map1.addToValue(0L, 10L));
        Assert.assertEquals(12L, map1.addToValue(1L, 9L));
        Assert.assertEquals(37L, map1.addToValue(31L, 5L));
        Assert.assertEquals(33L, map1.addToValue(32L, 33L));
        LongLongHashMap expected = LongLongHashMap.newWithKeysValues(0L, 11L, 1L, 12L, 31L, 37L, 32L, 33L);
        Assert.assertEquals(expected, map1);

        map1.removeKey(0L);
        map1.removeKey(1L);
        map1.removeKey(31L);
        map1.removeKey(32L);
        Assert.assertEquals(5L, map1.addToValue(31L, 5L));
        Assert.assertEquals(37L, map1.addToValue(31L, 32L));
        Assert.assertEquals(33L, map1.addToValue(32L, 33L));
        Assert.assertEquals(3L, map1.addToValue(1L, 3L));
        Assert.assertEquals(1L, map1.addToValue(0L, 1L));
        Assert.assertEquals(12L, map1.addToValue(1L, 9L));
        Assert.assertEquals(11L, map1.addToValue(0L, 10L));
        Assert.assertEquals(expected, map1);
    }

    @Test
    public void put_every_slot()
    {
        LongLongHashMap hashMap = new LongLongHashMap();
        for (int i = 2; i < 100; i++)
        {
            Assert.assertEquals(0L, hashMap.get((long) i));
            hashMap.put((long) i, (long) i);
            Assert.assertEquals((long) i, hashMap.get((long) i));
            hashMap.remove((long) i);
            Assert.assertEquals(0L, hashMap.get((long) i));
        }
    }

    @Test
    public void putDuplicateWithRemovedSlot()
    {
        long collision1 = AbstractMutableLongLongMapTestCase.generateCollisions().getFirst();
        long collision2 = AbstractMutableLongLongMapTestCase.generateCollisions().get(1);
        long collision3 = AbstractMutableLongLongMapTestCase.generateCollisions().get(2);
        long collision4 = AbstractMutableLongLongMapTestCase.generateCollisions().get(3);

        MutableLongLongMap hashMap = this.getEmptyMap();
        hashMap.put(collision1, 1L);
        hashMap.put(collision2, 2L);
        hashMap.put(collision3, 3L);
        Assert.assertEquals(2L, hashMap.get(collision2));
        hashMap.removeKey(collision2);
        hashMap.put(collision4, 4L);
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(collision1, 1L, collision3, 3L, collision4, 4L), hashMap);

        MutableLongLongMap hashMap1 = this.getEmptyMap();
        hashMap1.put(collision1, 1L);
        hashMap1.put(collision2, 2L);
        hashMap1.put(collision3, 3L);
        Assert.assertEquals(1L, hashMap1.get(collision1));
        hashMap1.removeKey(collision1);
        hashMap1.put(collision4, 4L);
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(collision2, 2L, collision3, 3L, collision4, 4L), hashMap1);

        MutableLongLongMap hashMap2 = this.getEmptyMap();
        hashMap2.put(collision1, 1L);
        hashMap2.put(collision2, 2L);
        hashMap2.put(collision3, 3L);
        Assert.assertEquals(3L, hashMap2.get(collision3));
        hashMap2.removeKey(collision3);
        hashMap2.put(collision4, 4L);
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(collision1, 1L, collision2, 2L, collision4, 4L), hashMap2);
    }

    @Test
    public void getIfAbsentPut()
    {
        MutableLongLongMap map1 = this.getEmptyMap();
        Assert.assertEquals(50L, map1.getIfAbsentPut(0L, 50L));
        Assert.assertEquals(50L, map1.getIfAbsentPut(0L, 100L));
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(0L, 50L), map1);
        Assert.assertEquals(50L, map1.getIfAbsentPut(1L, 50L));
        Assert.assertEquals(50L, map1.getIfAbsentPut(1L, 100L));
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(0L, 50L, 1L, 50L), map1);

        MutableLongLongMap map2 = this.getEmptyMap();
        Assert.assertEquals(50L, map2.getIfAbsentPut(1L, 50L));
        Assert.assertEquals(50L, map2.getIfAbsentPut(1L, 100L));
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(1L, 50L), map2);
        Assert.assertEquals(50L, map2.getIfAbsentPut(0L, 50L));
        Assert.assertEquals(50L, map2.getIfAbsentPut(0L, 100L));
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(0L, 50L, 1L, 50L), map2);

        MutableLongLongMap map3 = this.getEmptyMap();
        Assert.assertEquals(50L, map3.getIfAbsentPut(32L, 50L));
        Assert.assertEquals(50L, map3.getIfAbsentPut(32L, 100L));
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(32L, 50L), map3);

        MutableLongLongMap map4 = this.getEmptyMap();
        Assert.assertEquals(50L, map4.getIfAbsentPut(33L, 50L));
        Assert.assertEquals(50L, map4.getIfAbsentPut(33L, 100L));
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(33L, 50L), map4);
    }

    @Test
    public void getIfAbsentPut_Function()
    {
        LongFunction0 factory = () -> 100L;
        LongFunction0 factoryThrows = () -> { throw new AssertionError(); };

        MutableLongLongMap map1 = this.getEmptyMap();
        Assert.assertEquals(100L, map1.getIfAbsentPut(0L, factory));
        Assert.assertEquals(100L, map1.getIfAbsentPut(0L, factoryThrows));
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(0L, 100L), map1);
        Assert.assertEquals(100L, map1.getIfAbsentPut(1L, factory));
        Assert.assertEquals(100L, map1.getIfAbsentPut(1L, factoryThrows));
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(0L, 100L, 1L, 100L), map1);

        MutableLongLongMap map2 = this.getEmptyMap();
        Assert.assertEquals(100L, map2.getIfAbsentPut(1L, factory));
        Assert.assertEquals(100L, map2.getIfAbsentPut(1L, factoryThrows));
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(1L, 100L), map2);
        Assert.assertEquals(100L, map2.getIfAbsentPut(0L, factory));
        Assert.assertEquals(100L, map2.getIfAbsentPut(0L, factoryThrows));
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(0L, 100L, 1L, 100L), map2);

        MutableLongLongMap map3 = this.getEmptyMap();
        Assert.assertEquals(100L, map3.getIfAbsentPut(32L, factory));
        Assert.assertEquals(100L, map3.getIfAbsentPut(32L, factoryThrows));
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(32L, 100L), map3);

        MutableLongLongMap map4 = this.getEmptyMap();
        Assert.assertEquals(100L, map4.getIfAbsentPut(33L, factory));
        Assert.assertEquals(100L, map4.getIfAbsentPut(33L, factoryThrows));
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(33L, 100L), map4);
    }

    @Test
    public void getIfAbsentPutWith()
    {
        LongFunction<String> functionLength = (String string) -> (long) string.length();
        LongFunction<String> functionThrows = (String string) -> { throw new AssertionError(); };

        MutableLongLongMap map1 = this.getEmptyMap();
        Assert.assertEquals(9L, map1.getIfAbsentPutWith(0L, functionLength, "123456789"));
        Assert.assertEquals(9L, map1.getIfAbsentPutWith(0L, functionThrows, "unused"));
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(0L, 9L), map1);
        Assert.assertEquals(9L, map1.getIfAbsentPutWith(1L, functionLength, "123456789"));
        Assert.assertEquals(9L, map1.getIfAbsentPutWith(1L, functionThrows, "unused"));
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(0L, 9L, 1L, 9L), map1);

        MutableLongLongMap map2 = this.getEmptyMap();
        Assert.assertEquals(9L, map2.getIfAbsentPutWith(1L, functionLength, "123456789"));
        Assert.assertEquals(9L, map2.getIfAbsentPutWith(1L, functionThrows, "unused"));
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(1L, 9L), map2);
        Assert.assertEquals(9L, map2.getIfAbsentPutWith(0L, functionLength, "123456789"));
        Assert.assertEquals(9L, map2.getIfAbsentPutWith(0L, functionThrows, "unused"));
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(0L, 9L, 1L, 9L), map2);

        MutableLongLongMap map3 = this.getEmptyMap();
        Assert.assertEquals(9L, map3.getIfAbsentPutWith(32L, functionLength, "123456789"));
        Assert.assertEquals(9L, map3.getIfAbsentPutWith(32L, functionThrows, "unused"));
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(32L, 9L), map3);

        MutableLongLongMap map4 = this.getEmptyMap();
        Assert.assertEquals(9L, map4.getIfAbsentPutWith(33L, functionLength, "123456789"));
        Assert.assertEquals(9L, map4.getIfAbsentPutWith(33L, functionThrows, "unused"));
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(33L, 9L), map4);
    }

    @Test
    public void getIfAbsentPutWithKey()
    {
        LongToLongFunction function = (long longParameter) -> (long) longParameter;
        LongToLongFunction functionThrows = (long longParameter) -> { throw new AssertionError(); };

        MutableLongLongMap map1 = this.getEmptyMap();
        Assert.assertEquals(0L, map1.getIfAbsentPutWithKey(0L, function));
        Assert.assertEquals(0L, map1.getIfAbsentPutWithKey(0L, functionThrows));
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(0L, 0L), map1);
        Assert.assertEquals(1L, map1.getIfAbsentPutWithKey(1L, function));
        Assert.assertEquals(1L, map1.getIfAbsentPutWithKey(1L, functionThrows));
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(0L, 0L, 1L, 1L), map1);

        MutableLongLongMap map2 = this.getEmptyMap();
        Assert.assertEquals(1L, map2.getIfAbsentPutWithKey(1L, function));
        Assert.assertEquals(1L, map2.getIfAbsentPutWithKey(1L, functionThrows));
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(1L, 1L), map2);
        Assert.assertEquals(0L, map2.getIfAbsentPutWithKey(0L, function));
        Assert.assertEquals(0L, map2.getIfAbsentPutWithKey(0L, functionThrows));
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(0L, 0L, 1L, 1L), map2);

        MutableLongLongMap map3 = this.getEmptyMap();
        Assert.assertEquals(32L, map3.getIfAbsentPutWithKey(32L, function));
        Assert.assertEquals(32L, map3.getIfAbsentPutWithKey(32L, functionThrows));
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(32L, 32L), map3);

        MutableLongLongMap map4 = this.getEmptyMap();
        Assert.assertEquals(33L, map4.getIfAbsentPutWithKey(33L, function));
        Assert.assertEquals(33L, map4.getIfAbsentPutWithKey(33L, functionThrows));
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(33L, 33L), map4);
    }

    @Test
    public void updateValue()
    {
        LongToLongFunction incrementFunction = (long value) -> value + 1L;

        MutableLongLongMap map1 = this.getEmptyMap();
        Assert.assertEquals(1L, map1.updateValue(0L, 0L, incrementFunction));
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(0L, 1L), map1);
        Assert.assertEquals(2L, map1.updateValue(0L, 0L, incrementFunction));
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(0L, 2L), map1);
        Assert.assertEquals(1L, map1.updateValue(1L, 0L, incrementFunction));
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(0L, 2L, 1L, 1L), map1);
        Assert.assertEquals(2L, map1.updateValue(1L, 0L, incrementFunction));
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(0L, 2L, 1L, 2L), map1);

        MutableLongLongMap map2 = this.getEmptyMap();
        Assert.assertEquals(1L, map2.updateValue(1L, 0L, incrementFunction));
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(1L, 1L), map2);
        Assert.assertEquals(2L, map2.updateValue(1L, 0L, incrementFunction));
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(1L, 2L), map2);
        Assert.assertEquals(1L, map2.updateValue(0L, 0L, incrementFunction));
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(0L, 1L, 1L, 2L), map2);
        Assert.assertEquals(2L, map2.updateValue(0L, 0L, incrementFunction));
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(0L, 2L, 1L, 2L), map2);

        MutableLongLongMap map3 = this.getEmptyMap();
        Assert.assertEquals(1L, map3.updateValue(33L, 0L, incrementFunction));
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(33L, 1L), map3);
        Assert.assertEquals(2L, map3.updateValue(33L, 0L, incrementFunction));
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(33L, 2L), map3);
    }

    @Test
    public void freeze()
    {
        MutableLongLongMap mutableLongLongMap = this.classUnderTest();
        LongSet frozenSet = mutableLongLongMap.keySet().freeze();
        LongSet frozenSetCopy = LongHashSet.newSetWith(mutableLongLongMap.keySet().toArray());
        Assert.assertEquals(frozenSet, frozenSetCopy);
        Assert.assertEquals(frozenSetCopy, mutableLongLongMap.keySet().freeze());
        for (int i = 0; i < 32; i++)
        {
            mutableLongLongMap.put((long) i, (long) i);
            Assert.assertEquals(frozenSet, frozenSetCopy);
        }

        LongSet frozenSetForRemove = mutableLongLongMap.keySet().freeze();
        LongSet frozenSetCopyForRemove = LongHashSet.newSetWith(mutableLongLongMap.keySet().toArray());
        Assert.assertEquals(frozenSetForRemove, frozenSetCopyForRemove);
        Assert.assertEquals(frozenSetCopyForRemove, mutableLongLongMap.keySet().freeze());
        for (int i = 0; i < 32; i++)
        {
            mutableLongLongMap.remove((long) i);
            Assert.assertEquals(frozenSetForRemove, frozenSetCopyForRemove);
        }

        MutableLongLongMap mutableLongLongMapForClear = this.classUnderTest();
        LongSet frozenSetForClear = mutableLongLongMapForClear.keySet().freeze();
        LongSet frozenSetCopyForClear = LongHashSet.newSetWith(mutableLongLongMapForClear.keySet().toArray());
        mutableLongLongMapForClear.clear();
        Assert.assertEquals(frozenSetForClear, frozenSetCopyForClear);
    }

    @Test
    public void withoutKey()
    {
        MutableLongLongMap map = this.newWithKeysValues(0L, 0L, 1L, 1L, 31L, 31L, 32L, 32L);
        MutableLongLongMap mapWithout = map.withoutKey(32L);
        Assert.assertSame(map, mapWithout);
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(0L, 0L, 1L, 1L, 31L, 31L), mapWithout);
    }

    @Test
    public void withoutAllKeys()
    {
        MutableLongLongMap map = this.newWithKeysValues(0L, 0L, 1L, 1L, 31L, 31L, 32L, 32L);
        MutableLongLongMap mapWithout = map.withoutAllKeys(LongArrayList.newListWith(0L, 32L));
        Assert.assertSame(map, mapWithout);
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(1L, 1L, 31L, 31L), mapWithout);
    }

    @Test
    public void withKeysValues()
    {
        MutableLongLongMap hashMap = this.getEmptyMap();
        Assert.assertSame(hashMap.withKeyValue(1L, 1L), hashMap);
        Assert.assertEquals(LongLongHashMap.newWithKeysValues(1L, 1L), hashMap);
    }

    @Test
    public void asSynchronized()
    {
        Verify.assertInstanceOf(SynchronizedLongLongMap.class, this.classUnderTest().asSynchronized());
//        Assert.assertEquals(new SynchronizedLongLongMap(this.classUnderTest()), this.classUnderTest().asSynchronized());
    }

    @Test
    public void asUnmodifiable()
    {
        Verify.assertInstanceOf(UnmodifiableLongLongMap.class, this.classUnderTest().asUnmodifiable());
//        Assert.assertEquals(new UnmodifiableLongLongMap(this.classUnderTest()), this.classUnderTest().asUnmodifiable());
    }

    @Test
    public void longIterator_with_remove()
    {
        MutableLongLongMap mutableMap = this.classUnderTest();
        MutableLongIterator iterator = mutableMap.longIterator();

        while (iterator.hasNext())
        {
            iterator.next();
            iterator.remove();
        }
        Assert.assertFalse(iterator.hasNext());
        Verify.assertEmpty(mutableMap);
        Verify.assertThrows(NoSuchElementException.class, iterator::next);
    }

    @Test
    public void iterator_throws_on_invocation_of_remove_before_next()
    {
        MutableLongIterator iterator = this.classUnderTest().longIterator();
        Assert.assertTrue(iterator.hasNext());
        Verify.assertThrows(IllegalStateException.class, iterator::remove);
    }

    @Test
    public void iterator_throws_on_consecutive_invocation_of_remove()
    {
        MutableLongIterator iterator = this.classUnderTest().longIterator();
        Assert.assertTrue(iterator.hasNext());
        iterator.next();
        iterator.remove();
        Verify.assertThrows(IllegalStateException.class, iterator::remove);
    }
}
