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

import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.block.factory.primitive.LongPredicates;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;
import org.eclipse.collections.impl.test.Verify;
import org.junit.Assert;
import org.junit.Test;

/**
 * JUnit test for {@link LongLongHashMap#keySet()}.
 *
 * This file was automatically generated from template file primitivePrimitiveHashMapKeySetTest.stg.
 */
public abstract class LongLongHashMapKeySetTest extends AbstractLongSetTestCase
{
    @Override
    protected MutableLongSet classUnderTest()
    {
        return LongLongHashMap.newWithKeysValues(1L, 1L, 2L, 2L, 3L, 3L).keySet();
    }

    @Override
    protected MutableLongSet newWith(long... elements)
    {
        LongLongHashMap map = new LongLongHashMap();
        for (int i = 0; i < elements.length; i++)
        {
            map.put(elements[i], i);
        }
        return map.keySet();
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
    public void testEquals()
    {
        MutableLongSet set1 = this.newWith(1L, 31L, 32L);
        MutableLongSet set2 = this.newWith(32L, 31L, 1L);
        MutableLongSet set3 = this.newWith(32L, 32L, 31L, 1L);
        MutableLongSet set4 = this.newWith(32L, 32L, 31L, 1L, 1L);
        MutableLongSet set5 = this.newWith(32L, 1L);
        Verify.assertEqualsAndHashCode(set1, set2);
        Verify.assertEqualsAndHashCode(set1, set3);
        Verify.assertEqualsAndHashCode(set1, set4);
        Verify.assertEqualsAndHashCode(set2, set3);
        Verify.assertEqualsAndHashCode(set2, set4);
        Assert.assertNotEquals(set1, set5);
    }

    @Override
    @Test
    public void noneSatisfy()
    {
        super.noneSatisfy();
        Assert.assertFalse(this.newWith(0L, 1L, 2L).noneSatisfy(LongPredicates.equal(0L)));
    }

    @Override
    @Test
    public void sum()
    {
        super.sum();
        Assert.assertEquals(3L, this.newWith(0L, 1L, 2L).sum());
    }

    @Override
    public void testHashCode()
    {
        MutableLongSet set1 = this.newWith(0L, 1L, 31L, 32L);
        MutableLongSet set2 = this.newWith(32L, 31L, 1L, 0L);
        Assert.assertEquals(set1.hashCode(), set2.hashCode());
//        Assert.assertEquals(this.newObjectCollectionWith(0L, 1L, 31L, 32L).hashCode(), set1.hashCode());
    }
}
