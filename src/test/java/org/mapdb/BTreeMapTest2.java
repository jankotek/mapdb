/******************************************************************************
 * Copyright 2010 Cees De Groot, Alex Boisvert, Jan Kotek
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package org.mapdb;

import java.util.concurrent.ConcurrentMap;

public class BTreeMapTest2 extends ConcurrentMapInterfaceTest<Integer, String> {

    protected boolean valsOutside = false;

    public static class Outside extends BTreeMapTest2{
        {
            valsOutside = true;
        }
    }

    public BTreeMapTest2() {
        super(false, false, true, true, true, true, false);
    }

    StoreDirect r;


    @Override
    protected void setUp() throws Exception {
        r = new StoreDirect(null);
        r.init();
    }

    @Override
    protected void tearDown() throws Exception {
        r.close();
    }

    @Override
    protected Integer getKeyNotInPopulatedMap() throws UnsupportedOperationException {
        return -100;
    }

    @Override
    protected String getValueNotInPopulatedMap() throws UnsupportedOperationException {
        return "XYZ";
    }

    @Override
    protected String getSecondValueNotInPopulatedMap() throws UnsupportedOperationException {
        return "AAAA";
    }

    @Override
    protected ConcurrentMap<Integer, String> makeEmptyMap() throws UnsupportedOperationException {

        return new BTreeMap(r,false,
                BTreeMap.createRootRef(r,BTreeKeySerializer.INTEGER, Serializer.STRING, valsOutside, 0),
                6,valsOutside,0, BTreeKeySerializer.INTEGER,Serializer.STRING,
                0);
    }

    @Override
    protected ConcurrentMap<Integer, String> makePopulatedMap() throws UnsupportedOperationException {
        ConcurrentMap<Integer, String> map = makeEmptyMap();
        for (int i = 0; i < 100; i++){
            map.put(i, "aa" + i);
        }
        return map;
    }

}
