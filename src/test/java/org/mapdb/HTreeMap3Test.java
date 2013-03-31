/*******************************************************************************
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

public class HTreeMap3Test extends ConcurrentMapInterfaceTest<Integer, String> {

    public HTreeMap3Test() {
        super(false, false, true, true, true, true,true);
    }

    StoreDirect r = new StoreDirect(Volume.memoryFactory(false));

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
        return new HTreeMap<Integer,String>(r,true,false,0,null, null, null);
    }

    @Override
    protected ConcurrentMap<Integer, String> makePopulatedMap() throws UnsupportedOperationException {
        ConcurrentMap<Integer, String> map = makeEmptyMap();
        for (int i = 0; i < 100; i++)
            map.put(i, "aa" + i);
        return map;
    }

}
