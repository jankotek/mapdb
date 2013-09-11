/*
 * Copyright 2013 Jacob Dilles <jsdilles@gmail.com>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.mapdb;

import org.junit.Test;
import org.mapdb.perf.SerializerPerformance;
import static org.mapdb.perf.ObjectGenerators.*;

/**
 * This has to be in the base package due to package-private access in
 * Serializers
 *
 * @author Jacob Dilles
 */
public class SerializerPerformanceTest {

//    @Test
    public void basicSerializerPerformanceTest() {
        SerializerPerformance
                .testWith("S-equalDistro", randomInts(10 * 1000))
                .concurrentThreads(5)
                .testSerialization("Serializer.INTEGER ", Serializer.INTEGER)
                .testSerialization("SerializerBase     ", new SerializerBase())
                .testSerialization("V0.Integer.class   ", Serializers.wrap(null, Serializers.IntegerSerializer.INSTANCE_V0))
                .testSerialization("V1.Integer.class   ", Serializers.wrap(null, Serializers.IntegerSerializer.INSTANCE_V1))
                .testSerialization("V2.Integer.class   ", Serializers.wrap(null, Serializers.IntegerSerializer.INSTANCE_V2))
                
                .start();
    }
    
    @Test
    public void basicDeSerializerPerformanceTest() {
        SerializerPerformance
                .testWith("D-equalDistro", randomInts(10 * 1000))
                
//                .testDeSerialization("Serializer.INTEGER ", Serializer.INTEGER)
                .referenceSerializer(new SerializerBase())
                .testDeSerialization("V0.Integer.class   ",  Serializers.auto())
                
                .start();
    }


//    @Test
    public void small16_16_IntSerializerPerformanceTest() {
        SerializerPerformance
                .testWith("Small (0-16)", randomInts(10 * 1000, 8, 8))
                .concurrentThreads(5)
                .testSerialization("Serializer.INTEGER ", Serializer.INTEGER)
                .testSerialization("SerializerBase     ", new SerializerBase())
                .testSerialization("V0.Integer.class   ", Serializers.wrap(null, Serializers.IntegerSerializer.INSTANCE_V0))
                .testSerialization("V1.Integer.class   ", Serializers.wrap(null, Serializers.IntegerSerializer.INSTANCE_V1))
                .testSerialization("V2.Integer.class   ", Serializers.wrap(null, Serializers.IntegerSerializer.INSTANCE_V2))
                .start();
    }
    
}
