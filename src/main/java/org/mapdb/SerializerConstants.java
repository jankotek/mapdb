/*
 *  Copyright (c) 2012 Jan Kotek
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.mapdb;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * collection of known serializers
 */
public class SerializerConstants {

    static final Set knownSerializable = new HashSet(Arrays.asList(
            BTreeKeySerializer.STRING,
            BTreeKeySerializer.ZERO_OR_POSITIVE_LONG,
            BTreeKeySerializer.ZERO_OR_POSITIVE_INT,

            Utils.COMPARABLE_COMPARATOR, Utils.COMPARABLE_COMPARATOR_WITH_NULLS,

            Serializer.STRING_SERIALIZER, Serializer.LONG_SERIALIZER, Serializer.INTEGER_SERIALIZER,
            Serializer.EMPTY_SERIALIZER, Serializer.BASIC_SERIALIZER, Serializer.CRC32_CHECKSUM
    ));

}
