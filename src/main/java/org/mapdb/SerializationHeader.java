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

/**
 * Header byte, is used at start of each record to indicate data type
 * WARNING !!! values bellow must be unique !!!!!
 *
 * @author Jan Kotek
 */
interface SerializationHeader {

    int  NULL = 0;
    int  POJO = 1;
    int  BOOLEAN_TRUE = 2;
    int  BOOLEAN_FALSE = 3;
    int  INTEGER_MINUS_1 = 4;
    int  INTEGER_0 = 5;
    int  INTEGER_1 = 6;
    int  INTEGER_2 = 7;
    int  INTEGER_3 = 8;
    int  INTEGER_4 = 9;
    int  INTEGER_5 = 10;
    int  INTEGER_6 = 11;
    int  INTEGER_7 = 12;
    int  INTEGER_8 = 13;
    int  INTEGER_255 = 14;
    int  INTEGER_PACK_NEG = 15;
    int  INTEGER_PACK = 16;
    int  LONG_MINUS_1 = 17;
    int  LONG_0 = 18;
    int  LONG_1 = 19;
    int  LONG_2 = 20;
    int  LONG_3 = 21;
    int  LONG_4 = 22;
    int  LONG_5 = 23;
    int  LONG_6 = 24;
    int  LONG_7 = 25;
    int  LONG_8 = 26;
    int  LONG_PACK_NEG = 27;
    int  LONG_PACK = 28;
    int  LONG_255 = 29;
    int  LONG_MINUS_MAX = 30;
    int  SHORT_MINUS_1 = 31;
    int  SHORT_0 = 32;
    int  SHORT_1 = 33;
    int  SHORT_255 = 34;
    int  SHORT_FULL = 35;
    int  BYTE_MINUS_1 = 36;
    int  BYTE_0 = 37;
    int  BYTE_1 = 38;
    int  BYTE_FULL = 39;
    int  CHAR = 40;
    int  FLOAT_MINUS_1 = 41;
    int  FLOAT_0 = 42;
    int  FLOAT_1 = 43;
    int  FLOAT_255 = 44;
    int  FLOAT_SHORT = 45;
    int  FLOAT_FULL = 46;
    int  DOUBLE_MINUS_1 = 47;
    int  DOUBLE_0 = 48;
    int  DOUBLE_1 = 49;
    int  DOUBLE_255 = 50;
    int  DOUBLE_SHORT = 51;
    int  DOUBLE_FULL = 52;
    int  DOUBLE_ARRAY = 53;
    int  BIGDECIMAL = 54;
    int  BIGINTEGER = 55;
    int  FLOAT_ARRAY = 56;
    int  INTEGER_MINUS_MAX = 57;
    int  SHORT_ARRAY = 58;
    int  BOOLEAN_ARRAY = 59;

    int  ARRAY_INT_B_255 = 60;
    int  ARRAY_INT_B_INT = 61;
    int  ARRAY_INT_S = 62;
    int  ARRAY_INT_I = 63;
    int  ARRAY_INT_PACKED = 64;

    int  ARRAY_LONG_B = 65;
    int  ARRAY_LONG_S = 66;
    int  ARRAY_LONG_I = 67;
    int  ARRAY_LONG_L = 68;
    int  ARRAY_LONG_PACKED = 69;

    int  CHAR_ARRAY = 70;
    int ARRAY_BYTE = 71;
    int ARRAY_BYTE_ALL_EQUAL = 72;

    int  ARRAY_OBJECT = 73;
    //special cases for BTree values which stores references
    int  ARRAY_OBJECT_PACKED_LONG = 74;
    int  ARRAYLIST_PACKED_LONG = 75;
    int ARRAY_OBJECT_ALL_NULL = 76;
    int ARRAY_OBJECT_NO_REFS = 77;

    int  STRING_EMPTY = 101;
    int  STRING = 103;

    int  ARRAYLIST = 105;


    int  TREEMAP = 107;
    int  UUID = 108;
    int  HASHMAP = 109;

    int  LINKEDHASHMAP = 111;


    int  TREESET = 113;

    int  HASHSET = 115;

    int  LINKEDHASHSET = 117;

    int  LINKEDLIST = 119;

    int  SERIALIZER_COMPRESSION_WRAPPER = 120;

    int  VECTOR = 121;
    int  IDENTITYHASHMAP = 122;
    int  HASHTABLE = 123;
    int  LOCALE = 124;
    int  PROPERTIES = 125;

    int  CLASS = 126;
    int  DATE = 127;
    int FUN_HI = 128;

    int STRING_SERIALIZER = 129;
    int COMPARABLE_COMPARATOR = 130;
    int COMPARABLE_COMPARATOR_WITH_NULLS = 131;
    int BASIC_SERIALIZER = 132;
    int THIS_SERIALIZER = 133;

    int TUPLE2 = 134;
    int TUPLE3 = 135;
    int TUPLE4 = 136;
    int B_TREE_MAP_ROOT_HEADER = 137;
    int B_TREE_NODE_LEAF_LR = 138;
    int B_TREE_NODE_LEAF_L = 139;
    int B_TREE_NODE_LEAF_R = 140;
    int B_TREE_NODE_LEAF_C = 141;
    int B_TREE_NODE_DIR_LR = 142;
    int B_TREE_NODE_DIR_L = 143;
    int B_TREE_NODE_DIR_R = 144;
    int B_TREE_NODE_DIR_C = 145;
    int B_TREE_BASIC_KEY_SERIALIZER = 146;


    int B_TREE_SERIALIZER_POS_LONG = 147;
    int B_TREE_SERIALIZER_STRING = 148;
    int B_TREE_SERIALIZER_POS_INT = 149;
    int LONG_SERIALIZER = 150;
    int INTEGER_SERIALIZER = 151;
    int EMPTY_SERIALIZER = 152;
    int CRC32_SERIALIZER = 153;
    int MAPDB_STACK = 154;
    int MAPDB_QUEUE = 155;
    int MAPDB_CIRCULAR_QUEUE = 156;

    /**
     * used for reference to already serialized object in object graph
     */
    int OBJECT_STACK = 166;

    int JAVA_SERIALIZATION = 172;



}
