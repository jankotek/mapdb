package org.mapdb

import org.mapdb.BTreeMapJava.*

/**
 * Concurrent sorted BTree Map
 */
class BTreeMap<K,V>(
        val keySerializer:Serializer<K>,
        val valueSerializer:Serializer<V>,
        val rootRecidRecid:Long,
        val store:Store
){

    companion object{
        fun <K,V> make(
                keySerializer:Serializer<K> = Serializer.JAVA as Serializer<K>,
                valueSerializer:Serializer<V> = Serializer.JAVA as Serializer<V>,
                store:Store = StoreTrivial(),
                rootRecidRecid: Long = //insert recid of new empty node
                    store.put(
                        store.put(
                            Node(LEFT+RIGHT, 0L, arrayOf(), arrayOf<Any>()),
                            NodeSerializer(keySerializer, valueSerializer)),
                        Serializer.RECID)) =
            BTreeMap(
                    keySerializer = keySerializer,
                    valueSerializer = valueSerializer,
                    store = store,
                    rootRecidRecid = rootRecidRecid
            )
    }

    internal val nodeSer = NodeSerializer(keySerializer, valueSerializer);

    operator fun get(key:K):V?{
        var current =  store.get(rootRecidRecid, Serializer.RECID)
            ?: throw DBException.DataCorruption("Root Recid not found");
        var A = store.get(current, nodeSer)
            ?: throw DBException.DataCorruption("Referenced node not found");

        //dive into bottom
        while(A.isDir){
            current =  findChild(A, COMPARATOR, key)
            A = store.get(current, nodeSer)
                ?: throw DBException.DataCorruption("Referenced node not found");
        }

        //follow link until necessary
        var ret = leafGet(A,COMPARATOR, key)
        while(LINK==ret){
            current = A.link;
            A = store.get(current, nodeSer)
                    ?: throw DBException.DataCorruption("Referenced node not found");
            ret = leafGet(A,COMPARATOR, key)
        }
        return ret as V?;
    }

}