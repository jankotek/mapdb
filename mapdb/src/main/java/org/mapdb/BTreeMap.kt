package org.mapdb

import com.gs.collections.api.stack.primitive.LongStack
import com.gs.collections.impl.stack.mutable.primitive.LongArrayStack
import org.mapdb.BTreeMapJava.*
import java.util.*

/**
 * Concurrent sorted BTree Map
 */
class BTreeMap<K,V>(
        val keySerializer:Serializer<K>,
        val valueSerializer:Serializer<V>,
        val rootRecidRecid:Long,
        val store:Store,
        val maxNodeSize:Int
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
                        Serializer.RECID),
                maxNodeSize:Int=32) =
            BTreeMap(
                    keySerializer = keySerializer,
                    valueSerializer = valueSerializer,
                    store = store,
                    rootRecidRecid = rootRecidRecid,
                    maxNodeSize = maxNodeSize
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

    fun put(key:K, value:V):V?{
        var completed = false
        val stack = LongArrayStack()

        var current = store.get(rootRecidRecid, Serializer.RECID)
            ?: throw DBException.DataCorruption("rootRecid not found")
        var A = store.get(current, nodeSer)
            ?: throw DBException.DataCorruption("Node not found")
        while(A.isDir){
            var t = current
            current = findChild(A, COMPARATOR, key)
            if(current!=A.link){
                stack.push(t)
            }
            A = store.get(current, nodeSer)
                ?: throw DBException.DataCorruption("Node not found")
        }

        var level = 1
        do{
            var found:Boolean
            do{
                found = true
                lock(current)

                A = store.get(current, nodeSer)
                        ?: throw DBException.DataCorruption("Node not found")

                //follow link, until key is higher than highest key in node
                if(!A.isRightEdge && COMPARATOR.compare(key, A.keys[A.keys.size-1])>0){
                    //key is greater, load next link
                    unlock(current)
                    found = true
                    current = A.link
                    A = store.get(current, nodeSer)
                        ?: throw DBException.DataCorruption("Node not found")
                }
            }while(!found)

            //current node is locked, and its highest value is higher/equal to key
            var pos = findValue(A, COMPARATOR, key)
            if(pos>=0){
                pos = pos-1+A.intLeftEdge();
                //key exist in node, just update
                val values = (A.values as Array<Any>).clone()
                val oldValue = values[pos]
                values[pos] = value as Any;
                A = Node(A.flags.toInt(), A.link, A.keys, values)
                store.update(current, A, nodeSer)
                unlock(current)
                return oldValue as V
            }

            //key does not exist, node must be expanded, is it safe to insert without splitting?
            if(A.keys.size < maxNodeSize){
                A = copyAddKey(A, -pos-1, key, value)
                store.update(current, A, nodeSer)
                unlock(current)
                return null
            }

        }while(!completed)

        return null
    }

    private fun copyAddKey(a: Node, insertPos: Int, key: K, value: V): Node {
        val keys = arrayPut(a.keys, insertPos, key)

        val valuesInsertPos = insertPos-1+a.intLeftEdge();
        val values = arrayPut(a.values as Array<Any>, valuesInsertPos, value)

        return Node(a.flags.toInt(), a.link, keys, values)
    }


    fun lock(nodeRecid:Long){

    }

    fun unlock(nodeRecid:Long){

    }

}