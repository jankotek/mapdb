package org.mapdb

import java.util.*

/**
 * Read only Sorted Table Map. It stores data in table and uses binary search to find records
 */
class SortedTableMap<K,V>(
        val keySerializer : Serializer<K>,
        val valueSerializer : Serializer<V>,
        val pageSize:Int,
        internal val volume:Volume
):Map<K?, V?> {

    abstract class Consumer<K,V>:Pump.Consumer<Pair<K,V>, SortedTableMap<K,V>>(){}

    companion object{
        fun <K,V> import(
                keySerializer:Serializer<K>,
                valueSerializer:Serializer<V>,
                volume: Volume,
                pageSize:Int = CC.PAGE_SIZE.toInt(),
                nodeSize:Int = CC.BTREEMAP_MAX_NODE_SIZE
        ):Consumer<K,V> {
            return object:Consumer<K,V>(){

                val bytes = ByteArray(pageSize)

                val nodeKeys = ArrayList<ByteArray>()
                val nodeVals = ArrayList<ByteArray>()

                val pairs = ArrayList<Pair<K,V>>()
                var nodesSize = start;
                var fileTail = 0L

                override fun take(e: Pair<K, V>) {
                    pairs.add(e)
                    counter++
                    if(pairs.size<nodeSize)
                        return
                    pairsToNodes()
                }

                override fun finish():SortedTableMap<K,V> {
                    pairsToNodes()
                    //there is a chance it overflowed to next page
                    if(nodeKeys.isEmpty().not()) {
                        flushPage()
                    }
                    volume.putLong(SIZE_OFFSET, counter)
                    volume.sync()
                    return SortedTableMap(
                            keySerializer = keySerializer,
                            valueSerializer = valueSerializer,
                            pageSize =  pageSize,
                            volume = volume
                    )
                }

                fun pairsToNodes(){
                    // serialize pairs into nodes
                    val keys = pairs.map{it.first}.toArrayList().toArray()
                    val out = DataOutput2()
                    out.packInt(keys.size)
                    keySerializer.valueArraySerialize(out, keySerializer.valueArrayFromArray(keys))
                    val binaryKeys = out.copyBytes()

                    val values = pairs.map{it.second}.toArrayList().toArray()
                    out.pos = 0
                    valueSerializer.valueArraySerialize(out, valueSerializer.valueArrayFromArray(values))
                    val binaryVals = out.copyBytes()

                    pairs.clear()

                    // if size does not overflow
                    val newNodesSize = nodesSize+8+binaryKeys.size+binaryVals.size
                    if(newNodesSize < pageSize){
                        nodesSize = newNodesSize
                        nodeKeys.add(binaryKeys)
                        nodeVals.add(binaryVals)
                        return
                    }

                    // flush current nodes into page,
                    // the current node is not included (it would overflow page)
                    flushPage()

                    // clear everything and start over with current record
                    nodesSize = 4 + 8 + binaryKeys.size + binaryVals.size
                    nodeKeys.add(binaryKeys)
                    nodeVals.add(binaryVals)
                }

                fun flushPage(){
                    if(nodeKeys.isEmpty())
                        return
                    val bytes = bytes
                    val headSize = if(fileTail==0L) start else 0
                    var intPos = headSize
                    DataIO.putInt(bytes, intPos, nodeKeys.size)
                    intPos+=4
                    var pos = headSize + 4 + 2 * 4 * nodeKeys.size;

                    for(array in arrayOf(nodeKeys, nodeVals))
                    for(bb in array){
                        DataIO.putInt(bytes, intPos, pos)
                        if(pos+bb.size>bytes.size)
                            throw AssertionError()
                        System.arraycopy(bb, 0, bytes, pos, bb.size)
                        intPos+=4
                        pos+=bb.size
                    }
                    //clear rest of the volume
                    while(pos<pageSize){
                        bytes[pos++] = 0
                    }

                    if(CC.ASSERT && intPos != headSize + 4 + 2 * 4 * nodeKeys.size)
                        throw AssertionError()

                    //append to volume
                    volume.ensureAvailable(fileTail+pageSize)
                    volume.putData(fileTail, bytes, 0, bytes.size)
                    fileTail+=pageSize
                    nodeKeys.clear()
                    nodeVals.clear()

                }
            }
        }

        private val SIZE_OFFSET = 16L;
        private val PAGE_COUNT_OFFSET = 24L;
        private val PAGE_SIZE_OFFSET = 32L;

        private val start = 64;
    }

    val comparator = keySerializer

    /** first key at beginning of each page */
    internal val pageKeys:Any = {
        val vollen = volume.length()
        val keys = ArrayList<K>()
        for(i in 0 until vollen step pageSize.toLong()){
            val ii:Long = if(i==0L) start.toLong() else i
            val offset = i+volume.getInt(ii+4)
            val size = (i+volume.getInt(ii+8) - offset).toInt()
            val input = volume.getDataInput(offset, size);
            val keysSize = input.unpackInt()
            val key = keySerializer.valueArrayBinaryGet(input, keysSize, 0)
            keys.add(key)
        }
        keySerializer.valueArrayFromArray(keys.toArray())
    }()

    override fun containsKey(key: K?): Boolean {
        return get(key)!=null
    }

    override fun containsValue(value: V?): Boolean {
        throw UnsupportedOperationException()
    }


    override fun get(key: K?): V? {
        if(key==null)
            throw NullPointerException()

        var keyPos = keySerializer.valueArraySearch(pageKeys, key)
        if(keyPos<0)
            keyPos = -keyPos-2

        val headSize = if(keyPos==0) start else 0
        val offset = (keyPos*pageSize).toLong()
        val offsetWithHead = offset+headSize;
        val nodeCount =  volume.getInt(offsetWithHead)

        //run binary search on first keys on each node
        var pos = nodeSearch(key, offset, offsetWithHead, nodeCount)
        if(pos<0)
            pos = -pos-2

        //search in keys at pos
        val keysOffset = offset+volume.getInt(offsetWithHead+4+pos*4)
        val keysBinarySize = offset + volume.getInt(offsetWithHead+4+pos*4+4) - keysOffset
        val di = volume.getDataInput(keysOffset, keysBinarySize.toInt())
        val keysSize = di.unpackInt()
        val valuePos = keySerializer.valueArrayBinarySearch(key, di, keysSize, comparator )

        if(valuePos<0)
            return null

        val valOffset = offset + volume.getInt(offsetWithHead+4+(pos+nodeCount)*4)
        val valsBinarySize = offset + volume.getInt(offsetWithHead+4+(pos+nodeCount+1)*4) - valOffset
        val di2 = volume.getDataInput(valOffset, valsBinarySize.toInt())
        return valueSerializer.valueArrayBinaryGet(di2, keysSize, valuePos)
    }

    internal fun nodeSearch(key:K, offset:Long, offsetWithHead:Long, nodeCount:Int):Int{
        var lo = 0
        var hi = nodeCount - 1

        while (lo <= hi) {
            val mid = (lo + hi).ushr(1)
            val keysOffset = offset+volume.getInt(offsetWithHead+4+mid*4)
            val keysBinarySize = offset + volume.getInt(offsetWithHead+4+mid*4+4) - keysOffset
            val di = volume.getDataInput(keysOffset, keysBinarySize.toInt())
            val keysSize = di.unpackInt()
            val compare = comparator.compare(key, keySerializer.valueArrayBinaryGet(di, keysSize, 0))

            if (compare == 0)
                return mid
            else if (compare < 0)
                hi = mid - 1
            else
                lo = mid + 1
        }
        return -(lo + 1)

    }

    override fun isEmpty() = size==0

    override val size: Int
        get() = Math.min(Integer.MAX_VALUE.toLong(), sizeLong()).toInt()

    fun sizeLong():Long{
        return volume.getLong(SIZE_OFFSET);
    }

    override val entries: Set<Map.Entry<K, V>>
        get() = throw UnsupportedOperationException()

    override val keys: Set<K>
        get() = throw UnsupportedOperationException()
    override val values: Collection<V>
        get() = throw UnsupportedOperationException()
}