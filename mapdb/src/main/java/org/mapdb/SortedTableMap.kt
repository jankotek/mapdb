package org.mapdb

import java.util.*
import java.util.concurrent.ConcurrentMap

/**
 * Read only Sorted Table Map. It stores data in table and uses binary search to find records
 */
//TODO hashCodes for subcollections, use key/valueSerializers
class SortedTableMap<K,V>(
        val keySerializer : Serializer<K>,
        val valueSerializer : Serializer<V>,
        val pageSize:Int,
        internal val volume:Volume
): ConcurrentMap<K, V> {

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
                    if(counter==0L)
                        volume.ensureAvailable(start.toLong())
                    volume.putLong(SIZE_OFFSET, counter)
                    volume.putLong(PAGE_COUNT_OFFSET, (fileTail-pageSize)/pageSize)
                    volume.sync()
                    return SortedTableMap(
                            keySerializer = keySerializer,
                            valueSerializer = valueSerializer,
                            pageSize =  pageSize,
                            volume = volume
                    )
                }

                fun pairsToNodes(){
                    if(pairs.isEmpty())
                        return
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

    val sizeLong = volume.getLong(SIZE_OFFSET)
    val pageCount = volume.getLong(PAGE_COUNT_OFFSET)

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
        if(value==null)
            throw NullPointerException()
        val iter = valueIterator()
        while(iter.hasNext()) {
            if (valueSerializer.equals(value, iter.next())) {
                return true
            }
        }
        return false
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
        return sizeLong;
    }

    fun keyIterator():MutableIterator<K>{
        return object:MutableIterator<K>{

            var page = 0L
            var pageWithHead = start.toLong()
            var pageNodeCount = volume.getInt(pageWithHead)
            var node = 0
            var nodePos = 0
            var nodeKeys:Array<Any>? = null

            init{
                loadNextNode()
            }

            fun loadNextNode(){
                // is it last node on this page?
                if(node==pageNodeCount) {
                    // load next node?
                    if(page>=pageCount*pageSize) {
                        this.nodeKeys = null
                        return
                    }
                    page+=pageSize
                    pageWithHead = page
                    node = 0
                    pageNodeCount = volume.getInt(pageWithHead)
                }
                //load next node
                val keysOffset = volume.getInt(pageWithHead + 4 + 4 * (node++))
                val nextOffset = volume.getInt(pageWithHead + 4 + 4 * (node))
                val keysBinarySize = nextOffset - keysOffset
                val di = volume.getDataInput(page + keysOffset, keysBinarySize)
                val keysSize = di.unpackInt()
                this.nodeKeys = this@SortedTableMap.keySerializer.valueArrayToArray(
                        this@SortedTableMap.keySerializer.valueArrayDeserialize(di, keysSize)
                )
                this.nodePos = 0
            }

            override fun hasNext(): Boolean {
                return nodeKeys!=null;
            }

            override fun next(): K {
                val nodeKeys = nodeKeys
                    ?: throw NoSuchElementException()

                val ret = nodeKeys[nodePos++]
                if(nodeKeys.size==nodePos){
                    loadNextNode()
                }
                return ret as K
            }

            override fun remove() {
                throw UnsupportedOperationException("read-only")
            }
        }
    }

    fun entryIterator():MutableIterator<MutableMap.MutableEntry<K,V>>{
        return object:MutableIterator<MutableMap.MutableEntry<K,V>>{

            var page = 0L
            var pageWithHead = start.toLong()
            var pageNodeCount = volume.getInt(pageWithHead)
            var node = 0
            var nodePos = 0
            var nodeKeys:Array<Any>? = null
            var nodeVals:Array<Any>? = null

            init{
                loadNextNode()
            }

            fun loadNextNode(){
                // is it last node on this page?
                if(node==pageNodeCount) {
                    // load next node?
                    if(page>=pageCount*pageSize) {
                        this.nodeKeys = null
                        return
                    }
                    page+=pageSize
                    pageWithHead = page
                    node = 0
                    pageNodeCount = volume.getInt(pageWithHead)
                }
                //load next node
                val keysOffset = volume.getInt(pageWithHead + 4 + 4 * (node))
                val nextOffset = volume.getInt(pageWithHead + 4 + 4 * (node+1))
                val keysBinarySize = nextOffset - keysOffset
                val di = volume.getDataInput(page + keysOffset, keysBinarySize)
                val keysSize = di.unpackInt()
                this.nodeKeys = this@SortedTableMap.keySerializer.valueArrayToArray(
                        this@SortedTableMap.keySerializer.valueArrayDeserialize(di, keysSize)
                )

                val valsOffset = volume.getInt(pageWithHead + 4 + 4 * (pageNodeCount+node))
                val nextValsOffset = if(pageNodeCount==node-1) pageSize
                    else volume.getInt(pageWithHead + 4 + 4 * (pageNodeCount+node+1))
                val valsBinarySize = nextValsOffset - valsOffset
                val diVals = volume.getDataInput(page + valsOffset, valsBinarySize)
                this.nodeVals = this@SortedTableMap.valueSerializer.valueArrayToArray(
                        this@SortedTableMap.valueSerializer.valueArrayDeserialize(diVals, keysSize)
                )

                node++

                this.nodePos = 0
            }

            override fun hasNext(): Boolean {
                return nodeKeys!=null;
            }

            override fun next(): MutableMap.MutableEntry<K,V> {
                val nodeKeys = nodeKeys
                        ?: throw NoSuchElementException()

                val ret = AbstractMap.SimpleImmutableEntry(nodeKeys[nodePos] as K, nodeVals!![nodePos] as V)
                nodePos++
                if(nodeKeys.size==nodePos){
                    loadNextNode()
                }
                return ret
            }

            override fun remove() {
                throw UnsupportedOperationException("read-only")
            }
        }
    }


    fun valueIterator():MutableIterator<V>{
        return object:MutableIterator<V>{

            var page = 0L
            var pageWithHead = start.toLong()
            var pageNodeCount = volume.getInt(pageWithHead)
            var node = 0
            var nodePos = 0
            var nodeVals:Array<Any>? = null

            init{
                loadNextNode()
            }

            fun loadNextNode(){
                // is it last node on this page?
                if(node==pageNodeCount) {
                    // load next node?
                    if(page>=pageCount*pageSize) {
                        this.nodeVals = null
                        return
                    }
                    page+=pageSize
                    pageWithHead = page
                    node = 0
                    pageNodeCount = volume.getInt(pageWithHead)
                }
                //load next node
                val keysOffset = volume.getInt(pageWithHead + 4 + 4 * (node))
                val nextOffset = volume.getInt(pageWithHead + 4 + 4 * (node+1))
                val keysBinarySize = nextOffset - keysOffset
                val di = volume.getDataInput(page + keysOffset, keysBinarySize)
                val keysSize = di.unpackInt()

                val valsOffset = volume.getInt(pageWithHead + 4 + 4 * (pageNodeCount+node))
                val nextValsOffset = if(pageNodeCount==node-1) pageSize
                else volume.getInt(pageWithHead + 4 + 4 * (pageNodeCount+node+1))
                val valsBinarySize = nextValsOffset - valsOffset
                val diVals = volume.getDataInput(page + valsOffset, valsBinarySize)
                this.nodeVals = this@SortedTableMap.valueSerializer.valueArrayToArray(
                        this@SortedTableMap.valueSerializer.valueArrayDeserialize(diVals, keysSize)
                )

                node++

                this.nodePos = 0
            }

            override fun hasNext(): Boolean {
                return nodeVals!=null;
            }

            override fun next(): V {
                val nodeVals = nodeVals
                        ?: throw NoSuchElementException()

                val ret = nodeVals[nodePos] as V
                nodePos++
                if(nodeVals.size==nodePos){
                    loadNextNode()
                }
                return ret
            }

            override fun remove() {
                throw UnsupportedOperationException("read-only")
            }
        }
    }


    override val entries: MutableSet<MutableMap.MutableEntry<K, V>> = object: AbstractSet<MutableMap.MutableEntry<K, V>>(){

        override fun contains(element: MutableMap.MutableEntry<K, V>): Boolean {
            val value = this@SortedTableMap[element.key]
            return value!=null && this@SortedTableMap.valueSerializer.equals(value, element.value)
        }


        override fun isEmpty(): Boolean {
            return this@SortedTableMap.isEmpty()
        }

        override val size: Int
            get() = this@SortedTableMap.size

        override fun add(element: MutableMap.MutableEntry<K, V>): Boolean {
            throw UnsupportedOperationException("read-only")
        }

        override fun clear() {
            throw UnsupportedOperationException("read-only")
        }

        override fun iterator(): MutableIterator<MutableMap.MutableEntry<K, V>> {
            return this@SortedTableMap.entryIterator()
        }

        override fun remove(element: MutableMap.MutableEntry<K, V>): Boolean {
            throw UnsupportedOperationException("read-only")
        }

    }

    override val keys: MutableSet<K> = object:AbstractSet<K>(){

        override fun contains(element: K?): Boolean {
            return this@SortedTableMap.containsKey(element)
        }

        override fun isEmpty(): Boolean {
            return this@SortedTableMap.isEmpty()
        }

        override val size: Int
            get() = this@SortedTableMap.size

        override fun iterator(): MutableIterator<K> {
            return this@SortedTableMap.keyIterator()
        }

        override fun add(element: K): Boolean {
            throw UnsupportedOperationException("read-only")
        }

        override fun addAll(elements: Collection<K>): Boolean {
            throw UnsupportedOperationException("read-only")
        }

        override fun clear() {
            throw UnsupportedOperationException("read-only")
        }

        override fun remove(element: K): Boolean {
            throw UnsupportedOperationException("read-only")
        }


    }

    override val values: MutableCollection<V> = object : AbstractSet<V>(){

        override fun contains(element: V): Boolean {
            return this@SortedTableMap.containsValue(element)
        }

        override fun isEmpty(): Boolean {
            return this@SortedTableMap.isEmpty()
        }

        override val size: Int
            get() = this@SortedTableMap.size

        override fun add(element: V): Boolean {
            throw UnsupportedOperationException("read-only")
        }

        override fun clear() {
            throw UnsupportedOperationException("read-only")
        }

        override fun iterator(): MutableIterator<V> {
            return this@SortedTableMap.valueIterator()
        }

        override fun remove(element: V): Boolean {
            throw UnsupportedOperationException("read-only")
        }

    }

    override fun clear() {
        throw UnsupportedOperationException("read-only")
    }

    override fun put(key: K?, value: V?): V? {
        throw UnsupportedOperationException("read-only")
    }

    override fun putAll(from: Map<out K?, V?>) {
        throw UnsupportedOperationException("read-only")
    }

    override fun remove(key: K?): V? {
        throw UnsupportedOperationException("read-only")
    }

    override fun putIfAbsent(key: K?, value: V?): V? {
        throw UnsupportedOperationException("read-only")
    }

    override fun remove(key: Any?, value: Any?): Boolean {
        throw UnsupportedOperationException("read-only")
    }

    override fun replace(key: K?, oldValue: V?, newValue: V?): Boolean {
        throw UnsupportedOperationException("read-only")
    }

    override fun replace(key: K?, value: V?): V? {
        throw UnsupportedOperationException("read-only")
    }

    override fun equals(other: Any?): Boolean {
        if (other === this)
            return true

        if (other !is java.util.Map<*, *>)
            return false

        if (other.size() != size)
            return false

        try {
            val i = entries.iterator()
            while (i.hasNext()) {
                val e = i.next()
                val key = e.key
                val value = e.value
                if (value == null) {
                    if (!(other.get(key) == null && other.containsKey(key)))
                        return false
                } else {
                    if (value != other.get(key))
                        return false
                }
            }
        } catch (unused: ClassCastException) {
            return false
        } catch (unused: NullPointerException) {
            return false
        }


        return true
    }



}