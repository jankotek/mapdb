package org.mapdb

import org.mapdb.serializer.GroupSerializer
import org.mapdb.volume.Volume
import java.io.ObjectStreamException
import java.util.*
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.ConcurrentNavigableMap
import java.util.concurrent.ConcurrentSkipListMap
import java.util.function.BiConsumer
import java.io.Serializable

/**
 * Read only Sorted Table Map. It stores data in table and uses binary search to find records
 */
//TODO hashCodes for subcollections, use key/valueSerializers
class SortedTableMap<K,V>(
        override val keySerializer: GroupSerializer<K>,
        override val valueSerializer : GroupSerializer<V>,
        val pageSize:Long,
        protected val volume: Volume,
        override val hasValues: Boolean = false
): ConcurrentMap<K, V>, ConcurrentNavigableMap<K, V>, ConcurrentNavigableMapExtra<K,V>, Serializable{

    abstract class Sink<K, V> : Pump.Sink<Pair<K, V>, SortedTableMap<K, V>>() {
        fun put(key: K, value: V) {
            put(Pair(key, value))
        }
    }

    companion object {

        class Maker<K, V>(
            protected val _volume: Volume? = null,
            protected val _keySerializer: GroupSerializer<K>? = null,
            protected val _valueSerializer: GroupSerializer<V>? = null
        ) {
            protected var _pageSize: Long = CC.PAGE_SIZE
            protected var _nodeSize: Int = CC.BTREEMAP_MAX_NODE_SIZE

            fun pageSize(pageSize: Long): Maker<K, V> {
                _pageSize = DataIO.nextPowTwo(pageSize)
                return this
            }

            fun nodeSize(nodeSize: Int): Maker<K, V> {
                _nodeSize = nodeSize
                return this
            }

            fun createFrom(pairs: Iterable<Pair<K, V>>): SortedTableMap<K, V> = createFrom(pairs.iterator())

            fun createFrom(pairs: Iterator<Pair<K, V>>): SortedTableMap<K, V> {
                val consumer = createFromSink()
                for (pair in pairs)
                    consumer.put(pair)
                return consumer.create()
            }

            fun createFrom(map: Map<K, V>): SortedTableMap<K, V> {
                val consumer = createFromSink()
                for (pair in map)
                    consumer.put(Pair(pair.key, pair.value))
                return consumer.create()
            }

            fun createFromSink(): Sink<K, V> {
                return createFromSink(
                        keySerializer = _keySerializer!!,
                        valueSerializer = _valueSerializer!!,
                        volume = _volume!!,
                        pageSize = _pageSize,
                        nodeSize = _nodeSize)
            }
        }


        @JvmStatic fun <K, V> create(
                volume: Volume,
                keySerializer: GroupSerializer<K>,
                valueSerializer: GroupSerializer<V>
        ): Maker<K, V> {
            return Maker(
                    _volume = volume,
                    _keySerializer = keySerializer,
                    _valueSerializer = valueSerializer
            )
        }


        @JvmStatic fun <K, V> open(
                volume: Volume,
                keySerializer: GroupSerializer<K>,
                valueSerializer: GroupSerializer<V>
        ): SortedTableMap<K, V> {
            val pageSize = volume.getLong(PAGE_SIZE_OFFSET)
            if (pageSize <= 0 || pageSize > CC.PAGE_SIZE)
                throw DBException.DataCorruption("Wrong page size: " + pageSize)
            return SortedTableMap<K, V>(
                    keySerializer = keySerializer,
                    valueSerializer = valueSerializer,
                    volume = volume,
                    pageSize = pageSize
            )
        }

        fun <K, V> createFromSink(
                keySerializer: GroupSerializer<K>,
                valueSerializer: GroupSerializer<V>,
                volume: Volume,
                pageSize: Long = CC.PAGE_SIZE,
                nodeSize: Int = CC.BTREEMAP_MAX_NODE_SIZE
        ): Sink<K, V> {

            return object : Sink<K, V>() {

                val bytes = ByteArray(pageSize.toInt())

                val nodeKeys = ArrayList<ByteArray>()
                val nodeVals = ArrayList<ByteArray>()

                val pairs = ArrayList<Pair<K, V>>()
                var nodesSize = start+4+4;
                var fileTail = 0L
                var oldKey:K?=null

                override fun put(e: Pair<K, V>) {
                    if(oldKey!=null){
                        if(keySerializer.compare(oldKey,e.first)>=0)
                            throw DBException.NotSorted()
                    }
                    oldKey = e.first
                    pairs.add(e)
                    counter++
                    if (pairs.size < nodeSize)
                        return
                    pairsToNodes()
                }

                override fun create(): SortedTableMap<K, V> {
                    pairsToNodes()
                    //there is a chance it overflowed to next page
                    if (nodeKeys.isEmpty().not()) {
                        flushPage()
                    }
                    if (counter == 0L)
                        volume.ensureAvailable(start.toLong())
                    volume.putLong(0L, CC.FILE_HEADER.shl(7 * 8) + CC.FILE_TYPE_SORTED_SINGLE.shl(6 * 8))
                    volume.putLong(SIZE_OFFSET, counter)
                    volume.putLong(PAGE_COUNT_OFFSET, (fileTail - pageSize) / pageSize)
                    volume.putLong(PAGE_SIZE_OFFSET, pageSize.toLong())
                    volume.sync()
                    return SortedTableMap(
                            keySerializer = keySerializer,
                            valueSerializer = valueSerializer,
                            pageSize = pageSize,
                            volume = volume
                    )
                }

                fun pairsToNodes() {
                    if (pairs.isEmpty())
                        return
                    // serialize pairs into nodes
                    val keys = pairs.map { it.first }.toTypedArray<Any?>()
                    val out = DataOutput2()
                    out.packInt(keys.size)
                    keySerializer.valueArraySerialize(out, keySerializer.valueArrayFromArray(keys))
                    val binaryKeys = out.copyBytes()

                    val values = pairs.map { it.second }.toTypedArray<Any?>()
                    out.pos = 0
                    valueSerializer.valueArraySerialize(out, valueSerializer.valueArrayFromArray(values))
                    val binaryVals = out.copyBytes()

                    pairs.clear()

                    // if size does not overflow
                    val newNodesSize = nodesSize + 8 + binaryKeys.size + binaryVals.size
                    if (newNodesSize < pageSize) {
                        nodesSize = newNodesSize
                        nodeKeys.add(binaryKeys)
                        nodeVals.add(binaryVals)
                        return
                    }

                    // flush current nodes into page,
                    // the current node is not included (it would overflow page)
                    flushPage()

                    // clear everything and start over with current record
                    nodesSize = 4 + 4 + 8 + binaryKeys.size + binaryVals.size
                    nodeKeys.add(binaryKeys)
                    nodeVals.add(binaryVals)
                }

                fun flushPage() {
                    if (nodeKeys.isEmpty())
                        return
                    val bytes = bytes
                    val headSize = if (fileTail == 0L) start else 0
                    var intPos = headSize
                    DataIO.putInt(bytes, intPos, nodeKeys.size)
                    intPos += 4
                    var pos = headSize + 4 + 2 * 4 * nodeKeys.size + 4;

                    for (array in arrayOf(nodeKeys, nodeVals))
                        for (bb in array) {
                            DataIO.putInt(bytes, intPos, pos)
                            if (pos + bb.size > bytes.size)
                                throw AssertionError()
                            System.arraycopy(bb, 0, bytes, pos, bb.size)
                            intPos += 4
                            pos += bb.size
                        }
                    DataIO.putInt(bytes, intPos, pos)
                    intPos += 4
                    //clear rest of the volume
                    while (pos < pageSize) {
                        bytes[pos++] = 0
                    }

                    if (CC.ASSERT && intPos != headSize + 4 + 2 * 4 * nodeKeys.size + 4)
                        throw AssertionError()

                    //append to volume
                    volume.ensureAvailable(fileTail + pageSize)
                    volume.putData(fileTail, bytes, 0, bytes.size)
                    fileTail += pageSize
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

    init{
        if(volume.getUnsignedByte(0).toLong() != CC.FILE_HEADER){
            throw DBException.WrongFormat("Wrong file header, not MapDB file")
        }
        if(volume.getUnsignedByte(1).toLong() != CC.FILE_TYPE_SORTED_SINGLE)
            throw DBException.WrongFormat("Wrong file header, not StoreDirect file")
        if(volume.getUnsignedShort(2) != 0)
            throw DBException.NewMapDBFormat("SortedTableMap file was created with newer MapDB version")

        if(volume.getInt(4)!=0)
            throw DBException.NewMapDBFormat("SortedTableMap has some extra features, not supported in this version")
    }

    /** first key at beginning of each page */
    protected val pageKeys = {
        val keys = ArrayList<K>()
        for (i in 0..pageCount * pageSize step pageSize.toLong()) {
            val ii: Long = if (i == 0L) start.toLong() else i
            val offset = i + volume.getInt(ii + 4)
            val size = (i + volume.getInt(ii + 8) - offset).toInt()
            val input = volume.getDataInput(offset, size);
            val keysSize = input.unpackInt()
            val key = this.keySerializer.valueArrayBinaryGet(input, keysSize, 0)
            keys.add(key)
        }
        this.keySerializer.valueArrayFromArray(keys.toArray())
    }()

    override fun containsKey(key: K?): Boolean {
        return get(key) != null
    }

    override fun containsValue(value: V?): Boolean {
        if (value == null)
            throw NullPointerException()
        val iter = valueIterator()
        while (iter.hasNext()) {
            if (valueSerializer.equals(value, iter.next())) {
                return true
            }
        }
        return false
    }


    override fun get(key: K?): V? {
        if (key == null)
            throw NullPointerException()

        var keyPos = keySerializer.valueArraySearch(pageKeys, key)
        if (keyPos == -1)
            return null;
        if (keyPos < 0)
            keyPos = -keyPos - 2

        val headSize = if (keyPos == 0) start else 0
        val offset = (keyPos * pageSize).toLong()
        val offsetWithHead = offset + headSize;
        val nodeCount = volume.getInt(offsetWithHead)

        //run binary search on first keys on each node
        var pos = nodeSearch(key, offset, offsetWithHead, nodeCount)
        if (pos < 0)
            pos = -pos - 2

        //search in keys at pos
        val keysOffset = offset + volume.getInt(offsetWithHead + 4 + pos * 4)
        val keysBinarySize = offset + volume.getInt(offsetWithHead + 4 + pos * 4 + 4) - keysOffset
        val di = volume.getDataInput(keysOffset, keysBinarySize.toInt())
        val keysSize = di.unpackInt()
        val valuePos = keySerializer.valueArrayBinarySearch(key, di, keysSize, comparator)

        if (valuePos < 0)
            return null

        val valOffset = offset + volume.getInt(offsetWithHead + 4 + (pos + nodeCount) * 4)
        val valsBinarySize = offset + volume.getInt(offsetWithHead + 4 + (pos + nodeCount + 1) * 4) - valOffset
        val di2 = volume.getDataInput(valOffset, valsBinarySize.toInt())
        return valueSerializer.valueArrayBinaryGet(di2, keysSize, valuePos)
    }

    protected fun nodeSearch(key: K, offset: Long, offsetWithHead: Long, nodeCount: Int): Int {
        var lo = 0
        var hi = nodeCount - 1

        while (lo <= hi) {
            val mid = (lo + hi).ushr(1)
            val keysOffset = offset + volume.getInt(offsetWithHead + 4 + mid * 4)
            val keysBinarySize = offset + volume.getInt(offsetWithHead + 4 + mid * 4 + 4) - keysOffset
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

    override fun isEmpty() = size == 0

    override val size: Int
        get() = Math.min(Integer.MAX_VALUE.toLong(), sizeLong()).toInt()

    override fun sizeLong(): Long {
        return sizeLong;
    }

    protected class NodeIterator(
        private var map:SortedTableMap<*,*>,
        private var pageOffset: Long,
        private var pageWithHeadOffset: Long,
        private var pageNodeCount:Long ,
        private var node:Long
    ){

        fun moveToNext():Boolean{
            if(++node >= pageNodeCount){
                //move to next node
                pageOffset+=map.pageSize
                pageWithHeadOffset=pageOffset
                if(pageOffset>map.pageCount*map.pageSize){
                    //beyond EOF, end of iteration
                    return false
                }
                pageNodeCount = map.volume.getInt(pageWithHeadOffset).toLong()
                node = 0
            }
            return true
        }

        fun moveToPrev():Boolean{
            if(--node <= -1){
                //move to next node
                pageOffset-=map.pageSize
                pageWithHeadOffset=if(pageOffset==0L) start.toLong() else pageOffset
                if(pageOffset<0){
                    //beyond EOF, end of iteration
                    return false
                }
                pageNodeCount = map.volume.getInt(pageWithHeadOffset).toLong()
                node = pageNodeCount-1
            }
            return true
        }


        fun keysOffset() = pageOffset+map.volume.getInt(pageWithHeadOffset+(1+node)*4)
        fun keysOffsetEnd() = pageOffset+map.volume.getInt(pageWithHeadOffset+(1+node+1)*4)

        fun valsOffset() = pageOffset+map.volume.getInt(pageWithHeadOffset+(1+pageNodeCount+node)*4)
        fun valsOffsetEnd() = pageOffset+map.volume.getInt(pageWithHeadOffset+(1+pageNodeCount+node+1)*4)

        fun keysSize(): Int = map.volume.getPackedLong(keysOffset()).toInt()


        fun loadKeys():Array<Any>{
            val keysOffset = keysOffset()
            val keysBinarySize = keysOffsetEnd()-keysOffset
            val di = map.volume.getDataInput(keysOffset, keysBinarySize.toInt())
            val keysSize = di.unpackInt()
            return map.keySerializer.valueArrayToArray(
                    map.keySerializer.valueArrayDeserialize(di, keysSize)
            )
        }


        fun loadVals(keysSize:Int):Array<Any>{
            val valsOffset = valsOffset()
            val valsBinarySize = valsOffsetEnd()-valsOffset
            val di = map.volume.getDataInput(valsOffset, valsBinarySize.toInt())
            return map.valueSerializer.valueArrayToArray(
                    map.valueSerializer.valueArrayDeserialize(di, keysSize)
            )
        }

    }

    protected fun nodeIterator():NodeIterator{
        return NodeIterator(map = this,
                pageOffset=0L,
                pageWithHeadOffset = start.toLong(),
                pageNodeCount = volume.getInt(start.toLong()).toLong(),
                node=-1L)
    }


    protected fun nodeIterator(lo:K):NodeIterator{
        //binary search over pages
        var keyPos = keySerializer.valueArraySearch(pageKeys, lo)
        if (keyPos == -1)
            return nodeIterator(); //it starts before the first key
        if (keyPos < 0)
            keyPos = -keyPos - 2

        val headSize = if (keyPos == 0) start else 0
        val offset = (keyPos * pageSize).toLong()
        val offsetWithHead = offset + headSize;
        val nodeCount = volume.getInt(offsetWithHead)

        //run binary search on first keys on each node
        var pos = nodeSearch(lo, offset, offsetWithHead, nodeCount)
        if (pos < 0)
            pos = -pos - 2
        val pageOffset = keyPos.toLong()*pageSize;

        return NodeIterator(map = this,
                pageOffset=pageOffset,
                pageWithHeadOffset = if(pageOffset==0L) start.toLong() else pageOffset+headSize,
                pageNodeCount = nodeCount.toLong(),
                node=pos.toLong()-1)
    }

    protected fun descendingNodeIterator():NodeIterator{
        val page = pageCount*pageSize
        val pageWithHead = if(page==0L) start.toLong() else page
        val nodeCount = volume.getInt(pageWithHead).toLong()
        return NodeIterator(map = this,
                pageOffset=page,
                pageWithHeadOffset = pageWithHead,
                pageNodeCount = nodeCount,
                node=nodeCount)
    }


    override fun keyIterator():MutableIterator<K>{
        if(isEmpty())
            return Collections.emptyIterator()
        return object:MutableIterator<K>{

            val nodeIter = nodeIterator()
            var nodePos = 0
            var nodeKeys:Array<Any>? = null


            init{
                loadNextNode()
            }

            fun loadNextNode(){
                this.nodeKeys =
                        if(nodeIter.moveToNext()) nodeIter.loadKeys()
                        else null
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
        if(isEmpty())
            return Collections.emptyIterator()
        return object:MutableIterator<MutableMap.MutableEntry<K,V>>{

            val nodeIter = nodeIterator()
            var nodePos = 0
            var nodeKeys:Array<Any>? = null
            var nodeVals:Array<Any>? = null

            init{
                loadNextNode()
            }

            fun loadNextNode(){
                if(nodeIter.moveToNext()){
                    nodeKeys = nodeIter.loadKeys()
                    nodeVals = nodeIter.loadVals(nodeKeys!!.size)
                }else{
                    nodeKeys = null
                    nodeVals = null
                }
                this.nodePos = 0
            }

            override fun hasNext(): Boolean {
                return nodeKeys!=null;
            }

            override fun next(): MutableMap.MutableEntry<K,V> {
                val nodeKeys = nodeKeys
                        ?: throw NoSuchElementException()

                val ret =AbstractMap.SimpleImmutableEntry(nodeKeys[nodePos] as K, nodeVals!![nodePos] as V)
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
        if(isEmpty())
            return Collections.emptyIterator()
        return object:MutableIterator<V>{

            val nodeIter = nodeIterator()
            var nodePos = 0
            var nodeVals:Array<Any>? = null


            init{
                loadNextNode()
            }

            fun loadNextNode(){
                if(nodeIter.moveToNext()){
                    this.nodeVals = nodeIter.loadVals(nodeIter.keysSize())
                }else{
                    this.nodeVals = null
                }

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

    override val keys: NavigableSet<K> = BTreeMapJava.KeySet<K>(this as ConcurrentNavigableMapExtra<K, Any>, true)

    override fun navigableKeySet(): NavigableSet<K>? {
        return keys
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

    override fun putOnly(key: K?, value: V?) {
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

    override fun remove(key: K, value: V): Boolean {
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


    /*
     *   NavigableMap methods
     */
    override fun comparator(): Comparator<in K>? {
        return keySerializer //TODO custom comparator
    }

    override fun firstKey2(): K? {
        return firstEntry()?.key
    }

    override fun lastKey2(): K? {
        return lastEntry()?.key
    }

    override fun firstKey(): K {
        return firstKey2()?:
                throw NoSuchElementException()
    }

    override fun lastKey(): K {
        return lastKey2()?:
                throw NoSuchElementException()
    }

    override fun ceilingEntry(key: K?): MutableMap.MutableEntry<K, V>? {
        if(key==null)
            throw NullPointerException()
        return findHigher(key, true)
    }

    override fun ceilingKey(key: K?): K? {
        return ceilingEntry(key)?.key
    }

    override fun firstEntry(): MutableMap.MutableEntry<K, V>? {
        if(isEmpty())
            return null
        return entryIterator().next()
    }

    override fun floorEntry(key: K?): MutableMap.MutableEntry<K, V>? {
        if(key==null)
            throw NullPointerException()
        return findLower(key, true)
    }

    override fun floorKey(key: K?): K? {
        return floorEntry(key)?.key
    }

    override fun higherEntry(key: K?): MutableMap.MutableEntry<K, V>? {
        if(key==null)
            throw NullPointerException()
        return findHigher(key, false)
    }

    override fun higherKey(key: K?): K? {
        return higherEntry(key)?.key
    }

    override fun lastEntry(): MutableMap.MutableEntry<K, V>? {
        if(isEmpty())
            return null
        return descendingEntryIterator().next() as  MutableMap.MutableEntry<K, V>
    }

    override fun lowerEntry(key: K?): MutableMap.MutableEntry<K, V>? {
        if(key==null)
            throw NullPointerException()
        return findLower(key, false)
    }

    override fun lowerKey(key: K?): K? {
        return lowerEntry(key)?.key
    }

    override fun pollFirstEntry(): MutableMap.MutableEntry<K, V>? {
        throw UnsupportedOperationException("read-only")
    }

    override fun pollLastEntry(): MutableMap.MutableEntry<K, V>? {
        throw UnsupportedOperationException("read-only")
    }


    /*
     *    Submaps
     */
    override fun subMap(fromKey: K?,
                        fromInclusive: Boolean,
                        toKey: K?,
                        toInclusive: Boolean): ConcurrentNavigableMap<K, V> {
        if (fromKey == null || toKey == null)
            throw NullPointerException()
        return BTreeMapJava.SubMap(this, fromKey, fromInclusive, toKey, toInclusive)
    }

    override fun headMap(toKey: K?,
                         inclusive: Boolean): ConcurrentNavigableMap<K, V> {
        if (toKey == null)
            throw NullPointerException()
        return BTreeMapJava.SubMap(this, null, false, toKey, inclusive)
    }

    override fun tailMap(fromKey: K?,
                         inclusive: Boolean): ConcurrentNavigableMap<K, V> {
        if (fromKey == null)
            throw NullPointerException()
        return BTreeMapJava.SubMap(this, fromKey, inclusive, null, false)
    }

    override fun subMap(fromKey: K, toKey: K): ConcurrentNavigableMap<K, V> {
        return subMap(fromKey, true, toKey, false)
    }

    override fun headMap(toKey: K): ConcurrentNavigableMap<K, V> {
        return headMap(toKey, false)
    }

    override fun tailMap(fromKey: K): ConcurrentNavigableMap<K, V> {
        return tailMap(fromKey, true)
    }


    private val descendingMap = BTreeMapJava.DescendingMap(this, null, true, null, false)

    override fun descendingKeySet(): NavigableSet<K>? {
        return descendingMap.navigableKeySet()
    }

    override fun descendingMap(): ConcurrentNavigableMap<K, V> {
        return descendingMap;
    }

    /*
     * iterators
     */

    override fun descendingEntryIterator(): MutableIterator<MutableMap.MutableEntry<K, V?>> {
        if(isEmpty())
            return Collections.emptyIterator()
        return object:MutableIterator<MutableMap.MutableEntry<K,V?>>{

            val nodeIter = descendingNodeIterator()
            var nodePos = -1
            var nodeKeys:Array<Any>? = null
            var nodeVals:Array<Any>? = null

            init{
                loadNextNode()
            }

            fun loadNextNode(){
                if(nodeIter.moveToPrev()){
                    val k = nodeIter.loadKeys()
                    nodeKeys = nodeIter.loadKeys()
                    nodeVals = nodeIter.loadVals(k.size)
                    nodePos = k.size-1
                }else{
                    nodeKeys = null
                    nodeVals = null
                    nodePos = -1
                }
            }

            override fun hasNext(): Boolean {
                return nodeKeys!=null;
            }

            override fun next(): MutableMap.MutableEntry<K,V?> {
                val nodeKeys = nodeKeys
                        ?: throw NoSuchElementException()

                val ret =AbstractMap.SimpleImmutableEntry(nodeKeys[nodePos] as K, nodeVals!![nodePos] as V)
                nodePos--
                if(nodePos==-1){
                    loadNextNode()
                }
                return ret
            }

            override fun remove() {
                throw UnsupportedOperationException("read-only")
            }
        }
    }

    override fun descendingEntryIterator(lo: K?, loInclusive: Boolean, hi: K?, hiInclusive: Boolean): MutableIterator<MutableMap.MutableEntry<K, V?>> {
        if(isEmpty())
            return Collections.emptyIterator()

        //TODO simplify descending iterator
        return object:MutableIterator<MutableMap.MutableEntry<K, V?>>{

            var page:Long = pageSize.toLong()*pageCount
            var pageWithHead = if(page==0L) start.toLong() else page
            var pageNodeCount = volume.getInt(pageWithHead)
            var node = pageNodeCount-1
            var nodePos = 0
            var nodeKeys:Array<Any>? = null
            var nodeVals:Array<Any>? = null

            val loComp = if(loInclusive) 0 else 1

            init{
                if(hi==null){
                    loadFirstEntry()
                }else{
                    findHi()
                }
                checkLoBound()
            }

            fun loadFirstEntry(){
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
                val nextValsOffset =
                        if(pageNodeCount==node-1) pageSize.toInt()
                        else volume.getInt(pageWithHead + 4 + 4 * (pageNodeCount+node+1))
                val valsBinarySize = nextValsOffset - valsOffset
                val diVals = volume.getDataInput(page + valsOffset, valsBinarySize)
                nodePos = keysSize-1
                this.nodeVals = this@SortedTableMap.valueSerializer.valueArrayToArray(
                        this@SortedTableMap.valueSerializer.valueArrayDeserialize(diVals, keysSize)
                )
            }

            fun findHi(){
                if(hi==null)
                    throw NullPointerException()

                var keyPos = keySerializer.valueArraySearch(pageKeys, hi)

                pageLoop@ while(true) {
                    if (keyPos == -1) {
                        //cancel iteration,
                        nodeKeys = null
                        nodeVals = null
                        return
                    }
                    if (keyPos > pageCount){
                        loadFirstEntry()
                        return
                    }

                    if (keyPos < 0)
                        keyPos = -keyPos - 2

                    val headSize = if (keyPos == 0) start else 0
                    val offset = (keyPos * pageSize).toLong()
                    val offsetWithHead = offset + headSize;
                    val nodeCount = volume.getInt(offsetWithHead)

                    //run binary search on first keys on each node
                    var nodePos = nodeSearch(hi, offset, offsetWithHead, nodeCount)
                    if (nodePos < 0)
                        nodePos = -nodePos - 2

                    nodeLoop@ while(true) {
                        //search in keys at pos
                        val keysOffset = offset + volume.getInt(offsetWithHead + 4 + nodePos * 4)
                        val keysBinarySize = offset + volume.getInt(offsetWithHead + 4 + nodePos * 4 + 4) - keysOffset
                        val di = volume.getDataInput(keysOffset, keysBinarySize.toInt())
                        val keysSize = di.unpackInt()
                        val keys = keySerializer.valueArrayDeserialize(di, keysSize)
                        var valuePos = keySerializer.valueArraySearch(keys, hi, comparator)

                        if (!hiInclusive && valuePos >= 0)
                            valuePos--
                        else if (valuePos < 0)
                            valuePos = -valuePos - 2

                        //check if valuePos fits into current node
                        if (valuePos < 0) {
                            //does not fit, increase node and continue
                            nodePos--

                            //is the last node on this page? in that case increase page count and contine page loop
                            if(nodePos<0){
                                keyPos--
                                continue@pageLoop
                            }

                            continue@nodeLoop
                        }

                        if (valuePos >= keysSize) {
                            valuePos--
                        }

                        this.nodeKeys = keySerializer.valueArrayToArray(keys)
                        this.nodePos = valuePos
                        this.node = nodePos + 1
                        this.pageWithHead = offsetWithHead
                        this.pageNodeCount = nodeCount
                        this.page = offset

                        val valOffset = offset + volume.getInt(offsetWithHead + 4 + (nodePos + nodeCount) * 4)
                        val valsBinarySize = offset + volume.getInt(offsetWithHead + 4 + (nodePos + nodeCount + 1) * 4) - valOffset
                        val di2 = volume.getDataInput(valOffset, valsBinarySize.toInt())
                        val vals = valueSerializer.valueArrayDeserialize(di2, keysSize)
                        this.nodeVals = valueSerializer.valueArrayToArray(vals)
                        return
                    }
                }
            }


            fun loadNextNode(){
                // is it last node on this page?
                if(node==0) {
                    // load next node?
                    if(page==0L) {
                        this.nodeKeys = null
                        this.nodeVals = null
                        return
                    }
                    page-=pageSize
                    pageWithHead = if(page==0L) start.toLong() else page
                    pageNodeCount = volume.getInt(pageWithHead)
                    node = pageNodeCount
                }
                //load next node
                //load next node
                node--
                val keysOffset = volume.getInt(pageWithHead + 4 + 4 * (node))
                val nextOffset = volume.getInt(pageWithHead + 4 + 4 * (node+1))
                val keysBinarySize = nextOffset - keysOffset
                val di = volume.getDataInput(page + keysOffset, keysBinarySize)
                val keysSize = di.unpackInt()
                this.nodeKeys = this@SortedTableMap.keySerializer.valueArrayToArray(
                        this@SortedTableMap.keySerializer.valueArrayDeserialize(di, keysSize)
                )

                val valsOffset = volume.getInt(pageWithHead + 4 + 4 * (pageNodeCount+node))
                val nextValsOffset =
                        if(pageNodeCount==node-1) pageSize.toInt()
                        else volume.getInt(pageWithHead + 4 + 4 * (pageNodeCount+node+1))
                val valsBinarySize = nextValsOffset - valsOffset
                val diVals = volume.getDataInput(page + valsOffset, valsBinarySize)
                this.nodeVals = this@SortedTableMap.valueSerializer.valueArrayToArray(
                        this@SortedTableMap.valueSerializer.valueArrayDeserialize(diVals, keysSize)
                )

                this.nodePos = keysSize-1
            }

            override fun hasNext(): Boolean {
                return nodeVals!=null;
            }

            override fun next(): MutableMap.MutableEntry<K, V?> {
                val nodeKeys = nodeKeys
                        ?: throw NoSuchElementException()

                val ret = AbstractMap.SimpleImmutableEntry<K,V>(nodeKeys[nodePos] as K, nodeVals!![nodePos] as V)
                nodePos--
                if(nodePos==-1){
                    loadNextNode()
                }
                checkLoBound()
                return ret
            }

            fun checkLoBound(){
                val lo = lo
                        ?:return
                val nodeKeys = nodeKeys
                        ?:return

                val nextKey = nodeKeys[nodePos] as K
                if(keySerializer.compare(nextKey, lo)<loComp){
                    //high bound is lower, than key, cancel next node
                    this.nodeKeys = null
                    this.nodePos = -1
                    this.nodeVals = null
                }
            }


            override fun remove() {
                throw UnsupportedOperationException("read-only")
            }
        }
    }

    override fun descendingKeyIterator(): MutableIterator<K> {
        if(isEmpty())
            return Collections.emptyIterator()
        return object:MutableIterator<K>{

            val nodeIter = descendingNodeIterator()
            var nodePos = -1
            var nodeKeys:Array<Any>? = null


            init{
                loadNextNode()
            }

            fun loadNextNode() {
                if (nodeIter.moveToPrev()){
                    this.nodeKeys = nodeIter.loadKeys()
                    this.nodePos = nodeKeys!!.size-1
                }else{
                    this.nodeKeys = null
                    this.nodePos = -1
                }

            }

            override fun hasNext(): Boolean {
                return nodeKeys!=null;
            }

            override fun next(): K {
                val nodeKeys = nodeKeys
                        ?: throw NoSuchElementException()

                val ret = nodeKeys[nodePos--]
                if(nodePos==-1){
                    loadNextNode()
                }
                return ret as K
            }

            override fun remove() {
                throw UnsupportedOperationException("read-only")
            }
        }
    }

    override fun descendingKeyIterator(lo: K?, loInclusive: Boolean, hi: K?, hiInclusive: Boolean): MutableIterator<K> {
        if(isEmpty())
            return Collections.emptyIterator()

        //TODO simplify descending iterator
        return object:MutableIterator<K>{

            var page:Long = pageSize.toLong()*pageCount
            var pageWithHead = if(page==0L) start.toLong() else page
            var pageNodeCount = volume.getInt(pageWithHead)
            var node = pageNodeCount-1
            var nodePos = 0
            var nodeKeys:Array<Any>? = null

            val loComp = if(loInclusive) 0 else 1

            init{
                if(hi==null){
                    loadFirstEntry()
                }else{
                    findHi()
                }
                checkLoBound()
            }

            fun loadFirstEntry(){
                //load the last keys
                val keysOffset = volume.getInt(pageWithHead + 4 + 4 * (node))
                val nextOffset = volume.getInt(pageWithHead + 4 + 4 * (node+1))

                val di = volume.getDataInput(page+keysOffset, nextOffset-keysOffset)
                val nodeSize = di.unpackInt()
                nodePos = nodeSize-1
                nodeKeys = keySerializer.valueArrayToArray(keySerializer.valueArrayDeserialize(di, nodeSize))
            }

            fun findHi(){
                if(hi==null)
                    throw NullPointerException()

                var keyPos = keySerializer.valueArraySearch(pageKeys, hi)

                pageLoop@ while(true) {
                    if (keyPos == -1) {
                        //cancel iteration,
                        nodeKeys = null
                        return
                    }
                    if (keyPos > pageCount){
                        loadFirstEntry()
                        return
                    }

                    if (keyPos < 0)
                        keyPos = -keyPos - 2

                    val headSize = if (keyPos == 0) start else 0
                    val offset = (keyPos * pageSize).toLong()
                    val offsetWithHead = offset + headSize;
                    val nodeCount = volume.getInt(offsetWithHead)

                    //run binary search on first keys on each node
                    var nodePos = nodeSearch(hi, offset, offsetWithHead, nodeCount)
                    if (nodePos < 0)
                        nodePos = -nodePos - 2

                    nodeLoop@ while(true) {
                        //search in keys at pos
                        val keysOffset = offset + volume.getInt(offsetWithHead + 4 + nodePos * 4)
                        val keysBinarySize = offset + volume.getInt(offsetWithHead + 4 + nodePos * 4 + 4) - keysOffset
                        val di = volume.getDataInput(keysOffset, keysBinarySize.toInt())
                        val keysSize = di.unpackInt()
                        val keys = keySerializer.valueArrayDeserialize(di, keysSize)
                        var valuePos = keySerializer.valueArraySearch(keys, hi, comparator)

                        if (!hiInclusive && valuePos >= 0)
                            valuePos--
                        else if (valuePos < 0)
                            valuePos = -valuePos - 2

                        //check if valuePos fits into current node
                        if (valuePos < 0) {
                            //does not fit, increase node and continue
                            nodePos--

                            //is the last node on this page? in that case increase page count and contine page loop
                            if(nodePos<0){
                                keyPos--
                                continue@pageLoop
                            }

                            continue@nodeLoop
                        }

                        if (valuePos >= keysSize) {
                            valuePos--
                        }

                        this.nodeKeys = keySerializer.valueArrayToArray(keys)
                        this.nodePos = valuePos
                        this.node = nodePos
                        this.pageWithHead = offsetWithHead
                        this.pageNodeCount = nodeCount
                        this.page = offset
                        return
                    }
                }
            }


            fun loadNextNode(){
                // is it last node on this page?
                if(node==0) {
                    // load next node?
                    if(page==0L) {
                        this.nodeKeys = null
                        return
                    }
                    page-=pageSize
                    pageWithHead = if(page==0L) start.toLong() else page
                    pageNodeCount = volume.getInt(pageWithHead)
                    node = pageNodeCount
                }
                //load next node
                //load next node
                node--
                val keysOffset = volume.getInt(pageWithHead + 4 + 4 * (node))
                val nextOffset = volume.getInt(pageWithHead + 4 + 4 * (node+1))
                val keysBinarySize = nextOffset - keysOffset
                val di = volume.getDataInput(page + keysOffset, keysBinarySize)
                val keysSize = di.unpackInt()
                this.nodeKeys = this@SortedTableMap.keySerializer.valueArrayToArray(
                        this@SortedTableMap.keySerializer.valueArrayDeserialize(di, keysSize)
                )
                this.nodePos = keysSize-1
            }

            override fun hasNext(): Boolean {
                return nodeKeys!=null;
            }

            override fun next(): K {
                val nodeKeys = nodeKeys
                        ?: throw NoSuchElementException()

                //val ret = AbstractMap.SimpleImmutableEntry<K,V>(nodeKeys[nodePos] as K, nodeVals!![nodePos] as V)
                val ret = nodeKeys[nodePos] as K
                nodePos--
                if(nodePos==-1){
                    loadNextNode()
                }
                checkLoBound()
                return ret
            }

            fun checkLoBound(){
                val lo = lo
                        ?:return
                val nodeKeys = nodeKeys
                        ?:return

                val nextKey = nodeKeys[nodePos] as K
                if(keySerializer.compare(nextKey, lo)<loComp){
                    //high bound is lower, than key, cancel next node
                    this.nodeKeys = null
                    this.nodePos = -1
                }
            }


            override fun remove() {
                throw UnsupportedOperationException("read-only")
            }
        }
    }

    override fun descendingValueIterator(): MutableIterator<V> {
        if(isEmpty())
            return Collections.emptyIterator()
        return object:MutableIterator<V>{

            val nodeIter = descendingNodeIterator()
            var nodePos = -1
            var nodeVals:Array<Any>? = null


            init{
                loadNextNode()
            }

            fun loadNextNode(){
                if(nodeIter.moveToPrev()){
                    this.nodeVals = nodeIter.loadVals(nodeIter.keysSize())
                    nodePos = nodeVals!!.size-1
                }else{
                    this.nodeVals = null
                    nodePos = -1
                }
            }

            override fun hasNext(): Boolean {
                return nodeVals!=null;
            }


            override fun next(): V {
                val nodeVals = nodeVals
                        ?: throw NoSuchElementException()

                val ret = nodeVals[nodePos] as V
                nodePos--
                if(nodePos==-1){
                    loadNextNode()
                }
                return ret
            }

            override fun remove() {
                throw UnsupportedOperationException("read-only")
            }
        }
    }

    override fun descendingValueIterator(lo: K?, loInclusive: Boolean, hi: K?, hiInclusive: Boolean): MutableIterator<V> {
        if(isEmpty())
            return Collections.emptyIterator()

        //TODO simplify descending iterator
        return object:MutableIterator<V>{

            var page:Long = pageSize.toLong()*pageCount
            var pageWithHead = if(page==0L) start.toLong() else page
            var pageNodeCount = volume.getInt(pageWithHead)
            var node = pageNodeCount-1
            var nodePos = 0
            var nodeKeys:Array<Any>? = null
            var nodeVals:Array<Any>? = null

            val loComp = if(loInclusive) 0 else 1

            init{
                if(hi==null){
                    loadFirstEntry()
                }else{
                    findHi()
                }
                checkLoBound()
            }

            fun loadFirstEntry(){
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
                val nextValsOffset =
                        if(pageNodeCount==node-1) pageSize.toInt()
                        else volume.getInt(pageWithHead + 4 + 4 * (pageNodeCount+node+1))
                val valsBinarySize = nextValsOffset - valsOffset
                val diVals = volume.getDataInput(page + valsOffset, valsBinarySize)
                nodePos = keysSize-1
                this.nodeVals = this@SortedTableMap.valueSerializer.valueArrayToArray(
                        this@SortedTableMap.valueSerializer.valueArrayDeserialize(diVals, keysSize)
                )
            }

            fun findHi(){
                if(hi==null)
                    throw NullPointerException()

                var keyPos = keySerializer.valueArraySearch(pageKeys, hi)

                pageLoop@ while(true) {
                    if (keyPos == -1) {
                        //cancel iteration,
                        nodeKeys = null
                        nodeVals = null
                        return
                    }
                    if (keyPos > pageCount){
                        loadFirstEntry()
                        return
                    }

                    if (keyPos < 0)
                        keyPos = -keyPos - 2

                    val headSize = if (keyPos == 0) start else 0
                    val offset = (keyPos * pageSize).toLong()
                    val offsetWithHead = offset + headSize;
                    val nodeCount = volume.getInt(offsetWithHead)

                    //run binary search on first keys on each node
                    var nodePos = nodeSearch(hi, offset, offsetWithHead, nodeCount)
                    if (nodePos < 0)
                        nodePos = -nodePos - 2

                    nodeLoop@ while(true) {
                        //search in keys at pos
                        val keysOffset = offset + volume.getInt(offsetWithHead + 4 + nodePos * 4)
                        val keysBinarySize = offset + volume.getInt(offsetWithHead + 4 + nodePos * 4 + 4) - keysOffset
                        val di = volume.getDataInput(keysOffset, keysBinarySize.toInt())
                        val keysSize = di.unpackInt()
                        val keys = keySerializer.valueArrayDeserialize(di, keysSize)
                        var valuePos = keySerializer.valueArraySearch(keys, hi, comparator)

                        if (!hiInclusive && valuePos >= 0)
                            valuePos--
                        else if (valuePos < 0)
                            valuePos = -valuePos - 2

                        //check if valuePos fits into current node
                        if (valuePos < 0) {
                            //does not fit, increase node and continue
                            nodePos--

                            //is the last node on this page? in that case increase page count and contine page loop
                            if(nodePos<0){
                                keyPos--
                                continue@pageLoop
                            }

                            continue@nodeLoop
                        }

                        if (valuePos >= keysSize) {
                            valuePos--
                        }

                        this.nodeKeys = keySerializer.valueArrayToArray(keys)
                        this.nodePos = valuePos
                        this.node = nodePos + 1
                        this.pageWithHead = offsetWithHead
                        this.pageNodeCount = nodeCount
                        this.page = offset

                        val valOffset = offset + volume.getInt(offsetWithHead + 4 + (nodePos + nodeCount) * 4)
                        val valsBinarySize = offset + volume.getInt(offsetWithHead + 4 + (nodePos + nodeCount + 1) * 4) - valOffset
                        val di2 = volume.getDataInput(valOffset, valsBinarySize.toInt())
                        val vals = valueSerializer.valueArrayDeserialize(di2, keysSize)
                        this.nodeVals = valueSerializer.valueArrayToArray(vals)
                        return
                    }
                }
            }


            fun loadNextNode(){
                // is it last node on this page?
                if(node==0) {
                    // load next node?
                    if(page==0L) {
                        this.nodeKeys = null
                        this.nodeVals = null
                        return
                    }
                    page-=pageSize
                    pageWithHead = if(page==0L) start.toLong() else page
                    pageNodeCount = volume.getInt(pageWithHead)
                    node = pageNodeCount
                }
                //load next node
                //load next node
                node--
                val keysOffset = volume.getInt(pageWithHead + 4 + 4 * (node))
                val nextOffset = volume.getInt(pageWithHead + 4 + 4 * (node+1))
                val keysBinarySize = nextOffset - keysOffset
                val di = volume.getDataInput(page + keysOffset, keysBinarySize)
                val keysSize = di.unpackInt()
                this.nodeKeys = this@SortedTableMap.keySerializer.valueArrayToArray(
                        this@SortedTableMap.keySerializer.valueArrayDeserialize(di, keysSize)
                )

                val valsOffset = volume.getInt(pageWithHead + 4 + 4 * (pageNodeCount+node))
                val nextValsOffset =
                        if(pageNodeCount==node-1) pageSize.toInt()
                        else volume.getInt(pageWithHead + 4 + 4 * (pageNodeCount+node+1))
                val valsBinarySize = nextValsOffset - valsOffset
                val diVals = volume.getDataInput(page + valsOffset, valsBinarySize)
                this.nodeVals = this@SortedTableMap.valueSerializer.valueArrayToArray(
                        this@SortedTableMap.valueSerializer.valueArrayDeserialize(diVals, keysSize)
                )

                this.nodePos = keysSize-1
            }

            override fun hasNext(): Boolean {
                return nodeVals!=null;
            }

            override fun next(): V {
                val nodeVals = nodeVals
                        ?: throw NoSuchElementException()

                val ret = nodeVals[nodePos] as V
                nodePos--
                if(nodePos==-1){
                    loadNextNode()
                }
                checkLoBound()
                return ret
            }

            fun checkLoBound(){
                val lo = lo
                        ?:return
                val nodeKeys = nodeKeys
                        ?:return

                val nextKey = nodeKeys[nodePos] as K
                if(keySerializer.compare(nextKey, lo)<loComp){
                    //high bound is lower, than key, cancel next node
                    this.nodeKeys = null
                    this.nodePos = -1
                    this.nodeVals = null
                }
            }


            override fun remove() {
                throw UnsupportedOperationException("read-only")
            }
        }    }

    override fun entryIterator(lo: K?, loInclusive: Boolean, hi: K?, hiInclusive: Boolean): MutableIterator<MutableMap.MutableEntry<K, V?>> {
        if(isEmpty())
            return Collections.emptyIterator()
        return object:MutableIterator<MutableMap.MutableEntry<K, V?>>{

            val nodeIter = if(lo==null) nodeIterator() else nodeIterator(lo)
            var nodePos = 0
            var nodeKeys:Array<Any>? = null //TODO perf tailMap should not load the values
            var nodeVals:Array<Any>? = null

            val hiComp = if(hiInclusive) 0 else 1

            init{
                if(lo==null)
                    loadNextNode()
                else
                    findStart()
            }

            fun loadNextNode(){
                if(nodeIter.moveToNext()) {
                    this.nodeKeys = nodeIter.loadKeys()
                    this.nodeVals = nodeIter.loadVals(nodeKeys!!.size)
                }else{
                    this.nodeKeys = null
                    this.nodeVals = null
                }
                this.nodePos = 0
            }

            fun findStart(){
                val comp = if(loInclusive) -1 else 0
                keysLoop@ while(true){
                    loadNextNode()
                    val keys = nodeKeys ?: return
                    //iterate over node until bigger entry is found
                    var pos = 0
                    while(true){
                        if(pos>=keys.size){
                            //move to next node
                            continue@keysLoop
                        }
                        if(keySerializer.compare(keys[pos] as K, lo)>comp){
                            //end iteration
                            nodePos = pos
                            checkHiBound()
                            return
                        }
                        pos++
                    }
                }
            }

            override fun hasNext(): Boolean {
                return nodeKeys!=null;
            }

            override fun next(): MutableMap.MutableEntry<K, V?> {
                val nodeKeys = nodeKeys
                        ?: throw NoSuchElementException()
                val nodeVals = nodeVals
                        ?: throw NoSuchElementException()

                val ret = AbstractMap.SimpleImmutableEntry<K,V>(nodeKeys[nodePos] as K, nodeVals[nodePos] as V)
                nodePos++
                if(nodeVals.size==nodePos){
                    loadNextNode()
                }
                checkHiBound()
                return ret
            }


            fun checkHiBound(){
                val hi = hi
                        ?:return
                val nodeKeys = nodeKeys
                        ?:return

                val nextKey = nodeKeys[nodePos] as K
                if(keySerializer.compare(hi, nextKey)<hiComp){
                    //high bound is lower, than key, cancel next node
                    this.nodeKeys = null
                    this.nodePos = -1
                }
            }

            override fun remove() {
                throw UnsupportedOperationException("read-only")
            }
        }
    }

    override fun keyIterator(lo: K?, loInclusive: Boolean, hi: K?, hiInclusive: Boolean): MutableIterator<K> {
        if(isEmpty())
            return Collections.emptyIterator()
        return object:MutableIterator<K>{

            val nodeIter = if(lo==null) nodeIterator() else nodeIterator(lo)
            var nodePos = 0
            var nodeKeys:Array<Any>? = null

            val hiComp = if(hiInclusive) 0 else 1

            init{
                if(lo==null)
                    loadNextNode()
                else
                    findStart()
            }

            fun loadNextNode(){
                this.nodeKeys =
                        if(nodeIter.moveToNext()) nodeIter.loadKeys()
                        else null
                this.nodePos = 0
            }

            fun findStart(){
                val comp = if(loInclusive) -1 else 0
                keysLoop@ while(true){
                    loadNextNode()
                    val keys = nodeKeys ?: return
                    //iterate over node until bigger entry is found
                    var pos = 0
                    while(true){
                        if(pos>=keys.size){
                            //move to next node
                            continue@keysLoop
                        }
                        if(keySerializer.compare(keys[pos] as K, lo)>comp){
                            //end iteration
                            nodePos = pos
                            checkHiBound()
                            return
                        }
                        pos++
                    }
                }
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
                checkHiBound()
                return ret as K
            }


            fun checkHiBound(){
                val hi = hi
                        ?:return
                val nodeKeys = nodeKeys
                        ?:return

                val nextKey = nodeKeys[nodePos] as K
                if(keySerializer.compare(hi, nextKey)<hiComp){
                    //high bound is lower, than key, cancel next node
                    this.nodeKeys = null
                    this.nodePos = -1
                }
            }

            override fun remove() {
                throw UnsupportedOperationException("read-only")
            }
        }
    }

    override fun valueIterator(lo: K?, loInclusive: Boolean, hi: K?, hiInclusive: Boolean): MutableIterator<V> {
        if(isEmpty())
            return Collections.emptyIterator()
        return object:MutableIterator<V>{

            val nodeIter = if(lo==null) nodeIterator() else nodeIterator(lo)
            var nodePos = 0
            var nodeKeys:Array<Any>? = null //TODO perf tailMap should not load the values
            var nodeVals:Array<Any>? = null

            val hiComp = if(hiInclusive) 0 else 1

            init{
                if(lo==null)
                    loadNextNode()
                else
                    findStart()
            }

            fun loadNextNode(){
                if(nodeIter.moveToNext()) {
                    this.nodeKeys = nodeIter.loadKeys()
                    this.nodeVals = nodeIter.loadVals(nodeKeys!!.size)
                }else{
                    this.nodeKeys = null
                    this.nodeVals = null
                }
                this.nodePos = 0
            }

            fun findStart(){
                val comp = if(loInclusive) -1 else 0
                keysLoop@ while(true){
                    loadNextNode()
                    val keys = nodeKeys ?: return
                    //iterate over node until bigger entry is found
                    var pos = 0
                    while(true){
                        if(pos>=keys.size){
                            //move to next node
                            continue@keysLoop
                        }
                        if(keySerializer.compare(keys[pos] as K, lo)>comp){
                            //end iteration
                            nodePos = pos
                            checkHiBound()
                            return
                        }
                        pos++
                    }
                }
            }

            override fun hasNext(): Boolean {
                return nodeKeys!=null;
            }

            override fun next(): V {
                if(nodeKeys==null)
                    throw NoSuchElementException()
                val nodeVals = nodeVals
                        ?: throw NoSuchElementException()

                val ret = nodeVals[nodePos++]
                if(nodeVals.size==nodePos){
                    loadNextNode()
                }
                checkHiBound()
                return ret as V
            }


            fun checkHiBound(){
                val hi = hi
                        ?:return
                val nodeKeys = nodeKeys
                        ?:return

                val nextKey = nodeKeys[nodePos] as K
                if(keySerializer.compare(hi, nextKey)<hiComp){
                    //high bound is lower, than key, cancel next node
                    this.nodeKeys = null
                    this.nodePos = -1
                }
            }

            override fun remove() {
                throw UnsupportedOperationException("read-only")
            }
        }
    }


    override fun findHigher(key: K?, inclusive: Boolean): MutableMap.MutableEntry<K, V>? {
        if(key==null)
            throw NullPointerException()

        var keyPos = keySerializer.valueArraySearch(pageKeys, key)

        pageLoop@ while(true) {
            if (keyPos == -1) {
                return firstEntry()
            }
            if(keyPos>pageCount)
                return null

            if (keyPos < 0)
                keyPos = -keyPos - 2

            val headSize = if (keyPos == 0) start else 0
            val offset = (keyPos * pageSize).toLong()
            val offsetWithHead = offset + headSize;
            val nodeCount = volume.getInt(offsetWithHead)

            //run binary search on first keys on each node
            var nodePos = nodeSearch(key, offset, offsetWithHead, nodeCount)
            if(nodePos==-1)
                nodePos = 0
            else if (nodePos < 0)
                nodePos = -nodePos - 2


            nodeLoop@ while(true) {
                //search in keys at pos
                val keysOffset = offset + volume.getInt(offsetWithHead + 4 + nodePos * 4)
                val keysBinarySize = offset + volume.getInt(offsetWithHead + 4 + nodePos * 4 + 4) - keysOffset
                val di = volume.getDataInput(keysOffset, keysBinarySize.toInt())
                val keysSize = di.unpackInt()
                val keys = keySerializer.valueArrayDeserialize(di, keysSize)
                var valuePos = keySerializer.valueArraySearch(keys, key, comparator)

                if (!inclusive && valuePos >= 0)
                    valuePos++
                if (valuePos < 0)
                    valuePos = -valuePos - 1

                //check if valuePos fits into current node
                if (valuePos >= keysSize) {
                    //does not fit, increase node and continue
                    nodePos++

                    //is the last node on this page? in that case increase page count and contine page loop
                    if(nodePos>=nodeCount){
                        keyPos++
                        continue@pageLoop
                    }

                    continue@nodeLoop
                }

                val key2 = keySerializer.valueArrayGet(keys, valuePos)

                val valOffset = offset + volume.getInt(offsetWithHead + 4 + (nodePos + nodeCount) * 4)
                val valsBinarySize = offset + volume.getInt(offsetWithHead + 4 + (nodePos + nodeCount + 1) * 4) - valOffset
                val di2 = volume.getDataInput(valOffset, valsBinarySize.toInt())
                val value = valueSerializer.valueArrayBinaryGet(di2, keysSize, valuePos)
                return AbstractMap.SimpleImmutableEntry(key2, value)
            }
        }
    }

    override fun findLower(key: K?, inclusive: Boolean): MutableMap.MutableEntry<K, V>? {
        if(key==null)
            throw NullPointerException()

        var keyPos = keySerializer.valueArraySearch(pageKeys, key)

        pageLoop@ while(true) {
            if (keyPos == -1) {
                return null
            }
            if(keyPos>pageCount)
                return lastEntry()

            if (keyPos < 0)
                keyPos = -keyPos - 2

            val headSize = if (keyPos == 0) start else 0
            val offset = (keyPos * pageSize).toLong()
            val offsetWithHead = offset + headSize;
            val nodeCount = volume.getInt(offsetWithHead)

            //run binary search on first keys on each node
            var nodePos = nodeSearch(key, offset, offsetWithHead, nodeCount)
            if (nodePos < 0)
                nodePos = -nodePos - 2

            nodeLoop@ while(true) {
                //search in keys at pos
                val keysOffset = offset + volume.getInt(offsetWithHead + 4 + nodePos * 4)
                val keysBinarySize = offset + volume.getInt(offsetWithHead + 4 + nodePos * 4 + 4) - keysOffset
                val di = volume.getDataInput(keysOffset, keysBinarySize.toInt())
                val keysSize = di.unpackInt()
                val keys = keySerializer.valueArrayDeserialize(di, keysSize)
                var valuePos = keySerializer.valueArraySearch(keys, key, comparator)

                if (!inclusive && valuePos >= 0)
                    valuePos--
                else if (valuePos < 0)
                    valuePos = -valuePos - 2

                //check if valuePos fits into current node
                if (valuePos < 0) {
                    //does not fit, increase node and continue
                    nodePos--

                    //is the last node on this page? in that case increase page count and contine page loop
                    if(nodePos<0){
                        keyPos--
                        continue@pageLoop
                    }

                    continue@nodeLoop
                }

                if (valuePos >= keysSize) {
                    valuePos--
                }

                val key2 = keySerializer.valueArrayGet(keys, valuePos)

                val valOffset = offset + volume.getInt(offsetWithHead + 4 + (nodePos + nodeCount) * 4)
                val valsBinarySize = offset + volume.getInt(offsetWithHead + 4 + (nodePos + nodeCount + 1) * 4) - valOffset
                val di2 = volume.getDataInput(valOffset, valsBinarySize.toInt())
                val value = valueSerializer.valueArrayBinaryGet(di2, keysSize, valuePos)
                return AbstractMap.SimpleImmutableEntry(key2, value)
            }
        }
    }


    override fun findHigherKey(key: K?, inclusive: Boolean): K? {
        if(key==null)
            throw NullPointerException()

        var keyPos = keySerializer.valueArraySearch(pageKeys, key)

        pageLoop@ while(true) {
            if (keyPos == -1) {
                return firstKey()
            }
            if(keyPos>pageCount)
                return null

            if (keyPos < 0)
                keyPos = -keyPos - 2

            val headSize = if (keyPos == 0) start else 0
            val offset = (keyPos * pageSize).toLong()
            val offsetWithHead = offset + headSize;
            val nodeCount = volume.getInt(offsetWithHead)

            //run binary search on first keys on each node
            var nodePos = nodeSearch(key, offset, offsetWithHead, nodeCount)
            if(nodePos==-1)
                nodePos = 0
            else if (nodePos < 0)
                nodePos = -nodePos - 2


            nodeLoop@ while(true) {
                //search in keys at pos
                val keysOffset = offset + volume.getInt(offsetWithHead + 4 + nodePos * 4)
                val keysBinarySize = offset + volume.getInt(offsetWithHead + 4 + nodePos * 4 + 4) - keysOffset
                val di = volume.getDataInput(keysOffset, keysBinarySize.toInt())
                val keysSize = di.unpackInt()
                val keys = keySerializer.valueArrayDeserialize(di, keysSize)
                var valuePos = keySerializer.valueArraySearch(keys, key, comparator)

                if (!inclusive && valuePos >= 0)
                    valuePos++
                if (valuePos < 0)
                    valuePos = -valuePos - 1

                //check if valuePos fits into current node
                if (valuePos >= keysSize) {
                    //does not fit, increase node and continue
                    nodePos++

                    //is the last node on this page? in that case increase page count and contine page loop
                    if(nodePos>=nodeCount){
                        keyPos++
                        continue@pageLoop
                    }

                    continue@nodeLoop
                }

                return keySerializer.valueArrayGet(keys, valuePos)
            }
        }
    }

    override fun findLowerKey(key: K?, inclusive: Boolean): K? {
        if(key==null)
            throw NullPointerException()

        var keyPos = keySerializer.valueArraySearch(pageKeys, key)

        pageLoop@ while(true) {
            if (keyPos == -1) {
                return null
            }
            if(keyPos>pageCount)
                return lastKey()

            if (keyPos < 0)
                keyPos = -keyPos - 2

            val headSize = if (keyPos == 0) start else 0
            val offset = (keyPos * pageSize).toLong()
            val offsetWithHead = offset + headSize;
            val nodeCount = volume.getInt(offsetWithHead)

            //run binary search on first keys on each node
            var nodePos = nodeSearch(key, offset, offsetWithHead, nodeCount)
            if (nodePos < 0)
                nodePos = -nodePos - 2

            nodeLoop@ while(true) {
                //search in keys at pos
                val keysOffset = offset + volume.getInt(offsetWithHead + 4 + nodePos * 4)
                val keysBinarySize = offset + volume.getInt(offsetWithHead + 4 + nodePos * 4 + 4) - keysOffset
                val di = volume.getDataInput(keysOffset, keysBinarySize.toInt())
                val keysSize = di.unpackInt()
                val keys = keySerializer.valueArrayDeserialize(di, keysSize)
                var valuePos = keySerializer.valueArraySearch(keys, key, comparator)

                if (!inclusive && valuePos >= 0)
                    valuePos--
                else if (valuePos < 0)
                    valuePos = -valuePos - 2

                //check if valuePos fits into current node
                if (valuePos < 0) {
                    //does not fit, increase node and continue
                    nodePos--

                    //is the last node on this page? in that case increase page count and contine page loop
                    if(nodePos<0){
                        keyPos--
                        continue@pageLoop
                    }

                    continue@nodeLoop
                }

                if (valuePos >= keysSize) {
                    valuePos--
                }

                return keySerializer.valueArrayGet(keys, valuePos)
            }
        }
    }


    override fun forEachKey(procedure: (K) -> Unit) {
        //TODO PERF optimize forEach traversal
        for(k in keys)
            procedure.invoke(k)
    }

    override fun forEachValue(procedure: (V) -> Unit) {
        //TODO PERF optimize forEach traversal
        for(k in values)
            procedure.invoke(k)
    }


    override fun forEach(action: BiConsumer<in K, in V>){
        //TODO PERF optimize forEach traversal
        for(e in entries){
            action.accept(e.key, e.value)
        }
    }

    override fun isClosed(): Boolean {
        return volume.isClosed
    }

    fun close(){
        volume.close()
    }

    override fun putIfAbsentBoolean(key: K?, value: V?): Boolean {
        throw UnsupportedOperationException("read-only")
    }



    @Throws(ObjectStreamException::class)
    private fun writeReplace(): Any {
        val ret = ConcurrentSkipListMap<Any?, Any?>()
        forEach { k, v ->
            ret.put(k, v)
        }
        return ret
    }
}