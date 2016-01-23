package org.mapdb

import com.google.common.collect.Iterators
import com.gs.collections.api.list.primitive.MutableLongList
import com.gs.collections.impl.list.mutable.primitive.LongArrayList
import com.gs.collections.impl.set.mutable.primitive.LongHashSet
import com.gs.collections.impl.stack.mutable.primitive.LongArrayStack
import org.mapdb.BTreeMapJava.*
import java.io.PrintStream
import java.util.*
import java.util.concurrent.ConcurrentMap

/**
 * Concurrent sorted BTree Map
 */
class BTreeMap<K,V>(
        val keySerializer:Serializer<K>,
        val valueSerializer:Serializer<V>,
        val rootRecidRecid:Long,
        val store:Store,
        val maxNodeSize:Int
):Verifiable, ConcurrentMap<K, V> {

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

    internal val nodeSerializer = NodeSerializer(keySerializer, valueSerializer);

    internal val rootRecid:Long
        get() = store.get(rootRecidRecid, Serializer.RECID)
                ?: throw DBException.DataCorruption("Root Recid not found");

    /** recids of left-most nodes in tree */
    internal val leftEdges: MutableLongList = {
        val ret = LongArrayList()

        var recid = rootRecid
        while(true){
            val node = getNode(recid)
            if(CC.ASSERT && recid<=0L)
                throw AssertionError()
            ret.add(recid)
            if(node.isDir.not())
                break
            recid = node.children[0]
        }

        ret.toReversed().asSynchronized()
    }()

    override operator fun get(key:K?):V?{
        if(key==null)
            throw NullPointerException()

        var current =  rootRecid
        var A = getNode(current)

        //dive into bottom
        while(A.isDir){
            current =  findChild(A, COMPARATOR, key)
            A = getNode(current)
        }

        //follow link until necessary
        var ret = leafGet(A,COMPARATOR, key)
        while(LINK==ret){
            current = A.link;
            A = getNode(current)
            ret = leafGet(A,COMPARATOR, key)
        }
        return ret as V?;
    }
    override fun put(key:K?, value:V?):V?{
        if(key==null || value==null)
            throw NullPointerException()
        return put2(key,value, false)
    }

    fun put2(key:K, value:V, onlyIfAbsent:Boolean):V?{
        if(key==null || value==null)
            throw NullPointerException()

        var v = key!!
        var completed = false
        val stack = LongArrayStack()
        val rootRecid = rootRecid

        var current = rootRecid

        var A = getNode(current)
        while(A.isDir){
            var t = current
            current = findChild(A, COMPARATOR, v)
            if(current!=A.link){
                stack.push(t)
            }
            A = getNode(current)
        }

        var level = 1
        var p=0L
        do{

            leafLink@ while(true){
                lock(current)

                A = getNode(current)

                //follow link, until key is higher than highest key in node
                if(!A.isRightEdge && COMPARATOR.compare(v, A.highKey)>0){
                    //key is greater, load next link
                    unlock(current)
                    current = A.link
                    continue@leafLink
                }
                break@leafLink
            }

            //current node is locked, and its highest value is higher/equal to key
            var pos = findIndex(A, COMPARATOR, v)
            if(pos>=0){
                //entry exist in current node, so just update
                pos = pos-1+A.intLeftEdge();
                //key exist in node, just update
                val values = (A.values as Array<Any>).clone()
                val oldValue = values[pos] as V

                //update only if not exist, return
                if(!onlyIfAbsent) {
                    values[pos] = value as Any;
                    A = Node(A.flags.toInt(), A.link, A.keys, values)
                    store.update(current, A, nodeSerializer)
                }
                unlock(current)
                return oldValue
            }

            //normalise pos
            pos = -pos-1

            //key does not exist, node must be expanded
            A = if(A.isDir) copyAddKeyDir(A, pos, v, p)
                else copyAddKeyLeaf(A, pos, v, value)

            if(A.keys.size < maxNodeSize){
                //it is safe to insert without spliting
                store.update(current, A, nodeSerializer)
                unlock(current)
                return null
            }

            //node is not safe it requires split
            val splitPos = A.keys.size/2
            val B = copySplitRight(A, splitPos)
            val q = store.put(B, nodeSerializer)
            A = copySplitLeft(A, splitPos, q)
            store.update(current, A, nodeSerializer)

            if(current != rootRecid){
                //is not root
                unlock(current)
                p = q
                v = A.highKey as K
//                if(CC.ASSERT && COMPARATOR.compare(v, key)<0)
//                    throw AssertionError()
                level++
                current = if(stack.isEmpty.not()){
                    stack.pop()
                }else{
                    //pointer to left most node at level
                    leftEdges.get(level - 1)
                }
            }else{
                //is root
                val R = Node(
                        DIR + LEFT + RIGHT,
                        0L,
                        arrayOf(A.highKey),
                        longArrayOf(current, q)
                )

                unlock(current)
                lock(rootRecidRecid)
                val newRootRecid = store.put(R, nodeSerializer)
                leftEdges.add(newRootRecid)
                //TODO there could be a race condition between leftEdges  update and rootRecidRef update. Investigate!
                store.update(rootRecidRecid, newRootRecid, Serializer.RECID)

                unlock(rootRecidRecid)

                return null;
            }

        }while(!completed)

        return null
    }

    override fun remove(key:K?):V?{
        if(key==null)
            throw NullPointerException()

        return removeOrReplace(key, null, null)
    }

    protected fun removeOrReplace(key:K, expectedOldValue:V?, replaceWithValue:V?):V?{
        if(key==null)
            throw NullPointerException()

        val v = key

        val rootRecid = rootRecid

        var current = rootRecid

        var A = getNode(current)
        while(A.isDir){
            current = findChild(A, COMPARATOR, v)
            A = getNode(current)
        }

        leafLink@ while(true){
            lock(current)

            A = getNode(current)

            //follow link, until key is higher than highest key in node
            if(!A.isRightEdge && COMPARATOR.compare(v, A.highKey)>0){
                //key is greater, load next link
                unlock(current)
                current = A.link
                continue@leafLink
            }
            break@leafLink
        }

        //current node is locked, and its highest value is higher/equal to key
        val pos = findIndex(A, COMPARATOR, v)
        var oldValue:V? = null
        if(pos>=1-A.intLeftEdge() && pos!=A.keys.size-1+A.intRightEdge()){
            val valuePos = pos-1+A.intLeftEdge();
            //key exist in node, just update
            var values = (A.values as Array<Any>)
            oldValue = values[valuePos] as V
            var keys = A.keys
            if(expectedOldValue==null || valueSerializer.equals(expectedOldValue!!,oldValue)) {
                if(replaceWithValue==null){
                    //remove
                    keys = arrayRemove(keys, pos)
                    values = arrayRemove(values, valuePos)
                }else{
                    //just replace value, do not modify keys
                    values = values.clone()
                    values[valuePos] = replaceWithValue
                }

                A = Node(A.flags.toInt(), A.link, keys, values)
                store.update(current, A, nodeSerializer)
            }else{
                oldValue = null
            }
        }
        unlock(current)
        return oldValue
    }


    private fun copySplitLeft(a: Node, splitPos: Int, link:Long): Node {
        val flags = a.intDir()*DIR + a.intLeftEdge()*LEFT

        val keys = Arrays.copyOfRange(a.keys, 0, splitPos+1-a.intDir())
        if(!a.isDir)
            keys[keys.size-1] = keys[keys.size-2] //TODO lastKeyDouble flag

        val valSplitPos = splitPos-1+a.intLeftEdge();
        val values = if(a.isDir){
            val c = a.values as LongArray
            Arrays.copyOfRange(c, 0, valSplitPos)
        }else{
            val c = a.values as Array<Any>
            Arrays.copyOfRange(c, 0, valSplitPos)
        }

        return Node(flags, link, keys, values)

    }

    private fun copySplitRight(a: Node, splitPos: Int): Node {
        val flags = a.intDir()*DIR + a.intRightEdge()*RIGHT + a.intLastKeyDouble()*LAST_KEY_DOUBLE

        val keys = Arrays.copyOfRange(a.keys, splitPos-1, a.keys.size)

        val valSplitPos = splitPos-1+a.intLeftEdge();
        val values = if(a.isDir){
            val c = a.values as LongArray
            Arrays.copyOfRange(c, valSplitPos, c.size)
        }else{
            val c = a.values as Array<Any>
            Arrays.copyOfRange(c, valSplitPos, c.size)
        }

        return Node(flags, a.link, keys, values)
    }


    private fun copyAddKeyLeaf(a: Node, insertPos: Int, key: K, value: V): Node {
        if(CC.ASSERT && a.isDir)
            throw AssertionError()

        val keys = arrayPut(a.keys, insertPos, key)

        val valuesInsertPos = insertPos-1+a.intLeftEdge();
        val values = arrayPut(a.values as Array<Any>, valuesInsertPos, value)

        return Node(a.flags.toInt(), a.link, keys, values)
    }

    private fun copyAddKeyDir(a: Node, insertPos: Int, key: K, newChild: Long): Node {
        if(CC.ASSERT && a.isDir.not())
            throw AssertionError()

        val keys = arrayPut(a.keys, insertPos, key)

        val values = arrayPut(a.values as LongArray, insertPos+a.intLeftEdge(), newChild)

        return Node(a.flags.toInt(), a.link, keys, values)
    }



    fun lock(nodeRecid:Long){
        //TODO node locking
    }

    fun unlock(nodeRecid:Long){
        //TODO node locking
    }

    override fun verify() {
        fun verifyRecur(node:Node, left:Boolean, right:Boolean, knownNodes:LongHashSet, nextNodeRecid:Long){
            if(left!=node.isLeftEdge)
                throw AssertionError("left does not match $left")
            if(right!=node.isRightEdge)
                throw AssertionError("right does not match $right")

            //check keys are sorted, no duplicates
            for(i in 1 until node.keys.size){
                val compare = COMPARATOR.compare(node.keys[i-1], node.keys[i])
                val cresult = if(i==node.keys.size-1) 1 else 0
                if(compare>=cresult)
                    throw AssertionError("Not sorted: "+Arrays.toString(node.keys))
            }

            //iterate over child
            if(node.isDir){
                val child = node.values as LongArray
                var prevLink = 0L;
                for(i in child.size-1 downTo 0){
                    val recid = child[i]

                    if(knownNodes.contains(recid))
                        throw AssertionError("recid duplicate: $recid")
                    knownNodes.add(recid)
                    var node = getNode(recid)
                    verifyRecur(node, left = (i==0)&&left, right= (child.size==i+1)&&right,
                            knownNodes = knownNodes, nextNodeRecid=nextNodeRecid)

                    //TODO implement follow link
//                    //follow link until next node is found
//                    while(node.link!=prevLink){
//                        if(knownNodes.contains(node.link))
//                            throw AssertionError()
//                        knownNodes.add(node.link)
//
//                        node = getNode(node.link)
//
//                        verifyRecur(node, left = false, right= node.link==0L,
//                                knownNodes = knownNodes, nextNodeRecid=prevLink)
//                    }
                    prevLink = recid
                }
            }
        }


        val rootRecid = rootRecid
        val node = getNode(rootRecid)

        val knownNodes = LongHashSet.newSetWith(rootRecid)



        verifyRecur(node, left=true, right=true, knownNodes=knownNodes, nextNodeRecid = 0L)

        //verify that linked nodes share the same keys on their edges
        for(leftRecid in leftEdges.toArray()){

            if(knownNodes.contains(leftRecid).not())
                throw AssertionError()
            var node = getNode(leftRecid)
            if(!knownNodes.remove(leftRecid))
                throw AssertionError()

            while(node.isRightEdge.not()){
                //TODO enable once links are traced
//                if(!knownNodes.remove(node.link))
//                    throw AssertionError()

                val next = getNode(node.link)
                if(COMPARATOR.compare(node.highKey,next.keys[0])!=0)
                    throw AssertionError(node.link)

                node = next
            }
        }
        //TODO enable once links are traced
//        if(knownNodes.isEmpty.not())
//            throw AssertionError(knownNodes)
    }



    private fun getNode(nodeRecid:Long) =
            store.get(nodeRecid, nodeSerializer)
                ?: throw DBException.DataCorruption("Node not found")

    fun printStructure(out: PrintStream){
        fun printRecur(nodeRecid:Long, prefix:String){
            val node = getNode(nodeRecid);
            var str = if(node.isDir) "DIR " else "LEAF "

            if(node.isLeftEdge)
                str+="L"
            if(node.isRightEdge)
                str+="R"
            if(node.isLastKeyDouble)
                str+="D"
            str+=" recid=$nodeRecid, link=${node.link}, keys="+Arrays.toString(node.keys)+", "


            str+= if(node.isDir) "child="+Arrays.toString(node.children)
                  else "vals="+Arrays.toString(node.values as Array<Any>)

            out.println(prefix+str)

            if(node.isDir){
                node.children.forEach{
                    printRecur(it, "  "+prefix)
                }
            }
        }

        printRecur(rootRecid,"")

    }


    override fun putAll(from: Map<out K?, V?>) {
        for(e in from.entries){
            put(e.key, e.value)
        }
    }


    override fun putIfAbsent(key: K?, value: V?): V? {
        if(key==null || value==null)
            throw NullPointerException()
        return put2(key, value, true)
    }

    override fun remove(key: Any?, value: Any?): Boolean {
        if(key==null || value==null)
            throw NullPointerException()
        return removeOrReplace(key as K, value as V, null)!=null
    }

    override fun replace(key: K?, oldValue: V?, newValue: V?): Boolean {
        if(key==null || oldValue==null || newValue==null)
            throw NullPointerException()
        return removeOrReplace(key, oldValue, newValue)!=null
    }

    override fun replace(key: K?, value: V?): V? {
        if(key==null || value==null)
            throw NullPointerException()
        return removeOrReplace(key, null, value)
    }

    override fun clear() {
        //TODO PERF optimize, traverse nodes and clear each node in one step
        val iter = keys.iterator();
        while(iter.hasNext()){
            iter.next()
            iter.remove()
        }
    }

    override fun containsKey(key: K): Boolean {
        return get(key)!=null
    }

    override fun containsValue(value: V): Boolean {
        return values.contains(value)
    }

    override fun isEmpty(): Boolean {
        return keys.iterator().hasNext().not()
    }

    override val size: Int
        get() = Math.min(Int.MAX_VALUE.toLong(), sizeLong()).toInt()

    fun sizeLong():Long{
        var ret = 0L
        val iter = keys.iterator()
        while(iter.hasNext()){
            iter.next()
            ret++
        }
        return ret
    }

    //TODO retailAll etc should use serializers for comparasions, remove AbstractSet and AbstractCollection completely
    //TODO PERF replace iterator with forEach, much faster indexTree traversal
    override val entries: MutableSet<MutableMap.MutableEntry<K?, V?>> = object : AbstractSet<MutableMap.MutableEntry<K?, V?>>() {

        override fun add(element: MutableMap.MutableEntry<K?, V?>): Boolean {
            this@BTreeMap.put(element.key, element.value)
            return true
        }


        override fun clear() {
            this@BTreeMap.clear()
        }

        override fun iterator(): MutableIterator<MutableMap.MutableEntry<K?, V?>> {
            return object:BTreeIterator<K,V>(this@BTreeMap), MutableIterator<MutableMap.MutableEntry<K?, V?>>{
                override fun next(): MutableMap.MutableEntry<K?, V?> {
                    val leaf = currentLeaf ?: throw NoSuchElementException()
                    val key = leaf.keys[currentPos] as K
                    val value = (leaf.values as Array<Any>)[currentPos-1+leaf.intLeftEdge()] as V
                    advance()
                    return btreeEntry(key, value)
                }
            }
        }

        override fun remove(element: MutableMap.MutableEntry<K?, V?>?): Boolean {
            if(element==null || element.key==null|| element.value==null)
                throw NullPointerException()
            return this@BTreeMap.remove(element.key as Any, element.value)
        }


        override fun contains(element: MutableMap.MutableEntry<K?, V?>): Boolean {
            val v = this@BTreeMap.get(element.key)
                    ?: return false
            val value = element.value
                    ?: return false
            return valueSerializer.equals(value,v)
        }

        override fun isEmpty(): Boolean {
            return this@BTreeMap.isEmpty()
        }

        override val size: Int
            get() = this@BTreeMap.size

    }


    override val keys: MutableSet<K?> = object : AbstractSet<K>(){

        override fun iterator(): MutableIterator<K?> {
            return object: BTreeIterator<K,V>(this@BTreeMap), MutableIterator<K?>{
                override fun next(): K? {
                    val leaf = currentLeaf?:throw NoSuchElementException()
                    val key = leaf.keys[currentPos]
                    advance()
                    return key as K
                }
            }
        }

        override val size: Int
            get() = this@BTreeMap.size


        override fun add(element: K): Boolean {
            throw UnsupportedOperationException("Can not add without val")
        }

        override fun clear() {
            this@BTreeMap.clear()
        }

        override fun isEmpty(): Boolean {
            return this@BTreeMap.isEmpty()
        }

        override fun remove(element: K): Boolean {
            return this@BTreeMap.remove(element)!=null
        }
    }



    override val values: MutableCollection<V?> = object : AbstractCollection<V>(){

        override fun clear() {
            this@BTreeMap.clear()
        }

        override fun isEmpty(): Boolean {
            return this@BTreeMap.isEmpty()
        }

        override val size: Int
            get() = this@BTreeMap.size


        override fun iterator(): MutableIterator<V?> {
            return object: BTreeIterator<K,V>(this@BTreeMap), MutableIterator<V?>{
                override fun next(): V? {
                    val leaf = currentLeaf?:throw NoSuchElementException()
                    val value = (leaf.values as Array<Any>)[currentPos-1+leaf.intLeftEdge()]
                    advance()
                    return value as V
                }
            }
        }

        override fun contains(element: V): Boolean {
            if(element==null)
                throw NullPointerException()
            return super.contains(element)
        }
    }



    abstract class BTreeIterator<K,V>(val m:BTreeMap<K,V>){

        protected var currentPos = -1
        protected var currentLeaf:Node? = null
        protected var lastReturnedKey: K? = null

        init{
            advanceFrom(m.leftEdges.first!!)
        }


        fun hasNext():Boolean = currentLeaf!=null

        fun remove() {
            m.remove(lastReturnedKey ?: throw IllegalStateException())
            this.lastReturnedKey = null
        }


        private fun advanceFrom(recid: Long) {
            var node: Node? =
                    if(recid==0L) null
                    else m.getNode(recid);
            // iterate until node is not empty or link is not found
            while (node != null && node.keys.size == 2 - node.intLeftEdge() - node.intRightEdge()) {
                node =
                        if (node.isRightEdge) null
                        else m.getNode(node.link)
            }
            //set leaf
            currentLeaf = node
            currentPos = if (node == null) -1 else 1 - node.intLeftEdge()
        }

        protected fun advance(){
            val currentLeaf:Node = currentLeaf?:return
            lastReturnedKey = currentLeaf.keys[currentPos] as K
            currentPos++

            if(currentPos == currentLeaf.keys.size-1+currentLeaf.intRightEdge()){
                //reached end of current node, iterate to next
                advanceFrom(currentLeaf.link)
            }
        }
    }


    protected fun btreeEntry(key:K, valueOrig:V) : MutableMap.MutableEntry<K?,V?>{

        return object : MutableMap.MutableEntry<K?,V?>{
            override val key: K?
                get() = key

            override val value: V?
                get() = valueCached ?: this@BTreeMap.get(key)

            /** cached value, if null get value from map */
            private var valueCached:V? = valueOrig;

            override fun hashCode(): Int {
                return keySerializer.hashCode(this.key!!, 0) xor valueSerializer.hashCode(this.value!!,0)
            }
            override fun setValue(newValue: V?): V? {
                valueCached = null;
                return put(key,newValue)
            }


            override fun equals(other: Any?): Boolean {
                if (other !is Map.Entry<*, *>)
                    return false
                val okey = other.key ?: return false
                val ovalue = other.value ?: return false
                try{
                    return keySerializer.equals(key, okey as K)
                            && valueSerializer.equals(this.value!!, ovalue as V)
                }catch(e:ClassCastException) {
                    return false
                }
            }

            override fun toString(): String {
                return "MapEntry[${key}=${value}]"
            }

        }
    }

    override fun hashCode(): Int {
        var h = 0
        val i = entries.iterator()
        while (i.hasNext())
            h += i.next().hashCode()
        return h
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


    fun isClosed(): Boolean {
        return store.isClosed()
    }


}