package org.mapdb

import com.gs.collections.api.list.primitive.MutableLongList
import com.gs.collections.impl.list.mutable.primitive.LongArrayList
import com.gs.collections.impl.set.mutable.primitive.LongHashSet
import com.gs.collections.impl.stack.mutable.primitive.LongArrayStack
import org.mapdb.BTreeMapJava.*
import java.io.PrintStream
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
):Verifiable{

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

    operator fun get(key:K):V?{
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

    fun put(key:K, value:V):V?{
        var v = key
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

            //normalise pos
            pos = -pos-1

            //key does not exist, node must be expanded
            A = if(A.isDir) copyAddKeyDir(A, pos, v, p)
                else copyAddKeyLeaf(A, pos, v, value)

            if(A.keys.size < maxNodeSize){
                //it is safe to insert without spliting
                store.update(current, A, nodeSer)
                unlock(current)
                return null
            }

            //node is not safe it requires split
            val splitPos = A.keys.size/2
            val B = copySplitRight(A, splitPos)
            val q = store.put(B, nodeSer)
            A = copySplitLeft(A, splitPos, q)
            store.update(current, A, nodeSer)

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
                val newRootRecid = store.put(R, nodeSer)
                leftEdges.add(newRootRecid)
                //TODO there could be a race condition between leftEdges  update and rootRecidRef update. Investigate!
                store.update(rootRecidRecid, newRootRecid, Serializer.RECID)

                unlock(rootRecidRecid)

                return null;
            }

        }while(!completed)

        return null
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

    }

    fun unlock(nodeRecid:Long){

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
                    throw AssertionError()

                node = next
            }
        }
        //TODO enable once links are traced
//        if(knownNodes.isEmpty.not())
//            throw AssertionError(knownNodes)
    }



    private fun getNode(nodeRecid:Long) =
            store.get(nodeRecid, nodeSer)
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

}