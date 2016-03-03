
package org.mapdb

import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList
import java.io.PrintStream

/**
 * FIFO Queue with option to remove element from middle
 */

//TODO sequentially unsafe
class QueueLong(
    val store:Store,
    val tailRecid: Long,
    val headRecid: Long,
    val headPrevRecid: Long
):Verifiable{

    init{
        if(CC.ASSERT && tailRecid == headRecid)
            throw AssertionError("head==tail")
    }
    companion object{
        fun make(
                store:Store = StoreTrivial(),
                tailRecid:Long = store.put(store.put(null, Node.SERIALIZER), Serializer.RECID),
                //code bellow takes value from `tailRecid` and saves it as different record
                headRecid:Long = store.put(store.get(tailRecid, Serializer.RECID), Serializer.RECID),
                headPrevRecid:Long = store.put(0L, Serializer.RECID)
        ) = QueueLong(store=store, tailRecid=tailRecid, headRecid = headRecid, headPrevRecid = headPrevRecid)
    }

    data class Node(
            val prevRecid: Long,
            val nextRecid:Long,
            val timestamp:Long,
            val value:Long) {

        object SERIALIZER : Serializer<Node> {

            override fun serialize(out: DataOutput2, value: Node) {
                Serializer.RECID.serialize(out, value.prevRecid)
                Serializer.RECID.serialize(out, value.nextRecid)
                out.packLong(value.timestamp)
                out.packLong(value.value)
            }

            override fun deserialize(input: DataInput2, available: Int): Node? {
                return Node(
                    prevRecid = Serializer.RECID.deserialize(input, -1),
                    nextRecid = Serializer.RECID.deserialize(input, -1),
                    timestamp = input.unpackLong(),
                    value = input.unpackLong()
                )
            }

        }
    }

    var tail:Long
        get() = store.get(tailRecid, Serializer.RECID)!!
        set(value:Long) = store.update(tailRecid, value, Serializer.RECID)

    var head:Long
        get() = store.get(headRecid, Serializer.RECID)!!
        set(value:Long) = store.update(headRecid, value, Serializer.RECID)

    var headPrev:Long
        get() = store.get(headPrevRecid, Serializer.RECID)!!
        set(value:Long) = store.update(headPrevRecid, value, Serializer.RECID)


    /** puts Node into queue, returns recid which represents this node */
    fun put(timestamp: Long, value:Long ):Long{
        //allocate next node, we need its recid for 'nextRecid'
        val nextRecid = store.put(null, Node.SERIALIZER)

        // get heads and update it to point to next element
        //TODO PERF get/update in single operation
        val head2 = head
        head = nextRecid //update head to point to next element
        val headPrev2 = headPrev
        headPrev = head2

        val node = Node(prevRecid= headPrev2,
                nextRecid=nextRecid, timestamp=timestamp, value=value)
        store.update(head2, node, Node.SERIALIZER)

        return head2
    }

    /** puts Node into queue, returns recid which represents this node */
    fun put(timestamp: Long, value:Long, nodeRecid:Long){
        //update inserted node
        val prevRecid = headPrev
        val head2 = head
        store.update(nodeRecid, Node(prevRecid = prevRecid, nextRecid = head2,
                timestamp = timestamp, value=value), Node.SERIALIZER)

        //update headPrev
        headPrev = nodeRecid

        //and update previous node
        if(prevRecid!=0L){
            val prevNode = store.get(prevRecid, Node.SERIALIZER)
                ?:throw DBException.DataCorruption("prev node not found")
            store.update(prevRecid, prevNode.copy(nextRecid=nodeRecid),Node.SERIALIZER)
        }
        val tail2 = tail;
        if(tail2===head2){
            //update tail
            tail = nodeRecid
        }
    }


    fun take(): Node?{
        val tail2 = tail
        val curr = store.get(tail2, Node.SERIALIZER)
        if(curr!=null){
            // move element to next tail, if it exists
            store.delete(tail2, Node.SERIALIZER)
            tail = curr.nextRecid
            //zero out headPrev if needed
            store.compareAndSwap(headPrevRecid, tail2, 0L, Serializer.RECID)
            //fix prevRecid
            //TODO it should be possible to eliminate this step by comparing tail and node recid in #bump()
            val nextNode = store.get(curr.nextRecid, Node.SERIALIZER)
            if(nextNode!=null) { // did we reached end?
                //not, update prev node
                store.update(curr.nextRecid, nextNode.copy(prevRecid = 0L), Node.SERIALIZER)
            }else{
                //TODO update something?
            }
        }else{
            //is last element, so zero out headPrev
            headPrev = 0L;
        }
        return curr
    }


    /** Takes elements, until callback returns true. When callback returns false, last node is preserved in Queue*/
    fun takeUntil(f:QueueLongTakeUntil){
        while(true){
            val tail2 = tail
            val node = store.get(tail2, Node.SERIALIZER)
                    ?: return // reached head

            if(CC.ASSERT && node.prevRecid!=0L)
                throw DBException.DataCorruption("prevRecid not 0")

            val taken = f.take(tail2, node);
            if(!taken)
                return

            val nodeTaken = take()
            if(CC.ASSERT && node.value!=nodeTaken!!.value)
                throw DBException.DataCorruption("wrong nodes")
        }
    }

    fun remove(nodeRecid: Long, removeNode:Boolean):Node{
        //TODO PERF get/Delete in single operation
        val node = store.get(nodeRecid, Node.SERIALIZER)!!
        if(removeNode)
            store.delete(nodeRecid, Node.SERIALIZER)

        //TODO get/update in single operation, take transformation as an argument
        val nextNode = store.get(node.nextRecid, Node.SERIALIZER)
        if(nextNode!=null) {
            if(CC.ASSERT && nextNode.prevRecid!=nodeRecid)
                throw DBException.DataCorruption("node link error")
            store.update(node.nextRecid, nextNode.copy(prevRecid = node.prevRecid), Node.SERIALIZER)
        }else{
            if(CC.ASSERT && headPrev!=nodeRecid)
                throw DBException.DataCorruption("headPrev error")
            headPrev = node.prevRecid
        }

        if(node.prevRecid!=0L) {
            val prevNode = store.get(node.prevRecid, Node.SERIALIZER)
            if (prevNode != null) {
                if(CC.ASSERT && prevNode.nextRecid!=nodeRecid)
                    throw DBException.DataCorruption("node link error")
                store.update(node.prevRecid, prevNode.copy(nextRecid = node.nextRecid), Node.SERIALIZER)
            }
        }else{
            if(CC.ASSERT && tail!=nodeRecid)
                throw DBException.DataCorruption("tail error")
            tail = node.nextRecid
        }
        return node;
    }

    fun bump(nodeRecid: Long, newTimestamp:Long){
        val headPrev2 = headPrev
        if(headPrev2==nodeRecid){
            //already at top of queue, just update timestamp
            val node = store.get(nodeRecid,Node.SERIALIZER)
                ?: throw DBException.DataCorruption("link error")
            store.update(nodeRecid, node.copy(timestamp=newTimestamp), Node.SERIALIZER)
            return
        }

        //TODO PERF get/Delete in single operation
        val node = store.get(nodeRecid, Node.SERIALIZER)!!

        // remove this node from linkage

        //TODO get/update in single operation, take transformation as an argument
        val nextNode = store.get(node.nextRecid, Node.SERIALIZER)
        if(nextNode!=null) {
            if(CC.ASSERT && nextNode.prevRecid!=nodeRecid)
                throw DBException.DataCorruption("node link error")
            store.update(node.nextRecid, nextNode.copy(prevRecid = node.prevRecid), Node.SERIALIZER)
        }else{
            if(CC.ASSERT && headPrev!=nodeRecid)
                throw DBException.DataCorruption("headPrev error")
            headPrev = node.prevRecid
        }

        if(node.prevRecid!=0L) {
            val prevNode = store.get(node.prevRecid, Node.SERIALIZER)
            if (prevNode != null) {
                if(CC.ASSERT && prevNode.nextRecid!=nodeRecid)
                    throw DBException.DataCorruption("node link error")
                store.update(node.prevRecid, prevNode.copy(nextRecid = node.nextRecid), Node.SERIALIZER)
            }
        }else{
            if(CC.ASSERT && tail!=nodeRecid)
                throw DBException.DataCorruption("tail error")
            tail = node.nextRecid
        }

        //insert this node to end

        headPrev = nodeRecid
        //update previous node to point here
        val headPrevNode = store.get(headPrev2, Node.SERIALIZER)!!
        store.update(headPrev2, headPrevNode.copy(nextRecid=nodeRecid), Node.SERIALIZER)

        val newNode = node.copy(prevRecid = headPrev2, nextRecid = headPrevNode.nextRecid, timestamp = newTimestamp)
        store.update(nodeRecid, newNode, Node.SERIALIZER)

    }

    fun clear(){
        takeUntil(QueueLongTakeUntil { l, p -> true })
    }

    fun size():Long{
        var ret = 0L;

        val head = head
        var currentRecid = tail;
        while(head!=currentRecid){
            val node = store.get(currentRecid,Node.SERIALIZER)
                ?: throw DBException.DataCorruption("linked queue node not found")
            currentRecid = node.nextRecid
            ret++
        }


        return ret;
    }

    override fun verify(){
        val head = head
        val tail = tail
        val headPrev =headPrev

        if(head==tail){
            //empty queue
            if(headPrev!=0L)
                throw AssertionError("headPrev not 0")
            return
        }

        var node = store.get(tail, Node.SERIALIZER)
            ?: throw AssertionError("node not found")
        if(node.prevRecid!=0L)
            throw AssertionError("prevRecid not 0")
        var prevRecid = tail;

        while(node.nextRecid!=head){
            val recid = node.nextRecid
            node = store.get(recid, Node.SERIALIZER)
                ?: throw AssertionError("node not found")
            if(prevRecid!=node.prevRecid)
                throw AssertionError("prev recid")

            prevRecid = recid
        }

        if(store.get(head,Node.SERIALIZER)!=null)
            throw AssertionError("prealloc record")
        if(prevRecid != headPrev)
            throw AssertionError("wrong headPrevRecid")

    }

    fun valuesArray():LongArray{
        val ret = LongArrayList()

        var currRecid = tail
        while(true) {

            val node = store.get(currRecid, Node.SERIALIZER)
                    ?: return ret.toArray() // reached head

            ret.add(node.value)

            currRecid = node.nextRecid
        }
    }

    fun forEach(body:(expireRecid:Long, value:Long, timestamp:Long)->Unit){
        var currRecid = tail
        while(true) {
            val node = store.get(currRecid, Node.SERIALIZER)
                    ?: return
            body(currRecid, node.value, node.timestamp)
            currRecid = node.nextRecid
        }
    }

    fun printContent(out: PrintStream){
        var currRecid = tail
        out.println("==============================")
        out.println("TAIL:$tail, HEAD:$head, HEADPREV:$headPrev")
        while(true) {

            val node = store.get(currRecid, Node.SERIALIZER)
                    ?: break // reached head

            out.println("recid:$currRecid, prev:${node.prevRecid}, next:${node.nextRecid}, timestamp:${node.timestamp}, value:${node.value}")

            currRecid = node.nextRecid
        }
        out.println("==============================")
    }

}
