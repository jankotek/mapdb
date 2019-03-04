package org.mapdb.store

import org.mapdb.DBException
import org.mapdb.io.DataInput2ByteArray
import org.mapdb.ser.Serializer

import java.io.DataOutputStream
import java.io.OutputStream
import java.nio.ByteBuffer

/**
 * Space efficient Store with fast reads, useful for archiving
 */
class StoreArchive(val b: ByteBuffer):Store{

    private val recidCount = b.getLong(OFFSET_RECID_COUNT)
    private val recordBaseOffset = OFFSET_TABLE + recidCount * 12

    fun tableOffset(recid:Long ) = OFFSET_TABLE + (recid-1) * 12

    override fun <K> get(recid: Long, serializer: Serializer<K>): K? {
        if(recid<1 || recid>recidCount)
            throw DBException.RecidNotFound()
        val tableOffset = tableOffset(recid)

        //find offsets
        val recordOffset = recordBaseOffset + b.getLong(tableOffset.toInt())
        val recordSize = b.getInt(tableOffset.toInt()+8)

        //read
        val data = ByteArray(recordSize)
        val b2 = b.duplicate()
        b2.position(recordOffset.toInt())
        b2.get(data)

        //deserialize
        val input = DataInput2ByteArray(data)
        return serializer.deserialize(input)
    }


    override fun getAll(consumer: (Long, ByteArray?) -> Unit) {
        val b2 = b.duplicate()
        for(recid in 1L until recidCount){
            val tableOffset = tableOffset(recid)

            //find offsets
            val recordOffset = recordBaseOffset + b.getLong(tableOffset.toInt())
            val recordSize = b.getInt(tableOffset.toInt()+8)

            //read data
            val data = ByteArray(recordSize)
            b2.position(recordOffset.toInt())
            b2.get(data)

            consumer(recid, data)
        }
    }


    override fun close() {
        //TODO close ByteBuffer?
    }

    companion object {
        private val OFFSET_RECID_COUNT = 0

        private val OFFSET_TABLE = 8

        fun importFromStore(store:Store, out: OutputStream){
            val out2 = DataOutputStream(out)
            val offsets = ArrayList<Triple<Long,Int,ByteArray?>>()

            var recidCount = 0L
            var maxRecid = 0L
            var offset = 0L

            store.getAll{recid, data->
                assert(recid>maxRecid)
                //TODO non linear recids
                assert(recid==maxRecid+1)

                maxRecid = recid
                recidCount++
                val dataSize = data?.size ?: -1
                offsets+=Triple(offset, dataSize, data)

                offset += data?.size ?: 0
            }


            out2.writeLong(recidCount)
            for((offset, size) in offsets) {
                out2.writeLong(offset)
                out2.writeInt(size)
            }
            for((_, _, data) in offsets) {
                if(data!=null)
                    out.write(data)
            }

        }
    }

    override fun isEmpty(): Boolean = recidCount == 0L


}