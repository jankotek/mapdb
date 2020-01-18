package org.mapdb.examples

import org.junit.Test
import org.mapdb.TT
import org.mapdb.db.DB
import org.mapdb.list.DBLists
import org.mapdb.ser.Serializers

class TxProxyExample {


    @Test fun concurrent_transactions(){
        val db = DB.newOnHeapDB().txBlock().make()
        val list = DBLists.newMonoList(db, Serializers.INTEGER).create()

        TT.fork {
            list

        }


        TT.fork {


        }

    }
}