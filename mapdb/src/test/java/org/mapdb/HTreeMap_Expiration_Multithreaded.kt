package org.mapdb


import org.junit.Test
import java.util.Random
import java.util.UUID
import java.util.concurrent.TimeUnit

class HTreeMap_Expiration_Multithreaded {

    internal val duration = 10 * 60 * 1000.toLong()
    internal var b = ByteArray(100)

    @Test fun expireUUID() {
        if (TT.shortTest())
            return

        val endTime = duration + System.currentTimeMillis()

        val db = DBMaker.memoryDB().make()
        val m = db.hashMap("aa")
                .keySerializer(Serializer.UUID)
                .valueSerializer(Serializer.BYTE_ARRAY)
                .expireAfterCreate(20, TimeUnit.SECONDS)
                .expireExecutor(TT.executor(3))
                .create()

        TT.fork(10) {
            var r = Random(1)
            run {
                var i = 0
                while (i < 2e5){
                    val u = UUID(r.nextLong(), r.nextLong())
                    m.put(u, b)
                    i++
                }
            }

            while (System.currentTimeMillis() < endTime) {
                r = Random(1)
                run {
                    var i = 0
                    while (i < 1e5){
                        val u = UUID(r.nextLong(), r.nextLong())
                        m.get(u)
                        m.put(u, b)
                        i++
                    }
                }
                var i = 1e5.toInt()
                while (i < 2e5){
                    val u = UUID(r.nextLong(), r.nextLong())
                    m[u]
                    i++
                }
            }
        }
    }
}
