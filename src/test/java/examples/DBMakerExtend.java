package examples;

import org.mapdb.*;

import java.io.File;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * DBMaker behavior can be extended or modified by extending class and overriding `extendXXX()` methods.
 * Those methods are not yet documented, so you have to study `DBMaker.java` source code.
 *
 * This example shows how-to add extra logging into DBMaker
 * It also add extra option to optionally disable logging
 */
public class DBMakerExtend {

    //this class extends `DBMaker` and add our own options
    //generics are used so each option returns `ExtendedDBMaker` instead of old `DBMaker`
    static class ExtendedDBMaker extends DBMaker<ExtendedDBMaker>{


        //override one of protected extension methods
        @Override
        protected Engine extendWrapStore(Engine engine) {
            //do not wrap if this option was disabled
            if(!loggingEnabled)
                return engine;
            //modify engine behaviour by wrapping it
            return new DebugEngine(engine);
        }

        //logging is enabled by default in our DBMaker
        protected boolean loggingEnabled = true;

        //there is also new option to disable it
        ExtendedDBMaker loggingDisable(){
            loggingEnabled = false;
            return getThis();
        }
    }

    public static void main(String[] args) {
        File dbFile = new File("db");
        DB db = new ExtendedDBMaker() //static constructors can not be used on extended class
                ._newFileDB(dbFile) //so use constructor and call method with underscore
                .compressionEnable()
                .loggingDisable() //this option does not exist in original DBMaker
                .make();

        BTreeMap map = db.getTreeMap("map");

        //and now with DB as usual

    }
}


class DebugEngine extends EngineWrapper{

    final Queue<Record> records = new ConcurrentLinkedQueue<Record>();

    protected static final class Record{
        final long recid;
        final String desc;

        public Record(long recid, String desc) {
            this.recid = recid;
            this.desc = desc;
        }
    }

    public DebugEngine(Engine engine) {
        super(engine);
    }

    @Override
    public long preallocate() {
        long recid =  super.preallocate();
        records.add(new Record(recid,"PREALLOC"));
        return recid;
    }

    @Override
    public void preallocate(long[] recids) {
        super.preallocate(recids);
        for(long recid:recids)
            records.add(new Record(recid,"PREALLOC"));
    }


    @Override
    public <A> long put(A value, Serializer<A> serializer) {
        long recid =  super.put(value, serializer);
        records.add(new Record(recid,
                "INSERT \n  val:"+value+"\n  ser:"+serializer
        ));
        return recid;
    }

    @Override
    public <A> A get(long recid, Serializer<A> serializer) {
        A ret =  super.get(recid, serializer);
        records.add(new Record(recid,
                "GET \n  val:"+ret+"\n  ser:"+serializer
        ));
        return ret;
    }

    @Override
    public <A> void update(long recid, A value, Serializer<A> serializer) {
        super.update(recid, value, serializer);
        records.add(new Record(recid,
                "UPDATE \n  val:"+value+"\n  ser:"+serializer
        ));

    }

    @Override
    public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
        final boolean ret = super.compareAndSwap(recid, expectedOldValue, newValue, serializer);
        records.add(new Record(recid,"CAS "+ret+"\n  newVal:"+newValue+"\n  ser:"+serializer));
        return ret;
    }

    @Override
    public <A> void delete(long recid, Serializer<A> serializer){
        super.delete(recid,serializer);
        records.add(new Record(recid,"DEL"));
    }
}
