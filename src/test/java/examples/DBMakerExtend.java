package examples;

import org.mapdb.*;

import java.io.File;

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
            return new EngineWrapper.DebugEngine(engine);
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
