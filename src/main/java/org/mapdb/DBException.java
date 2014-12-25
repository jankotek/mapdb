package org.mapdb;

import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;

/**
 * General exception returned by MapDB if something goes wrong.
 * Subclasses inform about specific failure.
 *
 */
public class DBException extends RuntimeException{


    public DBException(String message) {
        super(message);
    }

    public DBException(String message, Throwable cause) {
        super(message,cause);
    }


    public static class EngineGetVoid extends DBException{
        public EngineGetVoid(){
            super("Recid passed to Engine.get() does not exist. Possible data corruption!");
        }
    }

    public static class EngineCompactUncommited extends DBException{
        public EngineCompactUncommited(){
            super("Engine.compact() called while there are uncommited data. Commit first, than compact!");
        }
    }

    /** @see java.nio.channels.ClosedByInterruptException */
    //TODO this thread was interrupted while doing IO?
    public static class VolumeClosedByInterrupt extends VolumeClosed{
        public VolumeClosedByInterrupt(ClosedByInterruptException cause){
            super("Some thread was interrupted while doing IO, and FileChannel was closed in result.", cause);
        }
    }

    public static class VolumeClosed extends DBException{
        public VolumeClosed(IOException cause){
            this("Volume (file or other device) was already closed.", cause);
        }

        protected VolumeClosed(String msg, IOException cause) {
            super(msg,cause);
        }
    }


    public static class VolumeIOError extends DBException{
        public VolumeIOError(IOException cause){
            super("IO failed", cause);
        }
    }


    public static class DataCorruption extends DBException{
        public DataCorruption(String msg){
            super(msg);
        }
    }

    public static class ChecksumBroken extends DataCorruption{
        public ChecksumBroken(){
            super("CRC checksum is broken");
        }
    }

    public static class HeadChecksumBroken extends DataCorruption{
        public HeadChecksumBroken(){
            super("Head checksum broken, perhaps db was not closed correctly?");
        }
    }

    public static class PointerChecksumBroken extends DataCorruption{
        public PointerChecksumBroken(){
            super("Bit parity in file pointer is broken, data possibly corrupted.");
        }
    }

}
