package net.kotek.jdbm;


import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * JDBM abstraction over ByteBuffer.
 * <p/>
 * ByteBuffer has maximal size 2GB, so this class chains multiple ByteBuffers together.
 * IO operations must be padded not to cross ByteBuffer boundaries.
 * <p/>
 * Also ByteBuffer is not grow-able, this solves it.
 */
public final class ByteBuffer2 {

    static final Logger LOG = CC.BB_LOG_WRITES? Logger.getLogger(ByteBuffer2.class.getName()) : null;

    final String logFileName;

    static final int  BUF_SIZE = 1<<30;

    static final int BUF_SIZE_INC = 1024*1024;

    static final int INITIAL_SIZE = 1024*32;

    long availSizeCheck = CC.BB_CHECK_AVAILABLE_SIZE ? 0L : Long.MIN_VALUE ;

    /** file channel backing this ByteBuffer, null for in-memory-store */
    protected FileChannel fileChannel;
    final protected FileChannel.MapMode mapMode;
    protected ByteBuffer[] buffers;
    final protected boolean inMemory;


    public ByteBuffer2(boolean inMemory, FileChannel fileChannel, FileChannel.MapMode mapMode, String logFileName){
        try{
        this.fileChannel = fileChannel;
        this.inMemory = inMemory;
        this.mapMode = mapMode;
        this.logFileName = CC.BB_LOG_WRITES? logFileName : null;
        if(inMemory){
            buffers = new ByteBuffer[]{ByteBuffer.allocate(INITIAL_SIZE)};
            if(mapMode == FileChannel.MapMode.READ_ONLY)
                buffers[0] = buffers[0].asReadOnlyBuffer();
        }else{
            final long fileSize = fileChannel.size();
            if(fileSize>0){
                //map existing data
                buffers = new ByteBuffer[(int) (1+fileSize/BUF_SIZE)];
                for(int i=0;i<=fileSize/BUF_SIZE;i++){
                    final long offset = 1L*BUF_SIZE*i;
                    buffers[i] = fileChannel.map(mapMode, offset, Math.min(BUF_SIZE, fileSize-offset));
                    if(mapMode == FileChannel.MapMode.READ_ONLY)
                        buffers[i] = buffers[i].asReadOnlyBuffer();
                    //TODO what if 'fileSize % 8 != 0'?
                }
            }else{
                buffers = new ByteBuffer[1];
                buffers[0] = fileChannel.map(mapMode, 0, INITIAL_SIZE);
                if(mapMode == FileChannel.MapMode.READ_ONLY)
                    buffers[0] = buffers[0].asReadOnlyBuffer();

            }
        }
        }catch(IOException e){
            throw new IOError(e);
        }
    }


    protected ByteBuffer internalByteBuffer(long offset) {
        return buffers[((int) (offset / BUF_SIZE))];
    }

    public void ensureAvailable(long offset) throws IOException {

        if(CC.BB_LOG_WRITES && LOG.isLoggable(Level.FINEST))
            LOG.finest(logFileName+":ensureAvailable: "+offset);

        if(CC.BB_CHECK_AVAILABLE_SIZE){
            availSizeCheck = Math.max(availSizeCheck, offset);
        }

        int buffersPos = (int) (offset/BUF_SIZE);

        //check for most common case, this is already mapped
        if(buffersPos<buffers.length && buffers[buffersPos]!=null &&
                buffers[buffersPos].capacity()>=offset%BUF_SIZE)
            return;

        //grow array if necessary
        if(buffersPos>=buffers.length){
            buffers = Arrays.copyOf(buffers, Math.max(buffersPos,  buffers.length*2));
        }


        if(inMemory){
            final int newBufSize = JdbmUtil.nextPowTwo((int) (offset%BUF_SIZE));
            //double size of existing in-memory-buffer
            ByteBuffer newBuf = ByteBuffer.allocate(newBufSize);
            if(buffers[buffersPos]!=null){
                //copy old buffer if it exists
                buffers[buffersPos].rewind();
                newBuf.put(buffers[buffersPos]);
            }
            if(mapMode == FileChannel.MapMode.READ_ONLY)
                newBuf = newBuf.asReadOnlyBuffer();
            buffers[buffersPos] = newBuf;
        }else{
            //just remap file buffer
            long newBufSize =  offset%BUF_SIZE;
            newBufSize = newBufSize + newBufSize%BUF_SIZE_INC; //round to BUF_SIZE_INC
            buffers[buffersPos] = fileChannel.map(mapMode, 1L*buffersPos*BUF_SIZE, newBufSize );
            if(mapMode == FileChannel.MapMode.READ_ONLY)
                buffers[buffersPos] = buffers[buffersPos].asReadOnlyBuffer();

        }
    }

    public void putLong(long offset, long value){
        if(CC.BB_LOG_WRITES && LOG.isLoggable(Level.FINEST))
            LOG.finest(logFileName+":putLong: "+offset+" - "+(value>>>48)+" - "+value);

        if(CC.BB_CHECK_AVAILABLE_SIZE && offset+8>availSizeCheck)
            throw new InternalError();

        ByteBuffer b = internalByteBuffer(offset);
        b.putLong((int) (offset%BUF_SIZE), value);
    }

    public void putUnsignedByte(long offset, byte value){
        if(CC.BB_LOG_WRITES && LOG.isLoggable(Level.FINEST))
            LOG.finest(logFileName+":putUnsignedByte: "+offset+" - "+value);

        if(CC.BB_CHECK_AVAILABLE_SIZE && offset+1>availSizeCheck)
            throw new InternalError();


        ByteBuffer b = internalByteBuffer(offset);
        b.put((int) (offset % BUF_SIZE), value);
    }

    public void putData(long offset, final DataOutput2 value) {
        putData(offset, value.buf, value.pos);
    }

    public void putData(long offset, byte[] value, int size) {
        if(CC.BB_LOG_WRITES && LOG.isLoggable(Level.FINEST))
            LOG.finest(logFileName+":putData: "+offset+" - "+size + " - "+Arrays.toString(Arrays.copyOf(value, size)));

        if(CC.BB_CHECK_AVAILABLE_SIZE && offset+size>availSizeCheck)
            throw new InternalError();

        ByteBuffer b = internalByteBuffer(offset);
        b.position((int) (offset%BUF_SIZE));
        b.put(value,0,size);
    }

    public void putUnsignedShort(long offset, int value) {
        if(CC.BB_LOG_WRITES && LOG.isLoggable(Level.FINEST))
            LOG.finest(logFileName+":putUnsignedShort: "+offset+" - "+value);

        if(CC.BB_CHECK_AVAILABLE_SIZE && offset+2>availSizeCheck)
            throw new InternalError();

        ByteBuffer b = internalByteBuffer(offset);
        b.putShort((int) (offset%BUF_SIZE), (short)value);
    }


    public long getLong(long offset){
        ByteBuffer b = internalByteBuffer(offset);
        return b.getLong((int) (offset%BUF_SIZE));
    }

    public int getUnsignedByte(long offset){
        int pos = (int) (offset%BUF_SIZE);
        ByteBuffer b = internalByteBuffer(offset);
        return b.get(pos) & 0xff;
    }


    public int getUnsignedShort(long offset) throws IOException {
        int pos = (int) (offset%BUF_SIZE);
        ByteBuffer b = internalByteBuffer(offset);
        return (( (b.get(pos++) & 0xff) << 8) |
                ( (b.get(pos) & 0xff)));
    }


    public DataInput2 getDataInput(final long offset, final int size){

        final ByteBuffer b = internalByteBuffer(offset);
        return new DataInput2(b, (int) (offset%BUF_SIZE));
    }


    public void close() throws IOException {
        if(fileChannel!=null){
            fileChannel.close();
            fileChannel = null;
        }
        if(mapMode!= FileChannel.MapMode.READ_ONLY)
            sync();
        for(ByteBuffer b:buffers){
            if(b!=null && (b instanceof MappedByteBuffer)){
                unmap((MappedByteBuffer)b);
            }
        }
        buffers = null;
    }

    public void sync() {
        for(ByteBuffer b:buffers){
            if(b!=null && (b instanceof MappedByteBuffer)){
                ((MappedByteBuffer)b).force();
            }
        }
    }


    /**
     * Hack to unmap MappedByteBuffer.
     * Unmap is necessary on Windows, otherwise file is locked until JVM exits or BB is GCed.
     * There is no public JVM API to unmap buffer, so this tries to use SUN proprietary API for unmap.
     * Any error is silently ignored (for example SUN API does not exist on Android).
     */
    public static final void unmap(MappedByteBuffer b){
        try{
            if(unmapHackSupported){
                sun.misc.Cleaner cleaner = ((sun.nio.ch.DirectBuffer) b).cleaner();
                if(cleaner!=null)
                    cleaner.clean();
            }
        }catch(Exception e){
            JdbmUtil.LOG.log(Level.FINE, "ByteBuffer Unmap failed", e);
        }
    }


    private static boolean unmapHackSupported = false;
    static{
        //TODO check how this works on Android
        try{
            unmapHackSupported =
                    Class.forName("sun.nio.ch.DirectBuffer")!=null;
        }catch(Exception e){
            unmapHackSupported = false;
        }
    }

}
