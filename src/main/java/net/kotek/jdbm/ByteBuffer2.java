package net.kotek.jdbm;

import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

/**
 * JDBM abstraction over ByteBuffer.
 * <p/>
 * ByteBuffer has maximal size 2GB, so this class chains multiple ByteBuffers together.
 * IO operations must be padded not to cross ByteBuffer boundaries.
 * <p/>
 * Also ByteBuffer is not grow-able, this solves it.
 */
public final class ByteBuffer2 {

    static final int  BUF_SIZE = 1<<30;

    static final int BUF_SIZE_INC = 1024*1024;

    static final int INITIAL_SIZE = 1024*32;

    /** file channel backing this ByteBuffer, null for in-memory-store */
    protected FileChannel fileChannel;
    final protected FileChannel.MapMode mapMode;
    protected ByteBuffer[] buffers;
    final protected boolean inMemory;


    public ByteBuffer2(boolean inMemory, FileChannel fileChannel, FileChannel.MapMode mapMode){
        try{
        this.fileChannel = fileChannel;
        this.inMemory = inMemory;
        this.mapMode = mapMode;
        if(inMemory){
            buffers = new ByteBuffer[]{ByteBuffer.allocate(INITIAL_SIZE)};
        }else{
            final long fileSize = fileChannel.size();
            if(fileSize>0){
                //map existing data
                buffers = new ByteBuffer[(int) (1+fileSize/BUF_SIZE)];
                for(int i=0;i<=fileSize/BUF_SIZE;i++){
                    final long offset = 1L*BUF_SIZE*i;
                    buffers[i] = fileChannel.map(mapMode, offset, Math.min(BUF_SIZE, fileSize-offset));
                    //TODO what if 'fileSize % 8 != 0'?
                }
            }else{
                buffers = new ByteBuffer[1];
                buffers[0] = fileChannel.map(mapMode, 0, INITIAL_SIZE);
            }
        }
        }catch(IOException e){
            throw new IOError(e);
        }
    }


    protected ByteBuffer getByteBuffer(long offset) {
        return buffers[((int) (offset / BUF_SIZE))];
    }

    public void ensureAvailable(long offset) throws IOException {
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
            buffers[buffersPos] = newBuf;
        }else{
            //just remap file buffer
            long newBufSize =  offset%BUF_SIZE;
            newBufSize = newBufSize + newBufSize%BUF_SIZE_INC; //round to BUF_SIZE_INC
            buffers[buffersPos] = fileChannel.map(mapMode, 1L*buffersPos*BUF_SIZE, newBufSize );
        }
    }

    public void putLong(long offset, long value){
        ByteBuffer b = getByteBuffer(offset);
        b.putLong((int) (offset%BUF_SIZE), value);
    }

    public void putUnsignedByte(long offset, byte value){
        ByteBuffer b = getByteBuffer(offset);
        b.put((int) (offset % BUF_SIZE), value);
    }

    public void putData(long offset, final DataOutput2 value) {
        putData(offset, value.buf, value.pos);
    }

    public void putData(long offset, byte[] value, int size) {
        ByteBuffer b = getByteBuffer(offset);
        b.position((int) (offset%BUF_SIZE));
        b.put(value,0,size);
    }

    public void putUnsignedShort(long offset, int value) {
        ByteBuffer b = getByteBuffer(offset);
        b.putShort((int) (offset%BUF_SIZE), (short)value);
    }


    public long getLong(long offset){
        ByteBuffer b = getByteBuffer(offset);
        return b.getLong((int) (offset%BUF_SIZE));
    }

    public int getUnsignedByte(long offset){
        int pos = (int) (offset%BUF_SIZE);
        ByteBuffer b = getByteBuffer(offset);
        return b.get(pos) & 0xff;
    }


    public int getUnsignedShort(long offset) throws IOException {
        int pos = (int) (offset%BUF_SIZE);
        ByteBuffer b = getByteBuffer(offset);
        return (( (b.get(pos++) & 0xff) << 8) |
                ( (b.get(pos) & 0xff)));
    }


    public DataInput2 getDataInput(final long offset, final int size){

        final ByteBuffer b = getByteBuffer(offset);
        return new DataInput2(b, (int) (offset%BUF_SIZE));
    }


    public void close() throws IOException {
        if(fileChannel!=null){
            fileChannel.close();
            fileChannel = null;
        }
        buffers = null;
    }

}
