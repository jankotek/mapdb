package org.mapdb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.zip.CRC32;

/**
 * Low level record store.
 */
public abstract class Store implements Engine{

    protected final boolean checksum;
    protected final boolean compress;
    protected final boolean encrypt;
    protected final byte[] password;
    protected final EncryptionXTEA encryptionXTEA;

    protected final ThreadLocal<CompressLZF> LZF;

    protected Store(boolean checksum, boolean compress, byte[] password) {
        this.checksum = checksum;
        this.compress = compress;
        this.encrypt =  password!=null;
        this.password = password;
        this.encryptionXTEA = !encrypt?null:new EncryptionXTEA(password);

        this.LZF = !compress?null:new ThreadLocal<CompressLZF>() {
            @Override
            protected CompressLZF initialValue() {
                return new CompressLZF();
            }
        };
    }

    public abstract long getMaxRecid();
    public abstract ByteBuffer getRaw(long recid);
    public abstract Iterator<Long> getFreeRecids();
    public abstract void updateRaw(long recid, ByteBuffer data);

    /** returns maximal store size or `0` if there is no limit */
    public abstract long getSizeLimit();

    /** returns current size occupied by physical store (does not include index). It means file allocated by physical file */
    public abstract long getCurrSize();

    /** returns free size in  physical store (does not include index). */
    public abstract long getFreeSize();

    /** get some statistics about store. This may require traversing entire store, so it can take some time.*/
    public abstract String calculateStatistics();

    public void printStatistics(){
        System.out.println(calculateStatistics());
    }


    protected final Queue<DataOutput2> recycledDataOuts = new ArrayBlockingQueue<DataOutput2>(128);


    protected <A> DataOutput2 serialize(A value, Serializer<A> serializer){
        try {
            DataOutput2 out = newDataOut2();

            serializer.serialize(out,value);

            if(out.pos>0){

                if(compress){
                    DataOutput2 tmp = newDataOut2();

                    //compress data into `tmp`
                    CompressLZF lzf = LZF.get();
                    tmp.ensureAvail(out.pos + 40);
                    int len;
                    try{
                        len = lzf.compress(out.buf, out.pos, tmp.buf, 0);
                    }catch (ArrayIndexOutOfBoundsException e){
                        len=0; //compressed data are larger than source
                    }
                    //data are in `tmp`, reset `out` and copy compressed data there
                    //check if compressed data are larger then original
                    if (len == 0 || out.pos <= len) {
                        //in this case do not compress data, write 0 at begging and move data by one1
                        out.ensureAvail(1);
                        System.arraycopy(out.buf,0,out.buf,1,out.pos);
                        out.buf[0] = 0;
                        out.pos+=1;
                    } else {
                        //data are compressed into `tmp`. Now write header into `out` and copy data
                        out.pos = 0;
                        Utils.packInt(out,len); //write original decompressed size
                        out.ensureAvail(len);
                        System.arraycopy(tmp.buf,0,out.buf,out.pos,len);
                        out.pos+=len;

                    }
                    recycledDataOuts.offer(tmp);
                }


                if(encrypt){
                    int size = out.pos;
                    //round size to 16
                    if(size%EncryptionXTEA.ALIGN!=0)
                        size += EncryptionXTEA.ALIGN - size%EncryptionXTEA.ALIGN;
                    final int sizeDif=size-out.pos;
                    //encrypt
                    out.ensureAvail(sizeDif+1);
                    encryptionXTEA.encrypt(out.buf,0,size);
                    //and write diff from 16
                    out.pos = size;
                    out.writeByte(sizeDif);
                }

                if(checksum){
                    CRC32 crc = new CRC32();
                    crc.update(out.buf,0,out.pos);
                    out.writeInt((int)crc.getValue());
                }
            }

            return out;
        } catch (IOException e) {
            throw new IOError(e);
        }

    }

    protected DataOutput2 newDataOut2() {
        DataOutput2 tmp = recycledDataOuts.poll();
        if(tmp==null) tmp = new DataOutput2();
        else tmp.pos=0;
        return tmp;
    }


    protected <A> A deserialize(Serializer<A> serializer, int size, DataInput2 di) throws IOException {
        if(size>0){
            if(checksum){
                //last two digits is checksum
                size -= 4;

                //read data into tmp buffer
                DataOutput2 tmp = newDataOut2();
                tmp.ensureAvail(size);
                int oldPos = di.pos;
                di.read(tmp.buf,0,size);
                di.pos = oldPos;
                //calculate checksums
                CRC32 crc32 = new CRC32();
                crc32.update(tmp.buf,0,size);
                recycledDataOuts.offer(tmp);
                int check = (int) crc32.getValue();
                int checkExpected = di.buf.getInt(di.pos+size);
                if(check!=checkExpected)
                    throw new IOException("CRC32 does not match, data broken");
            }

            if(encrypt){
                DataOutput2 tmp = newDataOut2();
                size-=1;
                tmp.ensureAvail(size);
                di.read(tmp.buf,0,size);
                encryptionXTEA.decrypt(tmp.buf, 0, size);
                int cut = di.readUnsignedByte(); //length dif from 16bytes
                di = new DataInput2(tmp.buf);
                size -= cut;
            }

            if(compress) {
                int compressLen = Utils.unpackInt(di);
                if(compressLen>0){
                    DataOutput2 tmp = newDataOut2();

                    tmp.ensureAvail(compressLen);
                    CompressLZF lzf = LZF.get();
                    lzf.expand(di.buf,di.pos,size-di.pos,tmp.buf,0,compressLen);

                    //data are now stored in `tmp`, turn it into DataInput
                    di = new DataInput2(tmp.buf);
                    size = compressLen;
                }else{
                    //in this case data are not compressed, so do nothing except updating sizes
                    size-=1;
                }
            }

        }

        int start = di.pos;

        A ret = serializer.deserialize(di,size);
        if(size+start>di.pos)throw new InternalError("data were not fully read, check your serializer ");
        if(size+start<di.pos)throw new InternalError("data were read beyond record size, check your serializer");
        return ret;
    }



}
