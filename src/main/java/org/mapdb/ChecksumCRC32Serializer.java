package org.mapdb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.zip.CRC32;

/**
 * Adds CRC32 checksum at end of each record to check data integrity.
 * It throws 'IOException("CRC32 does not match, data broken")' on de-serialization if data are corrupted
 */
public class ChecksumCRC32Serializer implements Serializer<byte[]> {
    @Override
    public void serialize(DataOutput out, byte[] value) throws IOException {
        CRC32 crc = new CRC32();
        crc.update(value);
        out.write(value);
        out.writeInt((int) crc.getValue());
    }

    @Override
    public byte[] deserialize(DataInput in, int available) throws IOException {
        byte[] value = new byte[available-4];
        in.readFully(value);
        CRC32 crc = new CRC32();
        crc.update(value);
        int checksum = in.readInt();
        if(checksum!=(int)crc.getValue()){
            throw new IOException("CRC32 does not match, data broken");
        }
        return value;
    }
}
