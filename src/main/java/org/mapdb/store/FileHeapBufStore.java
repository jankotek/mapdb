package org.mapdb.store;

import org.mapdb.util.IO;

import java.io.*;

public class FileHeapBufStore extends HeapBufStore{

    protected final File file;

    //-newRWLOCK

    public FileHeapBufStore(File file) {
        this.file = file;

        //-WLOCK
        reload();
        //-WUNLOCK
    }

    protected void reload() {
        //-AWLOCKED
        clear();

        try(DataInputStream is = new DataInputStream(new FileInputStream(file))) {
            long maxRecid = 0;

            //load records
            long recCount = IO.readLong(is);
            for(long i=0;i<recCount;i++){
                long recid = IO.readLong(is);
                int size = IO.readInt(is);
                byte[] b = size==-1?
                        PREALLOC_RECORD :
                        IO.readByteArray(is, size);
                records.put(recid, b);
                maxRecid = Math.max(maxRecid, recid);
            }

            //restore free recids
            for(long recid=0;recid<maxRecid;recid++){
                if(!records.containsKey(recid)){
                    freeRecids.add(recid);
                }
            }

        } catch (FileNotFoundException e) {
            return;
        } catch (IOException e) {
            clear();
            throw new IOError(e);
        }
    }

    protected void save() {
        //-ARLOCKED
        try( DataOutputStream out = new DataOutputStream(new FileOutputStream(file))) {
            IO.writeLong(out, records.size());

            records.forEachKeyValue((recid, buf) ->{
                try {
                    IO.writeLong(out, recid);
                    int size = buf==PREALLOC_RECORD? -1 : buf.length;
                    IO.writeInt(out, size);
                    if(size>0)
                        IO.writeByteArray(out, buf);
                }catch(IOException e){
                    throw new IOError(e);
                }
            });

            out.flush();
        } catch (FileNotFoundException e) {
            throw new IOError(e); //file could not be created
        } catch (IOException e) {
            throw new IOError(e);
        }
    }
    public void close() {
        //-WLOCK
        save();
        //-WUNLOCK
    }

    private void clear() {
        //-AWLOCK
        freeRecids.clear();
        freeRecids.trimToSize();
        records.clear();
        records.compact();
    }
}
