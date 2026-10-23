package org.mapdb.volume;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ReadOnlyBufferException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

@RunWith(Parameterized.class)
public class MappedFileVolSingleTest {

    @Parameterized.Parameters(name = "readOnly={0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] {{false}, {true}});
    }

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private final boolean readOnly;

    public MappedFileVolSingleTest(boolean readOnly) {
        this.readOnly = readOnly;
    }

    @Test
    public void closeUnmapsBuffer() throws Exception {
        assumeTrue(CleanerUtil.UNMAP_SUPPORTED);
        File file = temporaryFolder.newFile();
        try (RandomAccessFile writer = new RandomAccessFile(file, "rw")) {
            writer.writeLong(42L);
        }

        final Thread testThread = Thread.currentThread();
        final List<String> warnings = new ArrayList<>();
        Handler handler = new Handler() {
            @Override
            public void publish(LogRecord record) {
                if (Thread.currentThread() == testThread
                        && record.getLevel().intValue() >= Level.WARNING.intValue()) {
                    warnings.add(record.getMessage());
                }
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        Volume.LOG.addHandler(handler);
        try {
            MappedFileVolSingle volume = new MappedFileVolSingle(file, readOnly, 0L, 8L, true);
            try {
                assertEquals(42L, volume.getLong(0));
                if (readOnly) {
                    try {
                        volume.putLong(0, 99L);
                        fail("Read-only mapping must reject writes");
                    } catch (ReadOnlyBufferException expected) {
                        // Removing an unnecessary view must preserve read-only protection.
                    }
                } else {
                    volume.putLong(0, 99L);
                    assertEquals(99L, volume.getLong(0));
                }
            } finally {
                volume.close();
            }
            assertTrue("Unmapping warnings: " + warnings, warnings.isEmpty());
            assertTrue("Closed mapping must not keep the file locked", file.delete());
        } finally {
            Volume.LOG.removeHandler(handler);
        }
    }
}
