package org.mapdb.volume;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.mapdb.CC;

/**
 * Created by jan on 2/29/16.
 */
public abstract class VolumeFactory {
    public abstract Volume makeVolume(String file, boolean readOnly, long fileLockWait,
                                      int sliceShift, long initSize, boolean fixedSize);

    public Volume makeVolume(String file, boolean readOnly) {
        return makeVolume(file, readOnly, 0L);
    }


    public Volume makeVolume(String file, boolean readOnly,  long fileLockWait) {
        return makeVolume(file, readOnly, fileLockWait, CC.PAGE_SHIFT, 0, false);
    }

    @NotNull
    abstract public boolean exists(@Nullable String file);

    @NotNull
    public static VolumeFactory wrap(@NotNull final Volume volume, final boolean exists) {
        return new VolumeFactory() {
            @Override
            public Volume makeVolume(String file, boolean readOnly, long fileLockWait, int sliceShift, long initSize, boolean fixedSize) {
                return volume;
            }

            @NotNull
            @Override
            public boolean exists(@Nullable String file) {
                return exists;
            }

            @Override
            public boolean handlesReadonly() {
                return false;
            }
        };
    }


    public abstract boolean handlesReadonly();
}
