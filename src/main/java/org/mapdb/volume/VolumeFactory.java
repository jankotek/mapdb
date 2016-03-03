package org.mapdb.volume;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.mapdb.CC;

/**
 * Created by jan on 2/29/16.
 */
public abstract class VolumeFactory {
    public abstract Volume makeVolume(String file, boolean readOnly, boolean fileLockDisabled,
                                      int sliceShift, long initSize, boolean fixedSize);

    public Volume makeVolume(String file, boolean readOnly) {
        return makeVolume(file, readOnly, false);
    }


    public Volume makeVolume(String file, boolean readOnly, boolean fileLockDisable) {
        return makeVolume(file, readOnly, fileLockDisable, CC.PAGE_SHIFT, 0, false);
    }

    @NotNull
    abstract public boolean exists(@Nullable String file);

    @NotNull
    public static VolumeFactory wrap(@NotNull final Volume volume, final boolean exists) {
        return new VolumeFactory() {
            @Override
            public Volume makeVolume(String file, boolean readOnly, boolean fileLockDisabled, int sliceShift, long initSize, boolean fixedSize) {
                return volume;
            }

            @NotNull
            @Override
            public boolean exists(@Nullable String file) {
                return exists;
            }
        };
    }


}
