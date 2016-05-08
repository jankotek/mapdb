package org.mapdb.volume

/**
 * Wraps volume factory and returns volume as readonly
 */
class ReadOnlyVolumeFactory(protected val volfab:VolumeFactory): VolumeFactory() {

    override fun exists(file: String?): Boolean {
        return volfab.exists(file)
    }

    override fun makeVolume(file: String?, readOnly: Boolean, fileLockWait:Long, sliceShift: Int, initSize: Long, fixedSize: Boolean): Volume? {
        val volume = volfab.makeVolume(file, readOnly, fileLockWait, sliceShift, initSize, fixedSize)
        return ReadOnlyVolume(volume)
    }

    override fun handlesReadonly(): Boolean {
        return true
    }

}
