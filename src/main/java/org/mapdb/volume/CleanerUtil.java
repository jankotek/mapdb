/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.mapdb.volume;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.Objects;

import static java.lang.invoke.MethodHandles.constant;
import static java.lang.invoke.MethodHandles.dropArguments;
import static java.lang.invoke.MethodHandles.filterReturnValue;
import static java.lang.invoke.MethodHandles.guardWithTest;
import static java.lang.invoke.MethodType.methodType;

/**
 * sun.misc.Cleaner has moved in OpenJDK 9 and
 * sun.misc.Unsafe#invokeCleaner(ByteBuffer) is the replacement.
 * This class is a hack to use sun.misc.Cleaner in Java 8 and
 * use the replacement in Java 9+.
 * This implementation is based on Hadoop class
 * https://github.com/apache/hadoop/blob/5d084d7eca32cfa647a78ff6ed3c378659f5b186/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/util/CleanerUtil.java
 * Some adaptations have been done to handle the attachment() of the byte buffer (explained in Bug #776)
 */
public final class CleanerUtil {

    // Prevent instantiation
    private CleanerUtil(){}

    /**
     * <code>true</code>, if this platform supports unmapping mmapped files.
     */
    public static final boolean UNMAP_SUPPORTED;

    /**
     * if {@link #UNMAP_SUPPORTED} is {@code false}, this contains the reason
     * why unmapping is not supported.
     */
    public static final String UNMAP_NOT_SUPPORTED_REASON;

    public static boolean JAVA8_OR_LESS;
    private static final MethodHandle CLEANER;


    static {
        Object hack = unmapHackImpl();
        if (hack instanceof MethodHandle) {
            CLEANER = (MethodHandle) unmapHackImpl();
            UNMAP_SUPPORTED = true;
            UNMAP_NOT_SUPPORTED_REASON = null;
        } else {
            CLEANER = null;
            UNMAP_SUPPORTED = false;
            UNMAP_NOT_SUPPORTED_REASON = hack.toString();
        }
    }

    private static Object unmapHackImpl() {
        final MethodHandles.Lookup lookup = MethodHandles.lookup();
        try {
            try {
                // *** sun.misc.Unsafe unmapping (Java 9+) ***
                final Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
                // first check if Unsafe has the right method, otherwise we can
                // give up without doing any security critical stuff:
                final MethodHandle unmapper = lookup.findVirtual(unsafeClass,
                        "invokeCleaner", methodType(void.class, ByteBuffer.class));
                // fetch the unsafe instance and bind it to the virtual MH:
                final Field f = unsafeClass.getDeclaredField("theUnsafe");
                f.setAccessible(true);
                final Object theUnsafe = f.get(null);
                return unmapper.bindTo(theUnsafe);
            } catch (SecurityException se) {
                // rethrow to report errors correctly (we need to catch it here,
                // as we also catch RuntimeException below!):
                throw se;
            } catch (ReflectiveOperationException | RuntimeException e) {
                // *** sun.misc.Cleaner unmapping (Java 8) ***
                final Class<?> directBufferClass =
                        Class.forName("java.nio.DirectByteBuffer");

                final Method m = directBufferClass.getMethod("cleaner");
                m.setAccessible(true);
                final MethodHandle directBufferCleanerMethod = lookup.unreflect(m);
                final Class<?> cleanerClass =
                        directBufferCleanerMethod.type().returnType();

                /*
                 * "Compile" a MethodHandle that basically is equivalent
                 * to the following code:
                 *
                 * void unmapper(ByteBuffer byteBuffer) {
                 *   sun.misc.Cleaner cleaner =
                 *       ((java.nio.DirectByteBuffer) byteBuffer).cleaner();
                 *   if (Objects.nonNull(cleaner)) {
                 *     cleaner.clean();
                 *   } else {
                 *     // the noop is needed because MethodHandles#guardWithTest
                 *     // always needs ELSE
                 *     noop(cleaner);
                 *   }
                 * }
                 */
                final MethodHandle cleanMethod = lookup.findVirtual(
                        cleanerClass, "clean", methodType(void.class));
                final MethodHandle nonNullTest = lookup.findStatic(Objects.class,
                        "nonNull", methodType(boolean.class, Object.class))
                        .asType(methodType(boolean.class, cleanerClass));
                final MethodHandle noop = dropArguments(
                        constant(Void.class, null).asType(methodType(void.class)),
                        0, cleanerClass);
                final MethodHandle unmapper = filterReturnValue(
                        directBufferCleanerMethod,
                        guardWithTest(nonNullTest, cleanMethod, noop))
                        .asType(methodType(void.class, ByteBuffer.class));
                JAVA8_OR_LESS = true;
                return unmapper;
            }
        } catch (SecurityException se) {
            return "Unmapping is not supported, because not all required " +
                    "permissions are given to the MapDB JAR file: " + se +
                    " [Please grant at least the following permissions: " +
                    "RuntimePermission(\"accessClassInPackage.sun.misc\") " +
                    " and ReflectPermission(\"suppressAccessChecks\")]";
        } catch (ReflectiveOperationException | RuntimeException e) {
            return "Unmapping is not supported on this platform, " +
                    "because internal Java APIs are not compatible with " +
                    "this Hadoop version: " + e;
        }
    }

    public static boolean freeBuffer (ByteBuffer buffer) throws IOException {
            if (!buffer.isDirect() && UNMAP_SUPPORTED) {
                throw new IllegalArgumentException(
                        "unmapping only works with direct buffers");
            }

            try {
                CLEANER.invokeExact(buffer);
                /*
                * Handling attachment of the ByteBuffer object
                * Refer to https://github.com/jankotek/mapdb/issues/776 for details
                * */
                if (JAVA8_OR_LESS) {
                    final MethodHandles.Lookup lookup = MethodHandles.lookup();
                    final Class<?> directBufferClass =
                            Class.forName("java.nio.DirectByteBuffer");
                    Object bb = directBufferClass.cast(buffer);
                    final Method m = directBufferClass.getMethod("attachment");
                    m.setAccessible(true);
                    final MethodHandle directBufferAttachmentMethod = lookup.unreflect(m);
                    Object attachment = (Object) directBufferAttachmentMethod.invoke(bb);
                    if(attachment != null)
                        return freeBuffer((MappedByteBuffer) attachment);
                }
                return true;
            } catch (Throwable e) {
                throw new IOException(e);
            }

    }
}