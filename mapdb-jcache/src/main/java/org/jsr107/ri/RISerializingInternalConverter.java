/**
 *  Copyright 2011-2013 Terracotta, Inc.
 *  Copyright 2011-2013 Oracle America Incorporated
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.jsr107.ri;

import javax.cache.CacheException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.lang.ref.WeakReference;
import java.util.Arrays;

/**
 * An {@link RIInternalConverter} that converts values to and from their
 * serialized representation.
 *
 * @param <T> the type of value to serialize
 * @author Brian Oliver
 */
class RISerializingInternalConverter<T> implements RIInternalConverter<T> {

  /**
   * The {@link ClassLoader} to use for locating classes to serialize/deserialize.
   * <p>
   * This is a WeakReference to prevent ClassLoader memory leaks.
   * </p>
   */
  private WeakReference<ClassLoader> classLoaderReference;

  /**
   * Constructs a {@link RISerializingInternalConverter}.
   *
   * @param classLoader the {@link ClassLoader} to use for locating classes
   *                    when deserializing
   */
  public RISerializingInternalConverter(ClassLoader classLoader) {

    this.classLoaderReference = new WeakReference<ClassLoader>(classLoader);
  }

  /**
   * Gets the {@link ClassLoader} that will be used to locate classes
   * during serialization and deserialization.
   *
   * @return the {@link ClassLoader}
   */
  public ClassLoader getClassLoader() {
    return classLoaderReference.get();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Object toInternal(T value) {
    return new Serialized<T>(value);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public T fromInternal(Object internal) {
    if (internal == null) {
      return null;
    } else if (internal instanceof Serialized) {
      return (T) ((Serialized) internal).deserialize(getClassLoader());
    } else {
      throw new IllegalArgumentException("internal value is not a Serialized instance [" + internal + "]");
    }
  }

  /**
   * A container for a serialized object.
   *
   * @param <V> the type of value that was serialized
   */
  private static class Serialized<V> {

    /**
     * The serialized form of the value.
     */
    private final byte[] bytes;

    /**
     * The hashcode of the value.
     */
    private final int hashCode;

    /**
     * Constructs a {@link Serialized} representation of a value.
     *
     * @param value the value to be serialized (in a serialized form)
     */
    Serialized(V value) {
      if (value == null) {
        this.hashCode = 0;
        this.bytes = null;
      } else {
        this.hashCode = value.hashCode();

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
          ObjectOutputStream oos = new ObjectOutputStream(bos);
          oos.writeObject(value);
          bos.flush();
          this.bytes = bos.toByteArray();
        } catch (IOException e) {
          throw new IllegalArgumentException("Failed to serialize: " + value + " due to " + e.getMessage(), e);
        } finally {
          try {
            bos.close();
          } catch (IOException e) {
            // eat this up
          }
        }
      }
    }

    /**
     * Deserialize the {@link Serialized} value.
     *
     * @param classLoader the {@link ClassLoader} to use for resolving classes
     */
    public V deserialize(ClassLoader classLoader) {
      ByteArrayInputStream bos = new ByteArrayInputStream(bytes);
      ObjectInputStream ois;
      try {
        ois = new CustomizedClassLoaderObjectInputStream(bos, classLoader);

        //this must fail if the types are incompatible
        return (V) ois.readObject();
      } catch (IOException e) {
        throw new CacheException("Failed to deserialize: " + e.getMessage(), e);
      } catch (ClassNotFoundException e) {
        throw new CacheException("Failed to resolve a deserialized class: " + e.getMessage(), e);
      } finally {
        try {
          bos.close();
        } catch (IOException e) {
          // eat this up
        }
      }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object object) {
      if (this == object) {
        return true;
      }
      if (object == null) {
        return false;
      }
      if (!(object instanceof Serialized)) {
        return false;
      }
      Serialized<?> serialized = (Serialized<?>) object;
      if (!Arrays.equals(bytes, serialized.bytes)) {
        return false;
      }
      if (hashCode != serialized.hashCode) {
        return false;
      }
      return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
      return hashCode;
    }
  }

  /**
   * An {@link ObjectInputStream} that uses a specific {@link ClassLoader}.
   */
  private static final class CustomizedClassLoaderObjectInputStream extends ObjectInputStream {

    /**
     * The {@link ClassLoader} to use.
     */
    private final ClassLoader classloader;

    /**
     * Constructs a {@link CustomizedClassLoaderObjectInputStream}.
     *
     * @param in          the {@link InputStream}
     * @param classloader the {@link ClassLoader}
     * @throws IOException should the stream not be created
     */
    private CustomizedClassLoaderObjectInputStream(InputStream in, ClassLoader classloader) throws IOException {
      super(in);
      this.classloader = classloader;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
      String name = desc.getName();
      try {
        return Class.forName(name, false, classloader);
      } catch (ClassNotFoundException ex) {
        return super.resolveClass(desc);
      }
    }
  }
}
