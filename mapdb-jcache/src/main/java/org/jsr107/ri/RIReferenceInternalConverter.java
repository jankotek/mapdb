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

/**
 * An {@link RIInternalConverter} that simply returns a reference to the
 * provided value.
 *
 * @param <T> the type of values to convert
 * @author Brian Oliver
 */
public class RIReferenceInternalConverter<T> implements RIInternalConverter<T> {

  /**
   * {@inheritDoc}
   */
  @Override
  public T fromInternal(Object internal) {
    return (T) internal;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Object toInternal(T value) {
    return value;
  }
}
