/* Copyright 2014 The Johns Hopkins University Applied Physics Laboratory
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.jhuapl.tinkerpop;

import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

public enum EntryLocation {

  Row, ColF, ColQ, Value;

  public String extract(Entry<Key,Value> entry) {
    switch (this) {
      case Row:
        return entry.getKey().getRow().toString();
      case ColF:
        return entry.getKey().getColumnFamily().toString();
      case ColQ:
        return entry.getKey().getColumnQualifier().toString();
      case Value:
        return new String(entry.getValue().get());
      default:
        throw new AccumuloGraphException("Unexpected type: " + this);
    }
  }
}
