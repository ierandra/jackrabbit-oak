/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jackrabbit.oak.segment.file.tar.binaries;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.zip.CRC32;


import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.util.ReaderAtEnd;

class BinaryReferencesIndexLoaderV1 {

    static final int MAGIC = ('\n' << 24) + ('0' << 16) + ('B' << 8) + '\n';

    static final int FOOTER_SIZE = 16;

    static Buffer loadBinaryReferencesIndex(ReaderAtEnd reader) throws IOException, InvalidBinaryReferencesIndexException {
        Buffer meta = reader.readAtEnd(FOOTER_SIZE, FOOTER_SIZE);

        int crc32 = meta.getInt();
        int count = meta.getInt();
        int size = meta.getInt();
        int magic = meta.getInt();

        if (magic != MAGIC) {
            throw new InvalidBinaryReferencesIndexException("Invalid magic number");
        }
        if (count < 0) {
            throw new InvalidBinaryReferencesIndexException("Invalid count");
        }
        if (size < count * 22 + 16) {
            throw new InvalidBinaryReferencesIndexException("Invalid size");
        }

        return reader.readAtEnd(size, size);
    }

    public static BinaryReferencesIndex parseBinaryReferencesIndex(Buffer buffer) throws InvalidBinaryReferencesIndexException {
        Buffer data = buffer.slice();
        data.limit(data.limit() - FOOTER_SIZE);

        buffer.position(buffer.limit() - FOOTER_SIZE);
        Buffer meta = buffer.slice();

        int crc32 = meta.getInt();
        int count = meta.getInt();
        int size = meta.getInt();
        int magic = meta.getInt();

        if (magic != MAGIC) {
            throw new InvalidBinaryReferencesIndexException("Invalid magic number");
        }
        if (count < 0) {
            throw new InvalidBinaryReferencesIndexException("Invalid count");
        }
        if (size < count * 22 + 16) {
            throw new InvalidBinaryReferencesIndexException("Invalid size");
        }

        CRC32 checksum = new CRC32();
        data.mark();
        data.update(checksum);
        data.reset();

        if ((int) (checksum.getValue()) != crc32) {
            throw new InvalidBinaryReferencesIndexException("Invalid checksum");
        }

        return new BinaryReferencesIndex(parseBinaryReferencesIndex(count, data));
    }

    private static Map<Generation, Map<UUID, Set<String>>> parseBinaryReferencesIndex(int count, Buffer buffer) {
        Map<Generation, Map<UUID, Set<String>>> result = new HashMap<>(count);
        for (int i = 0; i < count; i++) {
            Generation k = parseGeneration(buffer);
            Map<UUID, Set<String>> v = parseEntriesBySegment(buffer);
            result.put(k, v);
        }
        return result;
    }

    private static Generation parseGeneration(Buffer buffer) {
        int generation = buffer.getInt();
        return new Generation(generation, generation, true);
    }

    private static Map<UUID, Set<String>> parseEntriesBySegment(Buffer buffer) {
        return parseEntriesBySegment(buffer.getInt(), buffer);
    }

    private static Map<UUID, Set<String>> parseEntriesBySegment(int count, Buffer buffer) {
        Map<UUID, Set<String>> result = new HashMap<>(count);
        for (int i = 0; i < count; i++) {
            UUID k = parseUUID(buffer);
            Set<String> v = parseEntries(buffer);
            result.put(k, v);
        }
        return result;
    }

    private static UUID parseUUID(Buffer buffer) {
        long msb = buffer.getLong();
        long lsb = buffer.getLong();
        return new UUID(msb, lsb);
    }

    private static Set<String> parseEntries(Buffer buffer) {
        return parseEntries(buffer.getInt(), buffer);
    }

    private static Set<String> parseEntries(int count, Buffer buffer) {
        Set<String> entries = new HashSet<>(count);
        for (int i = 0; i < count; i++) {
            entries.add(parseString(buffer));
        }
        return entries;
    }

    private static String parseString(Buffer buffer) {
        return parseString(buffer.getInt(), buffer);
    }

    private static String parseString(int length, Buffer buffer) {
        byte[] data = new byte[length];
        buffer.get(data);
        return new String(data, StandardCharsets.UTF_8);
    }
}
