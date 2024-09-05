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
package org.apache.jackrabbit.oak.segment.azure;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.specialized.BlockBlobClient;
import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.remote.AbstractRemoteSegmentArchiveReader;
import org.apache.jackrabbit.oak.segment.remote.RemoteSegmentArchiveEntry;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.jackrabbit.oak.segment.azure.AzureUtilities.readBufferFully;

public class AzureSegmentArchiveReader extends AbstractRemoteSegmentArchiveReader {

    private final BlobContainerClient blobContainerClient;

    private final String archiveName;

    private final String rootPrefix;

    private final long length;

    AzureSegmentArchiveReader(BlobContainerClient blobContainerClient, String rootPrefix, String archiveName, IOMonitor ioMonitor) throws IOException {
        super(ioMonitor);
        this.blobContainerClient = blobContainerClient;
        this.rootPrefix = rootPrefix;
        this.archiveName = archiveName;
        this.length = computeArchiveIndexAndLength();
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public String getName() {
        return archiveName;
    }

    @Override
    protected long computeArchiveIndexAndLength() throws IOException {
        AtomicLong length = new AtomicLong();

        ListBlobsOptions listOptions = new ListBlobsOptions();
        listOptions.setPrefix(rootPrefix + "/" + archiveName + "/");

        blobContainerClient.listBlobs(listOptions, null).stream().forEach(
                blobItem -> {
                    Map<String, String> metadata = blobItem.getMetadata();
                    if (AzureBlobMetadata.isSegment(metadata)) {
                        RemoteSegmentArchiveEntry indexEntry = AzureBlobMetadata.toIndexEntry(metadata, blobItem.getProperties().getContentLength().intValue());
                        index.put(new UUID(indexEntry.getMsb(), indexEntry.getLsb()), indexEntry);
                    }
                    length.addAndGet(blobItem.getProperties().getContentLength());
                }
        );

        return length.get();
    }

    @Override
    protected void doReadSegmentToBuffer(String segmentFileName, Buffer buffer) throws IOException {
        readBufferFully(getBlob(segmentFileName), buffer);
    }

    @Override
    protected Buffer doReadDataFile(String extension) throws IOException {
        return readBlob(getName() + extension);
    }

    @Override
    protected File archivePathAsFile() {
        return new File(rootPrefix + "/" + archiveName);
    }

    private BlockBlobClient getBlob(String name) throws IOException {
        try {
            return blobContainerClient.getBlobClient(rootPrefix + "/" + archiveName + "/" + name).getBlockBlobClient();
        } catch (BlobStorageException e) {
            throw new IOException(e);
        }
    }

    private Buffer readBlob(String name) throws IOException {
        try {
            BlockBlobClient blob = getBlob(name);
            if (!blob.exists()) {
                return null;
            }
            long length = blob.getProperties().getBlobSize();
            Buffer buffer = Buffer.allocate((int) length);
            AzureUtilities.readBufferFully(blob, buffer);
            return buffer;
        } catch (BlobStorageException e) {
            throw new IOException(e);
        }
    }
}
