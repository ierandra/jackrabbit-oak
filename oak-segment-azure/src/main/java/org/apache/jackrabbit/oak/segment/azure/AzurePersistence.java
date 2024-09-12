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
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.specialized.AppendBlobClient;
import com.azure.storage.blob.specialized.BlockBlobClient;
import org.apache.jackrabbit.oak.segment.remote.WriteAccessController;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.RemoteStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.persistence.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class AzurePersistence implements SegmentNodeStorePersistence {
    private static final Logger log = LoggerFactory.getLogger(AzurePersistence.class);

    protected final BlobContainerClient blobContainerClient;

    protected final String rootPrefix;

    protected WriteAccessController writeAccessController = new WriteAccessController();

    public AzurePersistence(BlobContainerClient blobContainerClient, String rootPrefix) {
        this.blobContainerClient = blobContainerClient;
        this.rootPrefix = rootPrefix;

        //TODO: ierandra
        //AzureRequestOptions.applyDefaultRequestOptions(segmentStoreDirectory.getServiceClient().getDefaultRequestOptions());
    }

    @Override
    public SegmentArchiveManager createArchiveManager(boolean mmap, boolean offHeapAccess, IOMonitor ioMonitor, FileStoreMonitor fileStoreMonitor, RemoteStoreMonitor remoteStoreMonitor) {
        attachRemoteStoreMonitor(remoteStoreMonitor);
        return new AzureArchiveManager(blobContainerClient, rootPrefix, ioMonitor, fileStoreMonitor, writeAccessController);
    }

    @Override
    public boolean segmentFilesExist() {
        try {
            ListBlobsOptions listOptions = new ListBlobsOptions();
            listOptions.setPrefix(rootPrefix + "/");
            return blobContainerClient.listBlobs(listOptions, null).stream()
                    .filter(BlobItem::isPrefix)
                    .anyMatch(blobItem -> blobItem.getName().endsWith(".tar") || blobItem.getName().endsWith(".tar/"));
        } catch (BlobStorageException e) {
            log.error("Can't check if the segment archives exists", e);
            return false;
        }
    }

    @Override
    public JournalFile getJournalFile() {
        return new AzureJournalFile(blobContainerClient, rootPrefix + "/journal.log", writeAccessController);
    }

    @Override
    public GCJournalFile getGCJournalFile() throws IOException {
        return new AzureGCJournalFile(getAppendBlob("gc.log"));
    }

    @Override
    public ManifestFile getManifestFile() throws IOException {
        return new AzureManifestFile(getBlockBlob("manifest"));
    }

    @Override
    public RepositoryLock lockRepository() throws IOException {
        return new AzureRepositoryLock(getBlockBlob("repo.lock"), () -> {
            log.warn("Lost connection to the Azure. The client will be closed.");
            // TODO close the connection
        }, writeAccessController).lock();
    }

    private BlockBlobClient getBlockBlob(String path) throws IOException {
        try {
            return blobContainerClient.getBlobClient(rootPrefix + "/" + path).getBlockBlobClient();
        } catch (BlobStorageException e) {
            throw new IOException(e);
        }
    }

    private AppendBlobClient getAppendBlob(String path) throws IOException {
        try {
            return blobContainerClient.getBlobClient(rootPrefix + "/" + path).getAppendBlobClient();
        } catch (BlobStorageException e) {
            throw new IOException(e);
        }
    }

    private static void attachRemoteStoreMonitor(RemoteStoreMonitor remoteStoreMonitor) {

        //TODO: ierandra
        /*OperationContext.getGlobalRequestCompletedEventHandler().addListener(new StorageEvent<RequestCompletedEvent>() {

            @Override
            public void eventOccurred(RequestCompletedEvent e) {
                Date startDate = e.getRequestResult().getStartDate();
                Date stopDate = e.getRequestResult().getStopDate();

                if (startDate != null && stopDate != null) {
                    long requestDuration = stopDate.getTime() - startDate.getTime();
                    remoteStoreMonitor.requestDuration(requestDuration, TimeUnit.MILLISECONDS);
                }

                Exception exception = e.getRequestResult().getException();

                if (exception == null) {
                    remoteStoreMonitor.requestCount();
                } else {
                    remoteStoreMonitor.requestError();
                }
            }

        });*/
    }

    public void setWriteAccessController(WriteAccessController writeAccessController) {
        this.writeAccessController = writeAccessController;
    }
}
