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
import com.azure.storage.blob.specialized.BlockBlobClient;
import org.apache.jackrabbit.oak.segment.remote.RemoteUtilities;
import org.apache.jackrabbit.oak.segment.remote.WriteAccessController;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveWriter;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.ArrayList;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class AzureArchiveManager implements SegmentArchiveManager {

    private static final Logger log = LoggerFactory.getLogger(AzureArchiveManager.class);

    protected final BlobContainerClient blobContainerClient;

    protected final String rootPrefix;

    protected final IOMonitor ioMonitor;

    protected final FileStoreMonitor monitor;
    private WriteAccessController writeAccessController;

    public AzureArchiveManager(BlobContainerClient blobContainerClient, String rootPrefix, IOMonitor ioMonitor, FileStoreMonitor fileStoreMonitor, WriteAccessController writeAccessController) {
        this.blobContainerClient = blobContainerClient;
        this.rootPrefix = rootPrefix;
        this.ioMonitor = ioMonitor;
        this.monitor = fileStoreMonitor;
        this.writeAccessController = writeAccessController;
    }

    @Override
    public List<String> listArchives() throws IOException {
        try {
            List<String> archiveNames = blobContainerClient.listBlobsByHierarchy(rootPrefix + "/").stream()
                    .filter(BlobItem::isPrefix)
                    .filter(blobItem -> blobItem.getName().endsWith(".tar") || blobItem.getName().endsWith(".tar/"))
                    .map(blobItem -> blobItem.getName().substring(rootPrefix.length() + 1, blobItem.getName().length() - 1))
                    .collect(Collectors.toList());

            Iterator<String> it = archiveNames.iterator();
            while (it.hasNext()) {
                String archiveName = it.next();
                if (isArchiveEmpty(archiveName)) {
                    delete(archiveName);
                    it.remove();
                }
            }
            return archiveNames;
        } catch (BlobStorageException e) {
            throw new IOException(e);
        }
    }

    /**
     * Check if there's a valid 0000. segment in the archive
     *
     * @param archiveName
     * @return true if the archive is empty (no 0000.* segment)
     */
    private boolean isArchiveEmpty(String archiveName) {
        ListBlobsOptions listOptions = new ListBlobsOptions();
        String prefix = String.format("%s/%s/%s", rootPrefix, archiveName, "0000.");
        listOptions.setPrefix(prefix);
        listOptions.setMaxResultsPerPage(1);
        return blobContainerClient.listBlobs(listOptions, null).iterator().hasNext();
    }

    @Override
    public SegmentArchiveReader open(String archiveName) throws IOException {
        try {
            boolean isArchiveClosed = blobContainerClient.getBlobClient(rootPrefix + "/" + archiveName + "/closed").getBlockBlobClient().exists();
            if (isArchiveClosed) {
                return null;
            }
            return new AzureSegmentArchiveReader(blobContainerClient, rootPrefix, archiveName, ioMonitor);
        } catch (BlobStorageException | IOException e) {
            throw new IOException(e);
        }
    }

    @Override
    public SegmentArchiveReader forceOpen(String archiveName) throws IOException {
        return new AzureSegmentArchiveReader(blobContainerClient, rootPrefix, archiveName, ioMonitor);
    }

    @Override
    public SegmentArchiveWriter create(String archiveName) throws IOException {
        return new AzureSegmentArchiveWriter(blobContainerClient, rootPrefix, archiveName, ioMonitor, monitor, writeAccessController);
    }

    @Override
    public boolean delete(String archiveName) {
        try {
            getBlobs(archiveName)
                    .forEach(blobItem -> {
                        try {
                            writeAccessController.checkWritingAllowed();
                            blobContainerClient.getBlobClient(blobItem.getName()).delete();
                        } catch (BlobStorageException e) {
                            log.error("Can't delete segment {}", blobItem.getName(), e);
                        }
                    });
            return true;
        } catch (IOException e) {
            log.error("Can't delete archive {}", archiveName, e);
            return false;
        }
    }

    @Override
    public boolean renameTo(String from, String to) {
        copyBlobs(from, to, true);
        return true;
    }

    @Override
    public void copyFile(String from, String to) throws IOException {
        copyBlobs(from, to, false);
    }

    @Override
    public boolean exists(String archiveName) {
        try {
            String archivePrefix = getDirectory(archiveName);
            ListBlobsOptions listOptions = new ListBlobsOptions();
            listOptions.setPrefix(archivePrefix);
            listOptions.setMaxResultsPerPage(1);
            return blobContainerClient.listBlobs(listOptions, null).iterator().hasNext();
        } catch (BlobStorageException e) {
            log.error("Can't check the existence of {}", archiveName, e);
            return false;
        }
    }

    @Override
    public void recoverEntries(String archiveName, LinkedHashMap<UUID, byte[]> entries) throws IOException {
        Pattern pattern = Pattern.compile(RemoteUtilities.SEGMENT_FILE_NAME_PATTERN);
        List<RecoveredEntry> entryList = new ArrayList<>();

        for (BlobItem b : getBlobs(archiveName)) {
            String name = b.getName();
            Matcher m = pattern.matcher(name);
            if (!m.matches()) {
                continue;
            }
            int position = Integer.parseInt(m.group(1), 16);
            UUID uuid = UUID.fromString(m.group(2));
            long length = b.getProperties().getContentLength();
            if (length > 0) {
                byte[] data;
                try {
                    data = blobContainerClient.getBlobClient(b.getName()).downloadContent().toBytes();
                } catch (BlobStorageException e) {
                    throw new IOException(e);
                }
                entryList.add(new RecoveredEntry(position, uuid, data, name));
            }
        }
        Collections.sort(entryList);

        int i = 0;
        for (RecoveredEntry e : entryList) {
            if (e.position != i) {
                log.warn("Missing entry {}.??? when recovering {}. No more segments will be read.", String.format("%04X", i), archiveName);
                break;
            }
            log.info("Recovering segment {}/{}", archiveName, e.fileName);
            entries.put(e.uuid, e.data);
            i++;
        }
    }

    private void delete(String archiveName, Set<UUID> recoveredEntries) throws IOException {
        getBlobs(archiveName)
                .forEach(cloudBlob -> {
                    if (!recoveredEntries.contains(RemoteUtilities.getSegmentUUID(cloudBlob.getName()))) {
                        try {
                            blobContainerClient.getBlobClient(cloudBlob.getName()).delete();
                        } catch (BlobStorageException e) {
                            log.error("Can't delete segment {}", cloudBlob.getName(), e);
                        }
                    }
                });
    }

    /**
     * Method is not deleting  segments from the directory given with {@code archiveName}, if they are in the set of recovered segments.
     * Reason for that is because during execution of this method, remote repository can be accessed by another application, and deleting a valid segment can
     * cause consistency issues there.
     */
    @Override
    public void backup(@NotNull String archiveName, @NotNull String backupArchiveName, @NotNull Set<UUID> recoveredEntries) throws IOException {
        copyFile(archiveName, backupArchiveName);
        delete(archiveName, recoveredEntries);
    }

    protected String getDirectory(String archiveName) {
        return String.format("%s/%s", rootPrefix, archiveName);
    }

    private List<BlobItem> getBlobs(String archiveName) throws IOException {
        ListBlobsOptions listOptions = new ListBlobsOptions();
        listOptions.setPrefix(archiveName);

        return blobContainerClient.listBlobs(listOptions, null).stream().collect(Collectors.toList());
    }

    private void copyBlobs(String from, String to, boolean deleteSource) {
        ListBlobsOptions options = new ListBlobsOptions();
        options.setPrefix(rootPrefix + "/" + from + "/");
        blobContainerClient.listBlobs(options, null).forEach(blobItem -> {
            BlockBlobClient sourceBlobClient = blobContainerClient.getBlobClient(blobItem.getName()).getBlockBlobClient();
            BlockBlobClient targetBlobClient = blobContainerClient.getBlobClient(blobItem.getName().replace(from, to)).getBlockBlobClient();

            targetBlobClient.beginCopy(sourceBlobClient.getBlobUrl(), Duration.ofMillis(100)).waitForCompletion();

            if (deleteSource) {
                sourceBlobClient.delete();
            }
        });
    }

    private static class RecoveredEntry implements Comparable<RecoveredEntry> {

        private final byte[] data;

        private final UUID uuid;

        private final int position;

        private final String fileName;

        public RecoveredEntry(int position, UUID uuid, byte[] data, String fileName) {
            this.data = data;
            this.uuid = uuid;
            this.position = position;
            this.fileName = fileName;
        }

        @Override
        public int compareTo(RecoveredEntry o) {
            return Integer.compare(this.position, o.position);
        }
    }

}
