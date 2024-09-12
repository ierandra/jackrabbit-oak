/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.segment.azure;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.LocationMode;
import com.microsoft.azure.storage.StorageCredentials;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import org.apache.commons.lang3.StringUtils;
import org.apache.jackrabbit.oak.segment.azure.v8.AzurePersistenceV8;
import org.apache.jackrabbit.oak.segment.azure.v8.AzureSegmentStoreServiceV8;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.jetbrains.annotations.NotNull;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Hashtable;
import java.util.Objects;

import static org.osgi.framework.Constants.SERVICE_PID;

@Component(
        configurationPolicy = ConfigurationPolicy.REQUIRE,
        configurationPid = {Configuration.PID})
public class AzureSegmentStoreService {

    private static final Logger log = LoggerFactory.getLogger(AzureSegmentStoreService.class);

    public static final String DEFAULT_CONTAINER_NAME = "oak";

    public static final String DEFAULT_ROOT_PATH = "/oak";

    public static final boolean DEFAULT_ENABLE_SECONDARY_LOCATION = false;
    public static final String DEFAULT_ENDPOINT_SUFFIX = "core.windows.net";

    private ServiceRegistration registration;
    private static AzureStorageCredentialManager azureStorageCredentialManager;

    private final boolean useAzureSdkV12 = Boolean.getBoolean("segment.azure.v12.enabled");


    @Activate
    public void activate(ComponentContext context, Configuration config) throws IOException {
        if (useAzureSdkV12) {
            log.info("Starting nodestore using Azure SDK 12");
            AzurePersistenceV8 persistence = AzureSegmentStoreServiceV8.createAzurePersistenceFrom(config);
            registration = context.getBundleContext()
                    .registerService(SegmentNodeStorePersistence.class, persistence, new Hashtable<String, Object>() {{
                        put(SERVICE_PID, String.format("%s(%s, %s)", AzurePersistenceV8.class.getName(), config.accountName(), config.rootPath()));
                        if (!Objects.equals(config.role(), "")) {
                            put("role", config.role());
                        }
                    }});
        } else {
            log.info("Starting nodestore using Azure SDK 8");
            AzurePersistenceV8 persistence = AzureSegmentStoreServiceV8.createAzurePersistenceFrom(config);
            registration = context.getBundleContext()
                    .registerService(SegmentNodeStorePersistence.class, persistence, new Hashtable<String, Object>() {{
                        put(SERVICE_PID, String.format("%s(%s, %s)", AzurePersistenceV8.class.getName(), config.accountName(), config.rootPath()));
                        if (!Objects.equals(config.role(), "")) {
                            put("role", config.role());
                        }
                    }});
        }
    }

    @Deactivate
    public void deactivate() throws IOException {
        if (registration != null) {
            registration.unregister();
            registration = null;
        }
        if (azureStorageCredentialManager != null) {
            azureStorageCredentialManager.close();
        }
    }

    private static AzurePersistenceV8 createAzurePersistenceFrom(Configuration configuration) throws IOException {
        if (!StringUtils.isBlank(configuration.connectionURL())) {
            return createPersistenceFromConnectionURL(configuration);
        }
        if (!StringUtils.isAnyBlank(configuration.clientId(), configuration.clientSecret(), configuration.tenantId())) {
            return createPersistenceFromServicePrincipalCredentials(configuration);
        }
        if (!StringUtils.isBlank(configuration.sharedAccessSignature())) {
            return createPersistenceFromSasUri(configuration);
        }
        return createPersistenceFromAccessKey(configuration);
    }

    private static AzurePersistenceV8 createPersistenceFromAccessKey(Configuration configuration) throws IOException {
        StringBuilder connectionString = new StringBuilder();
        connectionString.append("DefaultEndpointsProtocol=https;");
        connectionString.append("AccountName=").append(configuration.accountName()).append(';');
        connectionString.append("AccountKey=").append(configuration.accessKey()).append(';');
        if (!StringUtils.isBlank(configuration.blobEndpoint())) {
            connectionString.append("BlobEndpoint=").append(configuration.blobEndpoint()).append(';');
        }
        return createAzurePersistence(connectionString.toString(), configuration, true);
    }

    private static AzurePersistenceV8 createPersistenceFromSasUri(Configuration configuration) throws IOException {
        StringBuilder connectionString = new StringBuilder();
        connectionString.append("DefaultEndpointsProtocol=https;");
        connectionString.append("AccountName=").append(configuration.accountName()).append(';');
        connectionString.append("SharedAccessSignature=").append(configuration.sharedAccessSignature()).append(';');
        if (!StringUtils.isBlank(configuration.blobEndpoint())) {
            connectionString.append("BlobEndpoint=").append(configuration.blobEndpoint()).append(';');
        }
        return createAzurePersistence(connectionString.toString(), configuration, false);
    }

    @NotNull
    private static AzurePersistenceV8 createPersistenceFromConnectionURL(Configuration configuration) throws IOException {
        return createAzurePersistence(configuration.connectionURL(), configuration, true);
    }

    @NotNull
    private static AzurePersistenceV8 createPersistenceFromServicePrincipalCredentials(Configuration configuration) throws IOException {
        azureStorageCredentialManager = new AzureStorageCredentialManager();
        StorageCredentials storageCredentialsToken = azureStorageCredentialManager.getStorageCredentialAccessTokenFromServicePrincipals(configuration.accountName(), configuration.clientId(), configuration.clientSecret(), configuration.tenantId());

        try {
            CloudStorageAccount cloud = new CloudStorageAccount(storageCredentialsToken, true, DEFAULT_ENDPOINT_SUFFIX, configuration.accountName());
            return createAzurePersistence(cloud, configuration, true);
        } catch (StorageException | URISyntaxException e) {
            throw new IOException(e);
        }
    }

    @NotNull
    private static AzurePersistenceV8 createAzurePersistence(String connectionString, Configuration configuration, boolean createContainer) throws IOException {
        try {
            CloudStorageAccount cloud = CloudStorageAccount.parse(connectionString);
            log.info("Connection string: '{}'", cloud);
            return createAzurePersistence(cloud, configuration, createContainer);
        } catch (StorageException | URISyntaxException | InvalidKeyException e) {
            throw new IOException(e);
        }
    }

    @NotNull
    private static AzurePersistenceV8 createAzurePersistence(CloudStorageAccount cloud, Configuration configuration, boolean createContainer) throws URISyntaxException, StorageException {
        CloudBlobClient cloudBlobClient = cloud.createCloudBlobClient();
        BlobRequestOptions blobRequestOptions = new BlobRequestOptions();

        if (configuration.enableSecondaryLocation()) {
            blobRequestOptions.setLocationMode(LocationMode.PRIMARY_THEN_SECONDARY);
        }
        cloudBlobClient.setDefaultRequestOptions(blobRequestOptions);

        CloudBlobContainer container = cloudBlobClient.getContainerReference(configuration.containerName());
        if (createContainer && !container.exists()) {
            container.create();
        }
        String path = normalizePath(configuration.rootPath());
        return new AzurePersistenceV8(container.getDirectoryReference(path));
    }

    @NotNull
    private static String normalizePath(@NotNull String rootPath) {
        if (rootPath.length() > 0 && rootPath.charAt(0) == '/') {
            return rootPath.substring(1);
        }
        return rootPath;
    }

}