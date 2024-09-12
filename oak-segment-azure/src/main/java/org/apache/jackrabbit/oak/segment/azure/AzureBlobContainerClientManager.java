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

import com.azure.core.credential.AzureNamedKeyCredential;
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.jackrabbit.oak.segment.azure.util.Environment;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.segment.azure.AzureUtilities.AZURE_CLIENT_ID;
import static org.apache.jackrabbit.oak.segment.azure.AzureUtilities.AZURE_CLIENT_SECRET;
import static org.apache.jackrabbit.oak.segment.azure.AzureUtilities.AZURE_SECRET_KEY;
import static org.apache.jackrabbit.oak.segment.azure.AzureUtilities.AZURE_TENANT_ID;

public class AzureBlobContainerClientManager {
    private static final Logger log = LoggerFactory.getLogger(AzureBlobContainerClientManager.class);

    public BlobContainerClient getBlobContainerClientFromEnvironment(@NotNull String accountName, @NotNull String containerName, @NotNull Environment environment) {
        final String clientId = environment.getVariable(AZURE_CLIENT_ID);
        final String clientSecret = environment.getVariable(AZURE_CLIENT_SECRET);
        final String tenantId = environment.getVariable(AZURE_TENANT_ID);

        if (StringUtils.isNoneBlank(clientId, clientSecret, tenantId)) {
            try {
                return getBlobContainerClientFromServicePrincipals(accountName, containerName, clientId, clientSecret, tenantId);
            } catch (IllegalArgumentException | StringIndexOutOfBoundsException e) {
                log.error("Error occurred while connecting to Azure Storage using service principals: ", e);
                throw new IllegalArgumentException(
                        "Could not connect to the Azure Storage. Please verify if AZURE_CLIENT_ID, AZURE_CLIENT_SECRET and AZURE_TENANT_ID environment variables are correctly set!");
            }
        }

        log.warn("AZURE_CLIENT_ID, AZURE_CLIENT_SECRET and AZURE_TENANT_ID environment variables empty or missing. Switching to authentication with AZURE_SECRET_KEY.");

        String key = environment.getVariable(AZURE_SECRET_KEY);
        try {
            return new BlobContainerClientBuilder()
                    .credential(new AzureNamedKeyCredential(accountName, key))
                    .containerName(containerName)
                    .buildClient();
        } catch (IllegalArgumentException | StringIndexOutOfBoundsException e) {
            log.error("Error occurred while connecting to Azure Storage using secret key: ", e);
            throw new IllegalArgumentException(
                    "Could not connect to the Azure Storage. Please verify if AZURE_SECRET_KEY environment variable is correctly set!");
        }
    }

    public BlobContainerClient getBlobContainerClientFromServicePrincipals(String accountName, String containerName, String clientId, String clientSecret, String tenantId) {
        ClientSecretCredential clientSecretCredential = new ClientSecretCredentialBuilder()
                .clientId(clientId)
                .clientSecret(clientSecret)
                .tenantId(tenantId)
                .build();

        String endpoint = String.format("https://%s.blob.core.windows.net", accountName);

        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .endpoint(endpoint)
                .credential(clientSecretCredential)
                .buildClient();

        return blobServiceClient.getBlobContainerClient(containerName);
    }
}
