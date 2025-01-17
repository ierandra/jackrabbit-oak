/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */
package org.apache.jackrabbit.oak.run.osgi;

import static org.apache.jackrabbit.oak.run.osgi.OakOSGiRepositoryFactory.REPOSITORY_CONFIG_FILE;
import static org.junit.Assert.assertEquals;

import java.lang.management.ManagementFactory;
import java.util.Set;
import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.Query;
import javax.management.QueryExp;
import org.apache.jackrabbit.oak.api.jmx.RepositoryManagementMBean;
import org.junit.Before;
import org.junit.Test;

public class MBeanIntegrationTest extends AbstractRepositoryFactoryTest {

    @Before
    public void setupRepo() {
        config.put(REPOSITORY_CONFIG_FILE, createConfigValue("oak-base-config.json", "oak-tar-config.json"));
    }

    @Test
    public void jmxIntegration() throws Exception {
        repository = repositoryFactory.getRepository(config);
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        QueryExp q = Query.isInstanceOf(Query.value(RepositoryManagementMBean.class.getName()));
        Set<ObjectInstance> mbeans = server.queryMBeans(new ObjectName("org.apache.jackrabbit.oak:*"), q);
        assertEquals(1, mbeans.size());
    }

}
