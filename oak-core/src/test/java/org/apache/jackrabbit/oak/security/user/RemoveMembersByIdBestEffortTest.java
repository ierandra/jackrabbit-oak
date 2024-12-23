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
package org.apache.jackrabbit.oak.security.user;

import java.util.Set;

import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RemoveMembersByIdBestEffortTest extends AbstractRemoveMembersByIdTest {

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        return ConfigurationParameters.of(UserConfiguration.NAME,
                ConfigurationParameters.of(
                        ProtectedItemImporter.PARAM_IMPORT_BEHAVIOR, ImportBehavior.NAME_BESTEFFORT)
        );
    }

    @Test
    public void testNonExistingMember() throws Exception {
        Set<String> failed = removeNonExistingMember();
        assertEquals(Set.of(NON_EXISTING_IDS), failed);
    }

    @Test
    public void testNonExistingMemberAfterAdd() throws Exception {
        Set<String> failed = testGroup.addMembers(NON_EXISTING_IDS);
        assertTrue(failed.isEmpty());

        failed = removeNonExistingMember();
        assertTrue(failed.isEmpty());
    }

    @Test
    public void testMissingAccessMember() throws Exception {
        Set<String> failed = removeExistingMemberWithoutAccess();
        assertTrue(failed.isEmpty());

        root.refresh();
        assertFalse(testGroup.isMember(memberGroup));
    }

    @Test
    public void testMemberListExistingMembers() throws Exception {
        MembershipProvider mp = ((UserManagerImpl) getUserManager(root)).getMembershipProvider();
        try {
            mp.setMembershipSizeThreshold(5);
            for (int i = 0; i < 10; i++) {
                testGroup.addMembers("member" + i);
            }

            Set<String> failed = testGroup.removeMembers("member8");
            assertTrue(failed.isEmpty());
        } finally {
            mp.setMembershipSizeThreshold(100); // back to default
        }
    }
}