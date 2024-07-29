package com.linbit.linstor.dbcp.migration.k8s.crd;

import com.linbit.linstor.ControllerK8sCrdDatabase;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_15_0;

@K8sCrdMigration(
    description = "initial data",
    version = 1
)
public class Migration_01_v1_15_0_init extends BaseK8sCrdMigration
{
    public Migration_01_v1_15_0_init()
    {
        super(
            null, // only valid for migration 0 -> 1
            GenCrdV1_15_0.createMigrationContext()
        );
    }

    @Override
    public @Nullable MigrationResult migrateImpl(ControllerK8sCrdDatabase k8sDbRef) throws Exception
    {
        // load data from database that needs to change
        // noop for initial migration

        // update CRD entries for all DatabaseTables
        updateCrdSchemaForAllTables();

        // write modified data to database
        createSecConfiguration("SECURITYLEVEL", "SecurityLevel", "NO_SECURITY");
        createSecConfiguration("AUTHREQUIRED", "AuthRequired", "false");
        createSecIdentities("SYSTEM", "SYSTEM", null, null, true, true);
        createSecIdentities("PUBLIC", "PUBLIC", null, null, true, true);
        createSecTypes("SYSTEM", "SYSTEM", true);
        createSecTypes("PUBLIC", "PUBLIC", true);
        createSecTypes("SHARED", "SHARED", true);
        createSecTypes("SYSADM", "SysAdm", true);
        createSecTypes("USER", "User", true);
        createSecRoles("SYSTEM", "SYSTEM", "SYSTEM", true, -1);
        createSecRoles("PUBLIC", "PUBLIC", "PUBLIC", true, 0);
        createSecRoles("SYSADM", "SysAdm", "SYSADM", true, -1);
        createSecRoles("USER", "User", "USER", true, 0);
        createSecIdRoleMap("SYSTEM", "SYSTEM");
        createSecIdRoleMap("PUBLIC", "PUBLIC");
        createSecAccessTypes("CONTROL", 15);
        createSecAccessTypes("CHANGE", 7);
        createSecAccessTypes("USE", 3);
        createSecAccessTypes("VIEW", 1);
        createSecTypeRules("SYSTEM", "SYSTEM", 15);
        createSecTypeRules("SYSTEM", "PUBLIC", 15);
        createSecTypeRules("SYSTEM", "SHARED", 15);
        createSecTypeRules("SYSTEM", "SYSADM", 15);
        createSecTypeRules("SYSTEM", "USER", 15);
        createSecTypeRules("PUBLIC", "SYSTEM", 3);
        createSecTypeRules("PUBLIC", "PUBLIC", 15);
        createSecTypeRules("PUBLIC", "SHARED", 7);
        createSecTypeRules("PUBLIC", "SYSADM", 3);
        createSecTypeRules("PUBLIC", "USER", 3);
        createSecTypeRules("SYSADM", "SYSTEM", 15);
        createSecTypeRules("SYSADM", "PUBLIC", 15);
        createSecTypeRules("SYSADM", "SHARED", 15);
        createSecTypeRules("SYSADM", "SYSADM", 15);
        createSecTypeRules("SYSADM", "USER", 15);
        createSecTypeRules("USER", "SYSTEM", 3);
        createSecTypeRules("USER", "PUBLIC", 7);
        createSecTypeRules("USER", "SHARED", 7);
        createSecTypeRules("USER", "SYSADM", 3);
        createSecTypeRules("USER", "USER", 15);
        createSecDfltRoles("SYSTEM", "SYSTEM");
        createSecDfltRoles("PUBLIC", "PUBLIC");
        createSecObjectProtection("/sys/controller/nodesMap", "SYSTEM", "SYSADM", "SHARED");
        createSecObjectProtection("/sys/controller/rscDfnMap", "SYSTEM", "SYSADM", "SHARED");
        createSecObjectProtection("/sys/controller/storPoolMap", "SYSTEM", "SYSADM", "SHARED");
        createSecObjectProtection("/sys/controller/conf", "SYSTEM", "SYSADM", "SYSTEM");
        createSecObjectProtection("/sys/controller/shutdown", "SYSTEM", "SYSADM", "SYSTEM");
        createSecObjectProtection("/storpooldefinitions/DFLTSTORPOOL", "SYSTEM", "SYSADM", "SHARED");
        createSecObjectProtection(
            "/storpooldefinitions/DFLTDISKLESSSTORPOOL",
            "SYSTEM",
            "SYSADM",
            "SHARED"
        );
        createSecObjectProtection("/resourcegroups/DFLTRSCGRP", "SYSTEM", "SYSADM", "SHARED");
        createSecObjectProtection("/sys/controller/rscGrpMap", "SYSTEM", "SYSTEM", "SYSTEM");
        createSecObjectProtection("/sys/controller/freeSpaceMgrMap", "SYSTEM", "SYSTEM", "SYSTEM");
        createSecObjectProtection("/sys/controller/keyValueStoreMap", "SYSTEM", "SYSTEM", "SYSTEM");
        createSecObjectProtection("/sys/controller/externalFileMap", "SYSTEM", "SYSTEM", "SYSTEM");
        createSecObjectProtection("/sys/controller/remoteMap", "SYSTEM", "SYSTEM", "SYSTEM");
        createSecAclMap("/sys/controller/nodesMap", "SYSTEM", 15);
        createSecAclMap("/sys/controller/nodesMap", "USER", 7);
        createSecAclMap("/sys/controller/rscDfnMap", "SYSTEM", 15);
        createSecAclMap("/sys/controller/rscDfnMap", "USER", 7);
        createSecAclMap("/sys/controller/storPoolMap", "SYSTEM", 15);
        createSecAclMap("/sys/controller/storPoolMap", "USER", 7);
        createSecAclMap("/sys/controller/conf", "SYSTEM", 15);
        createSecAclMap("/storpooldefinitions/DFLTSTORPOOL", "PUBLIC", 7);
        createSecAclMap("/storpooldefinitions/DFLTSTORPOOL", "USER", 7);
        createSecAclMap("/storpooldefinitions/DFLTDISKLESSSTORPOOL", "PUBLIC", 7);
        createSecAclMap("/storpooldefinitions/DFLTDISKLESSSTORPOOL", "USER", 7);
        createSecAclMap("/sys/controller/nodesMap", "PUBLIC", 7);
        createSecAclMap("/sys/controller/rscDfnMap", "PUBLIC", 7);
        createSecAclMap("/sys/controller/storPoolMap", "PUBLIC", 7);
        createSecAclMap("/sys/controller/conf", "PUBLIC", 1);
        createSecAclMap("/resourcegroups/DFLTRSCGRP", "PUBLIC", 7);
        createSecAclMap("/resourcegroups/DFLTRSCGRP", "USER", 7);
        createSecAclMap("/sys/controller/rscGrpMap", "SYSTEM", 15);
        createSecAclMap("/sys/controller/freeSpaceMgrMap", "SYSTEM", 15);
        createSecAclMap("/sys/controller/keyValueStoreMap", "SYSTEM", 15);
        createSecAclMap("/sys/controller/externalFileMap", "SYSTEM", 15);
        createSecAclMap("/sys/controller/remoteMap", "SYSTEM", 15);
        createSecAclMap("/sys/controller/shutdown", "SYSTEM", 15);
        createStorPoolDefinitions("f51611c6-528f-4793-a87a-866d09e6733a", "DFLTSTORPOOL", "DfltStorPool");
        createStorPoolDefinitions(
            "622807eb-c8c4-44f0-b03d-a08173c8fa1b",
            "DFLTDISKLESSSTORPOOL",
            "DfltDisklessStorPool"
        );
        createResourceGroups(
            "a52e934a-9fd9-44cb-9db1-716dcd13aae3",
            "DFLTRSCGRP",
            "DfltRscGrp",
            null,
            null,
            2,
            "[]",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
        createPropsContainers("/CTRLCFG", "defaultDebugSslConnector", "DebugSslConnector");
        createPropsContainers("/CTRLCFG", "netcom/DebugSslConnector/bindaddress", "::0");
        createPropsContainers("/CTRLCFG", "netcom/DebugSslConnector/enabled", "true");
        createPropsContainers("/CTRLCFG", "netcom/DebugSslConnector/keyPasswd", "linstor");
        createPropsContainers("/CTRLCFG", "netcom/DebugSslConnector/keyStore", "ssl/keystore.jks");
        createPropsContainers("/CTRLCFG", "netcom/DebugSslConnector/keyStorePasswd", "linstor");
        createPropsContainers("/CTRLCFG", "netcom/DebugSslConnector/port", "3373");
        createPropsContainers("/CTRLCFG", "netcom/DebugSslConnector/sslProtocol", "TLSv1.2");
        createPropsContainers("/CTRLCFG", "netcom/DebugSslConnector/trustStore", "ssl/certificates.jks");
        createPropsContainers("/CTRLCFG", "netcom/DebugSslConnector/trustStorePasswd", "linstor");
        createPropsContainers("/CTRLCFG", "netcom/DebugSslConnector/type", "ssl");
        createPropsContainers("/CTRLCFG", "netcom/PlainConnector/enabled", "true");
        createPropsContainers("/CTRLCFG", "netcom/PlainConnector/port", "3376");
        createPropsContainers("/CTRLCFG", "netcom/PlainConnector/type", "plain");
        createPropsContainers("/CTRLCFG", "netcom/SslConnector/enabled", "true");
        createPropsContainers("/CTRLCFG", "netcom/SslConnector/keyPasswd", "linstor");
        createPropsContainers("/CTRLCFG", "netcom/SslConnector/keyStore", "ssl/keystore.jks");
        createPropsContainers("/CTRLCFG", "netcom/SslConnector/keyStorePasswd", "linstor");
        createPropsContainers("/CTRLCFG", "netcom/SslConnector/port", "3377");
        createPropsContainers("/CTRLCFG", "netcom/SslConnector/sslProtocol", "TLSv1.2");
        createPropsContainers("/CTRLCFG", "netcom/SslConnector/trustStore", "ssl/certificates.jks");
        createPropsContainers("/CTRLCFG", "netcom/SslConnector/trustStorePasswd", "linstor");
        createPropsContainers("/CTRLCFG", "netcom/SslConnector/type", "ssl");
        createPropsContainers("/CTRLCFG", "DrbdOptions/auto-quorum", "io-error");
        createPropsContainers("/CTRLCFG", "DrbdOptions/auto-add-quorum-tiebreaker", "True");
        createPropsContainers("/CTRLCFG", "netcom/PlainConnector/bindaddress", "");
        createPropsContainers("/CTRLCFG", "netcom/SslConnector/bindaddress", "");
        createPropsContainers("/CTRLCFG", "DrbdOptions/auto-diskful-allow-cleanup", "True");
        createPropsContainers("/CTRLCFG", "DrbdOptions/AutoEvictAllowEviction", "True");
        createPropsContainers(
            "/CTRLCFG",
            "DrbdOptions/auto-verify-algo-allowed-list",
            "crct10dif-pclmul;crct10dif-generic;sha384-generic;sha512-generic;sha256-generic;md5-generic"
        );
        createPropsContainers("/CTRLCFG", "Cluster/LocalID", "4ac9438a-ead8-4503-846b-56440ce1412a");
        createPropsContainers("STLTCFG", "Cluster/LocalID", "4ac9438a-ead8-4503-846b-56440ce1412a");
        createPropsContainers("/CTRLCFG", "defaultPlainConSvc", "PlainConnector");
        createPropsContainers("/CTRLCFG", "defaultSslConSvc", "SslConnector");

        return null;
    }

    private void createSecConfiguration(
        String entryKey,
        String entryDspKey,
        String entryValue
    )
    {
        txTo.create(
            GenCrdV1_15_0.GeneratedDatabaseTables.SEC_CONFIGURATION,
            GenCrdV1_15_0.createSecConfiguration(entryKey, entryDspKey, entryValue)
        );
    }

    private void createSecIdentities(
        String identityName,
        String identityDspName,
        @Nullable String passSalt,
        @Nullable String passHash,
        boolean idEnabled,
        boolean idLocked
    )
    {
        txTo.create(
            GenCrdV1_15_0.GeneratedDatabaseTables.SEC_IDENTITIES,
            GenCrdV1_15_0.createSecIdentities(identityName, identityDspName, passSalt, passHash, idEnabled, idLocked)
        );
    }

    private void createSecTypes(
        String typeName,
        String typeDspName,
        boolean typeEnabled
    )
    {
        txTo.create(
            GenCrdV1_15_0.GeneratedDatabaseTables.SEC_TYPES,
            GenCrdV1_15_0.createSecTypes(typeName, typeDspName, typeEnabled)
        );
    }

    private void createSecRoles(
        String roleName,
        String roleDspName,
        String domainName,
        boolean roleEnabled,
        long rolePrivileges
    )
    {
        txTo.create(
            GenCrdV1_15_0.GeneratedDatabaseTables.SEC_ROLES,
            GenCrdV1_15_0.createSecRoles(roleName, roleDspName, domainName, roleEnabled, rolePrivileges)
        );
    }

    private void createSecIdRoleMap(
        String identityName,
        String roleName
    )
    {
        txTo.create(
            GenCrdV1_15_0.GeneratedDatabaseTables.SEC_ID_ROLE_MAP,
            GenCrdV1_15_0.createSecIdRoleMap(
                identityName,
                roleName
            )
        );
    }

    private void createSecAccessTypes(
        String accessTypeName,
        int accessTypeValue
    )
    {
        txTo.create(
            GenCrdV1_15_0.GeneratedDatabaseTables.SEC_ACCESS_TYPES,
            GenCrdV1_15_0.createSecAccessTypes(
                accessTypeName,
                (short) accessTypeValue
            )
        );
    }

    private void createSecTypeRules(
        String domainName,
        String typeName,
        int accessType
    )
    {
        txTo.create(
            GenCrdV1_15_0.GeneratedDatabaseTables.SEC_TYPE_RULES,
            GenCrdV1_15_0.createSecTypeRules(
                domainName,
                typeName,
                (short) accessType
            )
        );
    }

    private void createSecDfltRoles(
        String identityName,
        String roleName
    )
    {
        txTo.create(
            GenCrdV1_15_0.GeneratedDatabaseTables.SEC_DFLT_ROLES,
            GenCrdV1_15_0.createSecDfltRoles(
                identityName,
                roleName
            )
        );
    }

    private void createSecObjectProtection(
        String objectPath,
        String creatorIdentityName,
        String ownerRoleName,
        String securityTypeName
    )
    {
        txTo.create(
            GenCrdV1_15_0.GeneratedDatabaseTables.SEC_OBJECT_PROTECTION,
            GenCrdV1_15_0.createSecObjectProtection(
                objectPath,
                creatorIdentityName,
                ownerRoleName,
                securityTypeName
            )
        );
    }

    private void createSecAclMap(
        String objectPath,
        String roleName,
        int accessType
    )
    {
        txTo.create(
            GenCrdV1_15_0.GeneratedDatabaseTables.SEC_ACL_MAP,
            GenCrdV1_15_0.createSecAclMap(
                objectPath,
                roleName,
                (short) accessType
            )
        );
    }

    private void createStorPoolDefinitions(
        String uuid,
        String poolName,
        String poolDspName
    )
    {
        txTo.create(
            GenCrdV1_15_0.GeneratedDatabaseTables.STOR_POOL_DEFINITIONS,
            GenCrdV1_15_0.createStorPoolDefinitions(
                uuid,
                poolName,
                poolDspName
            )
        );
    }

    private void createResourceGroups(
        String uuid,
        String resourceGroupName,
        String resourceGroupDspName,
        @Nullable String description,
        @Nullable String layerStack,
        int replicaCount,
        String nodeNameList,
        @Nullable String poolName,
        @Nullable String poolNameDiskless,
        @Nullable String doNotPlaceWithRscRegex,
        @Nullable String doNotPlaceWithRscList,
        @Nullable String replicasOnSame,
        @Nullable String replicasOnDifferent,
        @Nullable String allowedProviderList,
        @Nullable Boolean disklessOnRemaining
    )
    {
        txTo.create(
            GenCrdV1_15_0.GeneratedDatabaseTables.RESOURCE_GROUPS,
            GenCrdV1_15_0.createResourceGroups(
                uuid,
                resourceGroupName,
                resourceGroupDspName,
                description,
                layerStack,
                replicaCount,
                nodeNameList,
                poolName,
                poolNameDiskless,
                doNotPlaceWithRscRegex,
                doNotPlaceWithRscList,
                replicasOnSame,
                replicasOnDifferent,
                allowedProviderList,
                disklessOnRemaining
            )
        );
    }

    private void createPropsContainers(
        String propsInstance,
        String propKey,
        String propValue
    )
    {
        txTo.create(
            GenCrdV1_15_0.GeneratedDatabaseTables.PROPS_CONTAINERS,
            GenCrdV1_15_0.createPropsContainers(
                propsInstance,
                propKey,
                propValue
            )
        );
    }
}
