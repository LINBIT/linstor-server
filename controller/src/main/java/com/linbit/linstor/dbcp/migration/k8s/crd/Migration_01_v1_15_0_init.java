package com.linbit.linstor.dbcp.migration.k8s.crd;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbdrivers.DatabaseTable;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_15_0;
import com.linbit.linstor.transaction.BaseControllerK8sCrdTransactionMgrContext;
import com.linbit.linstor.transaction.K8sCrdMigrationContext;
import com.linbit.linstor.transaction.K8sCrdSchemaUpdateContext;
import com.linbit.linstor.transaction.K8sCrdTransaction;

@K8sCrdMigration(
    description = "initial data",
    version = 1
)
public class Migration_01_v1_15_0_init extends BaseK8sCrdMigration
{
    public Migration_01_v1_15_0_init()
    {
        super(
            new SpecialK8sTxMgrCtx(), // only valid for migration 0 -> 1
            GenCrdV1_15_0.createMigrationContext()
        );
    }

    /**
     * Special class that basically states that there are no tables. This class is only needed so that we do not have to
     * declare the fromCtx field as @Nullable. Otherwise ALL other migrations would also have to do null-checks
     */
    private static class SpecialK8sTxMgrCtx extends K8sCrdMigrationContext
    {
        private SpecialK8sTxMgrCtx()
        {
            // these Functions returning null should not be a problem, since the function's input should be a
            // DatabaseTable from "allDbTables" which we define being empty... In other words, the given Functions
            // should never be called anyways
            super(
                new BaseControllerK8sCrdTransactionMgrContext(ignored -> null, new DatabaseTable[0], "v0"),
                new K8sCrdSchemaUpdateContext(ignored -> null, ignored -> null, "v0")
            );
        }
    }

    @Override
    public @Nullable MigrationResult migrateImpl(MigrationContext migrationCtxRef) throws Exception
    {
        // load data from database that needs to change
        // noop for initial migration

        // update CRD entries for all DatabaseTables
        updateCrdSchemaForAllTables();

        K8sCrdTransaction txTo = migrationCtxRef.txTo;

        // write modified data to database
        createSecConfiguration(txTo, "SECURITYLEVEL", "SecurityLevel", "NO_SECURITY");
        createSecConfiguration(txTo, "AUTHREQUIRED", "AuthRequired", "false");
        createSecIdentities(txTo, "SYSTEM", "SYSTEM", null, null, true, true);
        createSecIdentities(txTo, "PUBLIC", "PUBLIC", null, null, true, true);
        createSecTypes(txTo, "SYSTEM", "SYSTEM", true);
        createSecTypes(txTo, "PUBLIC", "PUBLIC", true);
        createSecTypes(txTo, "SHARED", "SHARED", true);
        createSecTypes(txTo, "SYSADM", "SysAdm", true);
        createSecTypes(txTo, "USER", "User", true);
        createSecRoles(txTo, "SYSTEM", "SYSTEM", "SYSTEM", true, -1);
        createSecRoles(txTo, "PUBLIC", "PUBLIC", "PUBLIC", true, 0);
        createSecRoles(txTo, "SYSADM", "SysAdm", "SYSADM", true, -1);
        createSecRoles(txTo, "USER", "User", "USER", true, 0);
        createSecIdRoleMap(txTo, "SYSTEM", "SYSTEM");
        createSecIdRoleMap(txTo, "PUBLIC", "PUBLIC");
        createSecAccessTypes(txTo, "CONTROL", 15);
        createSecAccessTypes(txTo, "CHANGE", 7);
        createSecAccessTypes(txTo, "USE", 3);
        createSecAccessTypes(txTo, "VIEW", 1);
        createSecTypeRules(txTo, "SYSTEM", "SYSTEM", 15);
        createSecTypeRules(txTo, "SYSTEM", "PUBLIC", 15);
        createSecTypeRules(txTo, "SYSTEM", "SHARED", 15);
        createSecTypeRules(txTo, "SYSTEM", "SYSADM", 15);
        createSecTypeRules(txTo, "SYSTEM", "USER", 15);
        createSecTypeRules(txTo, "PUBLIC", "SYSTEM", 3);
        createSecTypeRules(txTo, "PUBLIC", "PUBLIC", 15);
        createSecTypeRules(txTo, "PUBLIC", "SHARED", 7);
        createSecTypeRules(txTo, "PUBLIC", "SYSADM", 3);
        createSecTypeRules(txTo, "PUBLIC", "USER", 3);
        createSecTypeRules(txTo, "SYSADM", "SYSTEM", 15);
        createSecTypeRules(txTo, "SYSADM", "PUBLIC", 15);
        createSecTypeRules(txTo, "SYSADM", "SHARED", 15);
        createSecTypeRules(txTo, "SYSADM", "SYSADM", 15);
        createSecTypeRules(txTo, "SYSADM", "USER", 15);
        createSecTypeRules(txTo, "USER", "SYSTEM", 3);
        createSecTypeRules(txTo, "USER", "PUBLIC", 7);
        createSecTypeRules(txTo, "USER", "SHARED", 7);
        createSecTypeRules(txTo, "USER", "SYSADM", 3);
        createSecTypeRules(txTo, "USER", "USER", 15);
        createSecDfltRoles(txTo, "SYSTEM", "SYSTEM");
        createSecDfltRoles(txTo, "PUBLIC", "PUBLIC");
        createSecObjectProtection(txTo, "/sys/controller/nodesMap", "SYSTEM", "SYSADM", "SHARED");
        createSecObjectProtection(txTo, "/sys/controller/rscDfnMap", "SYSTEM", "SYSADM", "SHARED");
        createSecObjectProtection(txTo, "/sys/controller/storPoolMap", "SYSTEM", "SYSADM", "SHARED");
        createSecObjectProtection(txTo, "/sys/controller/conf", "SYSTEM", "SYSADM", "SYSTEM");
        createSecObjectProtection(txTo, "/sys/controller/shutdown", "SYSTEM", "SYSADM", "SYSTEM");
        createSecObjectProtection(txTo, "/storpooldefinitions/DFLTSTORPOOL", "SYSTEM", "SYSADM", "SHARED");
        createSecObjectProtection(
            txTo,
            "/storpooldefinitions/DFLTDISKLESSSTORPOOL",
            "SYSTEM",
            "SYSADM", "SHARED"
        );
        createSecObjectProtection(txTo, "/resourcegroups/DFLTRSCGRP", "SYSTEM", "SYSADM", "SHARED");
        createSecObjectProtection(txTo, "/sys/controller/rscGrpMap", "SYSTEM", "SYSTEM", "SYSTEM");
        createSecObjectProtection(txTo, "/sys/controller/freeSpaceMgrMap", "SYSTEM", "SYSTEM", "SYSTEM");
        createSecObjectProtection(txTo, "/sys/controller/keyValueStoreMap", "SYSTEM", "SYSTEM", "SYSTEM");
        createSecObjectProtection(txTo, "/sys/controller/externalFileMap", "SYSTEM", "SYSTEM", "SYSTEM");
        createSecObjectProtection(txTo, "/sys/controller/remoteMap", "SYSTEM", "SYSTEM", "SYSTEM");
        createSecAclMap(txTo, "/sys/controller/nodesMap", "SYSTEM", 15);
        createSecAclMap(txTo, "/sys/controller/nodesMap", "USER", 7);
        createSecAclMap(txTo, "/sys/controller/rscDfnMap", "SYSTEM", 15);
        createSecAclMap(txTo, "/sys/controller/rscDfnMap", "USER", 7);
        createSecAclMap(txTo, "/sys/controller/storPoolMap", "SYSTEM", 15);
        createSecAclMap(txTo, "/sys/controller/storPoolMap", "USER", 7);
        createSecAclMap(txTo, "/sys/controller/conf", "SYSTEM", 15);
        createSecAclMap(txTo, "/storpooldefinitions/DFLTSTORPOOL", "PUBLIC", 7);
        createSecAclMap(txTo, "/storpooldefinitions/DFLTSTORPOOL", "USER", 7);
        createSecAclMap(txTo, "/storpooldefinitions/DFLTDISKLESSSTORPOOL", "PUBLIC", 7);
        createSecAclMap(txTo, "/storpooldefinitions/DFLTDISKLESSSTORPOOL", "USER", 7);
        createSecAclMap(txTo, "/sys/controller/nodesMap", "PUBLIC", 7);
        createSecAclMap(txTo, "/sys/controller/rscDfnMap", "PUBLIC", 7);
        createSecAclMap(txTo, "/sys/controller/storPoolMap", "PUBLIC", 7);
        createSecAclMap(txTo, "/sys/controller/conf", "PUBLIC", 1);
        createSecAclMap(txTo, "/resourcegroups/DFLTRSCGRP", "PUBLIC", 7);
        createSecAclMap(txTo, "/resourcegroups/DFLTRSCGRP", "USER", 7);
        createSecAclMap(txTo, "/sys/controller/rscGrpMap", "SYSTEM", 15);
        createSecAclMap(txTo, "/sys/controller/freeSpaceMgrMap", "SYSTEM", 15);
        createSecAclMap(txTo, "/sys/controller/keyValueStoreMap", "SYSTEM", 15);
        createSecAclMap(txTo, "/sys/controller/externalFileMap", "SYSTEM", 15);
        createSecAclMap(txTo, "/sys/controller/remoteMap", "SYSTEM", 15);
        createSecAclMap(txTo, "/sys/controller/shutdown", "SYSTEM", 15);
        createStorPoolDefinitions(txTo, "f51611c6-528f-4793-a87a-866d09e6733a", "DFLTSTORPOOL", "DfltStorPool");
        createStorPoolDefinitions(
            txTo,
            "622807eb-c8c4-44f0-b03d-a08173c8fa1b",
            "DFLTDISKLESSSTORPOOL", "DfltDisklessStorPool"
        );
        createResourceGroups(
            txTo,
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
        createPropsContainers(txTo, "/CTRLCFG", "defaultDebugSslConnector", "DebugSslConnector");
        createPropsContainers(txTo, "/CTRLCFG", "netcom/DebugSslConnector/bindaddress", "::0");
        createPropsContainers(txTo, "/CTRLCFG", "netcom/DebugSslConnector/enabled", "true");
        createPropsContainers(txTo, "/CTRLCFG", "netcom/DebugSslConnector/keyPasswd", "linstor");
        createPropsContainers(txTo, "/CTRLCFG", "netcom/DebugSslConnector/keyStore", "ssl/keystore.jks");
        createPropsContainers(txTo, "/CTRLCFG", "netcom/DebugSslConnector/keyStorePasswd", "linstor");
        createPropsContainers(txTo, "/CTRLCFG", "netcom/DebugSslConnector/port", "3373");
        createPropsContainers(txTo, "/CTRLCFG", "netcom/DebugSslConnector/sslProtocol", "TLSv1.2");
        createPropsContainers(txTo, "/CTRLCFG", "netcom/DebugSslConnector/trustStore", "ssl/certificates.jks");
        createPropsContainers(txTo, "/CTRLCFG", "netcom/DebugSslConnector/trustStorePasswd", "linstor");
        createPropsContainers(txTo, "/CTRLCFG", "netcom/DebugSslConnector/type", "ssl");
        createPropsContainers(txTo, "/CTRLCFG", "netcom/PlainConnector/enabled", "true");
        createPropsContainers(txTo, "/CTRLCFG", "netcom/PlainConnector/port", "3376");
        createPropsContainers(txTo, "/CTRLCFG", "netcom/PlainConnector/type", "plain");
        createPropsContainers(txTo, "/CTRLCFG", "netcom/SslConnector/enabled", "true");
        createPropsContainers(txTo, "/CTRLCFG", "netcom/SslConnector/keyPasswd", "linstor");
        createPropsContainers(txTo, "/CTRLCFG", "netcom/SslConnector/keyStore", "ssl/keystore.jks");
        createPropsContainers(txTo, "/CTRLCFG", "netcom/SslConnector/keyStorePasswd", "linstor");
        createPropsContainers(txTo, "/CTRLCFG", "netcom/SslConnector/port", "3377");
        createPropsContainers(txTo, "/CTRLCFG", "netcom/SslConnector/sslProtocol", "TLSv1.2");
        createPropsContainers(txTo, "/CTRLCFG", "netcom/SslConnector/trustStore", "ssl/certificates.jks");
        createPropsContainers(txTo, "/CTRLCFG", "netcom/SslConnector/trustStorePasswd", "linstor");
        createPropsContainers(txTo, "/CTRLCFG", "netcom/SslConnector/type", "ssl");
        createPropsContainers(txTo, "/CTRLCFG", "DrbdOptions/auto-quorum", "io-error");
        createPropsContainers(txTo, "/CTRLCFG", "DrbdOptions/auto-add-quorum-tiebreaker", "True");
        createPropsContainers(txTo, "/CTRLCFG", "netcom/PlainConnector/bindaddress", "");
        createPropsContainers(txTo, "/CTRLCFG", "netcom/SslConnector/bindaddress", "");
        createPropsContainers(txTo, "/CTRLCFG", "DrbdOptions/auto-diskful-allow-cleanup", "True");
        createPropsContainers(txTo, "/CTRLCFG", "DrbdOptions/AutoEvictAllowEviction", "True");
        createPropsContainers(
            txTo,
            "/CTRLCFG",
            "DrbdOptions/auto-verify-algo-allowed-list", "crct10dif-pclmul;crct10dif-generic;sha384-generic;sha512-generic;sha256-generic;md5-generic"
        );
        createPropsContainers(txTo, "/CTRLCFG", "Cluster/LocalID", "4ac9438a-ead8-4503-846b-56440ce1412a");
        createPropsContainers(txTo, "STLTCFG", "Cluster/LocalID", "4ac9438a-ead8-4503-846b-56440ce1412a");
        createPropsContainers(txTo, "/CTRLCFG", "defaultPlainConSvc", "PlainConnector");
        createPropsContainers(txTo, "/CTRLCFG", "defaultSslConSvc", "SslConnector");

        return null;
    }

    private void createSecConfiguration(
        K8sCrdTransaction txToRef,
        String entryKey,
        String entryDspKey, String entryValue
    )
    {
        txToRef.create(
            GenCrdV1_15_0.GeneratedDatabaseTables.SEC_CONFIGURATION,
            GenCrdV1_15_0.createSecConfiguration(entryKey, entryDspKey, entryValue)
        );
    }

    private void createSecIdentities(
        K8sCrdTransaction txToRef,
        String identityName,
        String identityDspName,
        @Nullable String passSalt,
        @Nullable String passHash,
        boolean idEnabled, boolean idLocked
    )
    {
        txToRef.create(
            GenCrdV1_15_0.GeneratedDatabaseTables.SEC_IDENTITIES,
            GenCrdV1_15_0.createSecIdentities(identityName, identityDspName, passSalt, passHash, idEnabled, idLocked)
        );
    }

    private void createSecTypes(
        K8sCrdTransaction txToRef,
        String typeName,
        String typeDspName, boolean typeEnabled
    )
    {
        txToRef.create(
            GenCrdV1_15_0.GeneratedDatabaseTables.SEC_TYPES,
            GenCrdV1_15_0.createSecTypes(typeName, typeDspName, typeEnabled)
        );
    }

    private void createSecRoles(
        K8sCrdTransaction txToRef,
        String roleName,
        String roleDspName,
        String domainName,
        boolean roleEnabled, long rolePrivileges
    )
    {
        txToRef.create(
            GenCrdV1_15_0.GeneratedDatabaseTables.SEC_ROLES,
            GenCrdV1_15_0.createSecRoles(roleName, roleDspName, domainName, roleEnabled, rolePrivileges)
        );
    }

    private void createSecIdRoleMap(
        K8sCrdTransaction txToRef,
        String identityName, String roleName
    )
    {
        txToRef.create(
            GenCrdV1_15_0.GeneratedDatabaseTables.SEC_ID_ROLE_MAP,
            GenCrdV1_15_0.createSecIdRoleMap(
                identityName,
                roleName
            )
        );
    }

    private void createSecAccessTypes(
        K8sCrdTransaction txToRef,
        String accessTypeName, int accessTypeValue
    )
    {
        txToRef.create(
            GenCrdV1_15_0.GeneratedDatabaseTables.SEC_ACCESS_TYPES,
            GenCrdV1_15_0.createSecAccessTypes(
                accessTypeName,
                (short) accessTypeValue
            )
        );
    }

    private void createSecTypeRules(
        K8sCrdTransaction txToRef,
        String domainName,
        String typeName, int accessType
    )
    {
        txToRef.create(
            GenCrdV1_15_0.GeneratedDatabaseTables.SEC_TYPE_RULES,
            GenCrdV1_15_0.createSecTypeRules(
                domainName,
                typeName,
                (short) accessType
            )
        );
    }

    private void createSecDfltRoles(
        K8sCrdTransaction txToRef,
        String identityName, String roleName
    )
    {
        txToRef.create(
            GenCrdV1_15_0.GeneratedDatabaseTables.SEC_DFLT_ROLES,
            GenCrdV1_15_0.createSecDfltRoles(
                identityName,
                roleName
            )
        );
    }

    private void createSecObjectProtection(
        K8sCrdTransaction txToRef,
        String objectPath,
        String creatorIdentityName,
        String ownerRoleName, String securityTypeName
    )
    {
        txToRef.create(
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
        K8sCrdTransaction txToRef,
        String objectPath,
        String roleName, int accessType
    )
    {
        txToRef.create(
            GenCrdV1_15_0.GeneratedDatabaseTables.SEC_ACL_MAP,
            GenCrdV1_15_0.createSecAclMap(
                objectPath,
                roleName,
                (short) accessType
            )
        );
    }

    private void createStorPoolDefinitions(
        K8sCrdTransaction txToRef,
        String uuid,
        String poolName, String poolDspName
    )
    {
        txToRef.create(
            GenCrdV1_15_0.GeneratedDatabaseTables.STOR_POOL_DEFINITIONS,
            GenCrdV1_15_0.createStorPoolDefinitions(
                uuid,
                poolName,
                poolDspName
            )
        );
    }

    private void createResourceGroups(
        K8sCrdTransaction txToRef,
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
        @Nullable String allowedProviderList, @Nullable Boolean disklessOnRemaining
    )
    {
        txToRef.create(
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
        K8sCrdTransaction txToRef,
        String propsInstance,
        String propKey, String propValue
    )
    {
        txToRef.create(
            GenCrdV1_15_0.GeneratedDatabaseTables.PROPS_CONTAINERS,
            GenCrdV1_15_0.createPropsContainers(
                propsInstance,
                propKey,
                propValue
            )
        );
    }
}
