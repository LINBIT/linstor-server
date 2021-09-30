package com.linbit.linstor.dbcp.migration.k8s.crd;

import com.linbit.linstor.ControllerK8sCrdDatabase;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_15_0;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_15_0.Rollback;
import com.linbit.linstor.transaction.BaseControllerK8sCrdTransactionMgr;
import com.linbit.linstor.transaction.K8sCrdTransaction;

import io.fabric8.kubernetes.client.KubernetesClient;

@K8sCrdMigration(
    description = "initial data",
    version = 1
)
public class Migration_1_v1_15_0_init extends BaseK8sCrdMigration
{
    @Override
    public void migrate(ControllerK8sCrdDatabase k8sDbRef) throws Exception
    {
        BaseControllerK8sCrdTransactionMgr<GenCrdV1_15_0.Rollback> txMgr = new BaseControllerK8sCrdTransactionMgr<>(
            k8sDbRef,
            GenCrdV1_15_0.createTxMgrContext()
        );

        KubernetesClient k8sClient = k8sDbRef.getClient();
        K8sCrdTransaction<Rollback> transaction = txMgr.getTransaction();
        // load data from database that needs to change
        // noop for initial migration

        // update CRD entries for all DatabaseTables
        updateCrdSchemaForAllTables(
            k8sClient,
            GenCrdV1_15_0.createSchemaUpdateContext()
        );

        // write modified data to database
        createSecConfiguration(transaction, "SECURITYLEVEL", "SecurityLevel", "NO_SECURITY");
        createSecConfiguration(transaction, "AUTHREQUIRED", "AuthRequired", "false");
        createSecIdentities(transaction, "SYSTEM", "SYSTEM", null, null, true, true);
        createSecIdentities(transaction, "PUBLIC", "PUBLIC", null, null, true, true);
        createSecTypes(transaction, "SYSTEM", "SYSTEM", true);
        createSecTypes(transaction, "PUBLIC", "PUBLIC", true);
        createSecTypes(transaction, "SHARED", "SHARED", true);
        createSecTypes(transaction, "SYSADM", "SysAdm", true);
        createSecTypes(transaction, "USER", "User", true);
        createSecRoles(transaction, "SYSTEM", "SYSTEM", "SYSTEM", true, -1);
        createSecRoles(transaction, "PUBLIC", "PUBLIC", "PUBLIC", true, 0);
        createSecRoles(transaction, "SYSADM", "SysAdm", "SYSADM", true, -1);
        createSecRoles(transaction, "USER", "User", "USER", true, 0);
        createSecIdRoleMap(transaction, "SYSTEM", "SYSTEM");
        createSecIdRoleMap(transaction, "PUBLIC", "PUBLIC");
        createSecAccessTypes(transaction, "CONTROL", 15);
        createSecAccessTypes(transaction, "CHANGE", 7);
        createSecAccessTypes(transaction, "USE", 3);
        createSecAccessTypes(transaction, "VIEW", 1);
        createSecTypeRules(transaction, "SYSTEM", "SYSTEM", 15);
        createSecTypeRules(transaction, "SYSTEM", "PUBLIC", 15);
        createSecTypeRules(transaction, "SYSTEM", "SHARED", 15);
        createSecTypeRules(transaction, "SYSTEM", "SYSADM", 15);
        createSecTypeRules(transaction, "SYSTEM", "USER", 15);
        createSecTypeRules(transaction, "PUBLIC", "SYSTEM", 3);
        createSecTypeRules(transaction, "PUBLIC", "PUBLIC", 15);
        createSecTypeRules(transaction, "PUBLIC", "SHARED", 7);
        createSecTypeRules(transaction, "PUBLIC", "SYSADM", 3);
        createSecTypeRules(transaction, "PUBLIC", "USER", 3);
        createSecTypeRules(transaction, "SYSADM", "SYSTEM", 15);
        createSecTypeRules(transaction, "SYSADM", "PUBLIC", 15);
        createSecTypeRules(transaction, "SYSADM", "SHARED", 15);
        createSecTypeRules(transaction, "SYSADM", "SYSADM", 15);
        createSecTypeRules(transaction, "SYSADM", "USER", 15);
        createSecTypeRules(transaction, "USER", "SYSTEM", 3);
        createSecTypeRules(transaction, "USER", "PUBLIC", 7);
        createSecTypeRules(transaction, "USER", "SHARED", 7);
        createSecTypeRules(transaction, "USER", "SYSADM", 3);
        createSecTypeRules(transaction, "USER", "USER", 15);
        createSecDfltRoles(transaction, "SYSTEM", "SYSTEM");
        createSecDfltRoles(transaction, "PUBLIC", "PUBLIC");
        createSecObjectProtection(transaction, "/sys/controller/nodesMap", "SYSTEM", "SYSADM", "SHARED");
        createSecObjectProtection(transaction, "/sys/controller/rscDfnMap", "SYSTEM", "SYSADM", "SHARED");
        createSecObjectProtection(transaction, "/sys/controller/storPoolMap", "SYSTEM", "SYSADM", "SHARED");
        createSecObjectProtection(transaction, "/sys/controller/conf", "SYSTEM", "SYSADM", "SYSTEM");
        createSecObjectProtection(transaction, "/sys/controller/shutdown", "SYSTEM", "SYSADM", "SYSTEM");
        createSecObjectProtection(transaction, "/storpooldefinitions/DFLTSTORPOOL", "SYSTEM", "SYSADM", "SHARED");
        createSecObjectProtection(
            transaction,
            "/storpooldefinitions/DFLTDISKLESSSTORPOOL",
            "SYSTEM",
            "SYSADM",
            "SHARED"
        );
        createSecObjectProtection(transaction, "/resourcegroups/DFLTRSCGRP", "SYSTEM", "SYSADM", "SHARED");
        createSecObjectProtection(transaction, "/sys/controller/rscGrpMap", "SYSTEM", "SYSTEM", "SYSTEM");
        createSecObjectProtection(transaction, "/sys/controller/freeSpaceMgrMap", "SYSTEM", "SYSTEM", "SYSTEM");
        createSecObjectProtection(transaction, "/sys/controller/keyValueStoreMap", "SYSTEM", "SYSTEM", "SYSTEM");
        createSecObjectProtection(transaction, "/sys/controller/externalFileMap", "SYSTEM", "SYSTEM", "SYSTEM");
        createSecObjectProtection(transaction, "/sys/controller/remoteMap", "SYSTEM", "SYSTEM", "SYSTEM");
        createSecAclMap(transaction, "/sys/controller/nodesMap", "SYSTEM", 15);
        createSecAclMap(transaction, "/sys/controller/nodesMap", "USER", 7);
        createSecAclMap(transaction, "/sys/controller/rscDfnMap", "SYSTEM", 15);
        createSecAclMap(transaction, "/sys/controller/rscDfnMap", "USER", 7);
        createSecAclMap(transaction, "/sys/controller/storPoolMap", "SYSTEM", 15);
        createSecAclMap(transaction, "/sys/controller/storPoolMap", "USER", 7);
        createSecAclMap(transaction, "/sys/controller/conf", "SYSTEM", 15);
        createSecAclMap(transaction, "/storpooldefinitions/DFLTSTORPOOL", "PUBLIC", 7);
        createSecAclMap(transaction, "/storpooldefinitions/DFLTSTORPOOL", "USER", 7);
        createSecAclMap(transaction, "/storpooldefinitions/DFLTDISKLESSSTORPOOL", "PUBLIC", 7);
        createSecAclMap(transaction, "/storpooldefinitions/DFLTDISKLESSSTORPOOL", "USER", 7);
        createSecAclMap(transaction, "/sys/controller/nodesMap", "PUBLIC", 7);
        createSecAclMap(transaction, "/sys/controller/rscDfnMap", "PUBLIC", 7);
        createSecAclMap(transaction, "/sys/controller/storPoolMap", "PUBLIC", 7);
        createSecAclMap(transaction, "/sys/controller/conf", "PUBLIC", 1);
        createSecAclMap(transaction, "/resourcegroups/DFLTRSCGRP", "PUBLIC", 7);
        createSecAclMap(transaction, "/resourcegroups/DFLTRSCGRP", "USER", 7);
        createSecAclMap(transaction, "/sys/controller/rscGrpMap", "SYSTEM", 15);
        createSecAclMap(transaction, "/sys/controller/freeSpaceMgrMap", "SYSTEM", 15);
        createSecAclMap(transaction, "/sys/controller/keyValueStoreMap", "SYSTEM", 15);
        createSecAclMap(transaction, "/sys/controller/externalFileMap", "SYSTEM", 15);
        createSecAclMap(transaction, "/sys/controller/remoteMap", "SYSTEM", 15);
        createSecAclMap(transaction, "/sys/controller/shutdown", "SYSTEM", 15);
        createStorPoolDefinitions(transaction, "f51611c6-528f-4793-a87a-866d09e6733a", "DFLTSTORPOOL", "DfltStorPool");
        createStorPoolDefinitions(
            transaction,
            "622807eb-c8c4-44f0-b03d-a08173c8fa1b",
            "DFLTDISKLESSSTORPOOL",
            "DfltDisklessStorPool"
        );
        createResourceGroups(
            transaction,
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
        createPropsContainers(transaction, "/CTRLCFG", "defaultDebugSslConnector", "DebugSslConnector");
        createPropsContainers(transaction, "/CTRLCFG", "netcom/DebugSslConnector/bindaddress", "::0");
        createPropsContainers(transaction, "/CTRLCFG", "netcom/DebugSslConnector/enabled", "true");
        createPropsContainers(transaction, "/CTRLCFG", "netcom/DebugSslConnector/keyPasswd", "linstor");
        createPropsContainers(transaction, "/CTRLCFG", "netcom/DebugSslConnector/keyStore", "ssl/keystore.jks");
        createPropsContainers(transaction, "/CTRLCFG", "netcom/DebugSslConnector/keyStorePasswd", "linstor");
        createPropsContainers(transaction, "/CTRLCFG", "netcom/DebugSslConnector/port", "3373");
        createPropsContainers(transaction, "/CTRLCFG", "netcom/DebugSslConnector/sslProtocol", "TLSv1.2");
        createPropsContainers(transaction, "/CTRLCFG", "netcom/DebugSslConnector/trustStore", "ssl/certificates.jks");
        createPropsContainers(transaction, "/CTRLCFG", "netcom/DebugSslConnector/trustStorePasswd", "linstor");
        createPropsContainers(transaction, "/CTRLCFG", "netcom/DebugSslConnector/type", "ssl");
        createPropsContainers(transaction, "/CTRLCFG", "netcom/PlainConnector/enabled", "true");
        createPropsContainers(transaction, "/CTRLCFG", "netcom/PlainConnector/port", "3376");
        createPropsContainers(transaction, "/CTRLCFG", "netcom/PlainConnector/type", "plain");
        createPropsContainers(transaction, "/CTRLCFG", "netcom/SslConnector/enabled", "true");
        createPropsContainers(transaction, "/CTRLCFG", "netcom/SslConnector/keyPasswd", "linstor");
        createPropsContainers(transaction, "/CTRLCFG", "netcom/SslConnector/keyStore", "ssl/keystore.jks");
        createPropsContainers(transaction, "/CTRLCFG", "netcom/SslConnector/keyStorePasswd", "linstor");
        createPropsContainers(transaction, "/CTRLCFG", "netcom/SslConnector/port", "3377");
        createPropsContainers(transaction, "/CTRLCFG", "netcom/SslConnector/sslProtocol", "TLSv1.2");
        createPropsContainers(transaction, "/CTRLCFG", "netcom/SslConnector/trustStore", "ssl/certificates.jks");
        createPropsContainers(transaction, "/CTRLCFG", "netcom/SslConnector/trustStorePasswd", "linstor");
        createPropsContainers(transaction, "/CTRLCFG", "netcom/SslConnector/type", "ssl");
        createPropsContainers(transaction, "/CTRLCFG", "DrbdOptions/auto-quorum", "io-error");
        createPropsContainers(transaction, "/CTRLCFG", "DrbdOptions/auto-add-quorum-tiebreaker", "True");
        createPropsContainers(transaction, "/CTRLCFG", "netcom/PlainConnector/bindaddress", "");
        createPropsContainers(transaction, "/CTRLCFG", "netcom/SslConnector/bindaddress", "");
        createPropsContainers(transaction, "/CTRLCFG", "DrbdOptions/auto-diskful-allow-cleanup", "True");
        createPropsContainers(transaction, "/CTRLCFG", "DrbdOptions/AutoEvictAllowEviction", "True");
        createPropsContainers(
            transaction,
            "/CTRLCFG",
            "DrbdOptions/auto-verify-algo-allowed-list",
            "crct10dif-pclmul;crct10dif-generic;sha384-generic;sha512-generic;sha256-generic;md5-generic"
        );
        createPropsContainers(transaction, "/CTRLCFG", "Cluster/LocalID", "4ac9438a-ead8-4503-846b-56440ce1412a");
        createPropsContainers(transaction, "STLTCFG", "Cluster/LocalID", "4ac9438a-ead8-4503-846b-56440ce1412a");
        createPropsContainers(transaction, "/CTRLCFG", "defaultPlainConSvc", "PlainConnector");
        createPropsContainers(transaction, "/CTRLCFG", "defaultSslConSvc", "SslConnector");

        txMgr.commit();
    }

    private void createSecConfiguration(
        K8sCrdTransaction<Rollback> transactionRef,
        String entryKey,
        String entryDspKey,
        String entryValue
    )
    {
        transactionRef.update(
            GeneratedDatabaseTables.SEC_CONFIGURATION,
            GenCrdV1_15_0.createSecConfiguration(entryKey, entryDspKey, entryValue)
        );
    }

    private void createSecIdentities(
        K8sCrdTransaction<Rollback> transactionRef,
        String identityName,
        String identityDspName,
        String passSalt,
        String passHash,
        boolean idEnabled,
        boolean idLocked
    )
    {
        transactionRef.update(
            GeneratedDatabaseTables.SEC_IDENTITIES,
            GenCrdV1_15_0.createSecIdentities(identityName, identityDspName, passSalt, passHash, idEnabled, idLocked)
        );
    }

    private void createSecTypes(
        K8sCrdTransaction<Rollback> transactionRef,
        String typeName,
        String typeDspName,
        boolean typeEnabled
    )
    {
        transactionRef.update(
            GeneratedDatabaseTables.SEC_TYPES,
            GenCrdV1_15_0.createSecTypes(typeName, typeDspName, typeEnabled)
        );
    }

    private void createSecRoles(
        K8sCrdTransaction<Rollback> transactionRef,
        String roleName,
        String roleDspName,
        String domainName,
        boolean roleEnabled,
        long rolePrivileges
    )
    {
        transactionRef.update(
            GeneratedDatabaseTables.SEC_ROLES,
            GenCrdV1_15_0.createSecRoles(roleName, roleDspName, domainName, roleEnabled, rolePrivileges)
        );
    }

    private void createSecIdRoleMap(
        K8sCrdTransaction<Rollback> transactionRef,
        String identityName,
        String roleName
    )
    {
        transactionRef.update(
            GeneratedDatabaseTables.SEC_ID_ROLE_MAP,
            GenCrdV1_15_0.createSecIdRoleMap(
                identityName,
                roleName
            )
        );
    }

    private void createSecAccessTypes(
        K8sCrdTransaction<Rollback> transactionRef,
        String accessTypeName,
        int accessTypeValue
    )
    {
        transactionRef.update(
            GeneratedDatabaseTables.SEC_ACCESS_TYPES,
            GenCrdV1_15_0.createSecAccessTypes(
                accessTypeName,
                (short) accessTypeValue
            )
        );
    }

    private void createSecTypeRules(
        K8sCrdTransaction<Rollback> transactionRef,
        String domainName,
        String typeName,
        int accessType
    )
    {
        transactionRef.update(
            GeneratedDatabaseTables.SEC_TYPE_RULES,
            GenCrdV1_15_0.createSecTypeRules(
                domainName,
                typeName,
                (short) accessType
            )
        );
    }

    private void createSecDfltRoles(
        K8sCrdTransaction<Rollback> transactionRef,
        String identityName,
        String roleName
    )
    {
        transactionRef.update(
            GeneratedDatabaseTables.SEC_DFLT_ROLES,
            GenCrdV1_15_0.createSecDfltRoles(
                identityName,
                roleName
            )
        );
    }

    private void createSecObjectProtection(
        K8sCrdTransaction<Rollback> transactionRef,
        String objectPath,
        String creatorIdentityName,
        String ownerRoleName,
        String securityTypeName
    )
    {
        transactionRef.update(
            GeneratedDatabaseTables.SEC_OBJECT_PROTECTION,
            GenCrdV1_15_0.createSecObjectProtection(
                objectPath,
                creatorIdentityName,
                ownerRoleName,
                securityTypeName
            )
        );
    }

    private void createSecAclMap(
        K8sCrdTransaction<Rollback> transactionRef,
        String objectPath,
        String roleName,
        int accessType
    )
    {
        transactionRef.update(
            GeneratedDatabaseTables.SEC_ACL_MAP,
            GenCrdV1_15_0.createSecAclMap(
                objectPath,
                roleName,
                (short) accessType
            )
        );
    }

    private void createStorPoolDefinitions(
        K8sCrdTransaction<Rollback> transactionRef,
        String uuid,
        String poolName,
        String poolDspName
    )
    {
        transactionRef.update(
            GeneratedDatabaseTables.STOR_POOL_DEFINITIONS,
            GenCrdV1_15_0.createStorPoolDefinitions(
                uuid,
                poolName,
                poolDspName
            )
        );
    }

    private void createResourceGroups(
        K8sCrdTransaction<Rollback> transactionRef,
        String uuid,
        String resourceGroupName,
        String resourceGroupDspName,
        String description,
        String layerStack,
        int replicaCount,
        String nodeNameList,
        String poolName,
        String poolNameDiskless,
        String doNotPlaceWithRscRegex,
        String doNotPlaceWithRscList,
        String replicasOnSame,
        String replicasOnDifferent,
        String allowedProviderList,
        Boolean disklessOnRemaining
    )
    {
        transactionRef.update(
            GeneratedDatabaseTables.RESOURCE_GROUPS,
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
        K8sCrdTransaction<Rollback> transactionRef,
        String propsInstance,
        String propKey,
        String propValue
    )
    {
        transactionRef.update(
            GeneratedDatabaseTables.PROPS_CONTAINERS,
            GenCrdV1_15_0.createPropsContainers(
                propsInstance,
                propKey,
                propValue
            )
        );
    }
}
