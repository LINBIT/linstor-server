package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.ResourceGroups;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecAccessTypes;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecAclMap;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecConfiguration;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecDfltRoles;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecIdRoleMap;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecIdentities;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecObjectProtection;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecRoles;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecTypeRules;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecTypes;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.StorPoolDefinitions;
import com.linbit.linstor.dbdrivers.etcd.EtcdUtils;

import com.ibm.etcd.api.TxnResponse;
import com.ibm.etcd.client.kv.KvClient;

@SuppressWarnings("checkstyle:typename")
public class Migration_00_Init extends EtcdMigration
{
    private static final String PRIMARY_KEY_DELI = ":";

    private static KvClient.FluentTxnOps<?> secConfiguration(
        KvClient.FluentTxnOps<?> txn,
        String entryKey,
        String entryDspKey,
        String entryValue
    )
    {
        return txn
            .put(putReq(
                tblKey(SecConfiguration.ENTRY_DSP_KEY, entryKey),
                entryDspKey))
            .put(putReq(
                tblKey(SecConfiguration.ENTRY_VALUE, entryKey),
                entryValue));
    }

    private static KvClient.FluentTxnOps<?> secIdentities(
        KvClient.FluentTxnOps<?> txn,
        String identityName,
        String identityDspName,
        boolean idEnabled,
        boolean idLocked
    )
    {
        return txn
            .put(putReq(
                tblKey(SecIdentities.IDENTITY_NAME, identityName),
                identityName
            ))
            .put(putReq(
                tblKey(SecIdentities.IDENTITY_DSP_NAME, identityName),
                identityDspName
            ))
            .put(putReq(
                tblKey(SecIdentities.ID_ENABLED, identityName),
                Boolean.toString(idEnabled).toUpperCase()
            ))
            .put(putReq(
                tblKey(SecIdentities.ID_LOCKED, identityName),
                Boolean.toString(idLocked).toUpperCase()
            ));
    }

    private static KvClient.FluentTxnOps<?> secTypes(
        KvClient.FluentTxnOps<?> txn,
        String typeName,
        String typeDspName,
        boolean typeEnabled
    )
    {
        return txn
            .put(putReq(
                tblKey(SecTypes.TYPE_NAME, typeName),
                typeName
            ))
            .put(putReq(
                tblKey(SecTypes.TYPE_DSP_NAME, typeName),
                typeDspName
            ))
            .put(putReq(
                tblKey(SecTypes.TYPE_ENABLED, typeName),
                Boolean.toString(typeEnabled).toUpperCase()
            )
        );
    }

    private static KvClient.FluentTxnOps<?> secRoles(
        KvClient.FluentTxnOps<?> txn,
        String roleName,
        String roleDspName,
        String domainName,
        boolean roleEnabled,
        int rolePrivileges
    )
    {
        return txn
            .put(putReq(
                tblKey(SecRoles.ROLE_NAME, roleName),
                roleName
            ))
            .put(putReq(
                tblKey(SecRoles.ROLE_DSP_NAME, roleName),
                roleDspName
            ))
            .put(putReq(
                tblKey(SecRoles.DOMAIN_NAME, roleName),
                domainName
            ))
            .put(putReq(
                tblKey(SecRoles.ROLE_ENABLED, roleName),
                Boolean.toString(roleEnabled).toUpperCase()
            ))
            .put(putReq(tblKey(SecRoles.ROLE_PRIVILEGES, roleName),
                Integer.toString(rolePrivileges)
            ));
    }

    private static KvClient.FluentTxnOps<?> secIdRoleMap(
        KvClient.FluentTxnOps<?> txn,
        String identityName,
        String roleName
    )
    {
        final String pk = identityName + PRIMARY_KEY_DELI + roleName;
        return txn
            .put(putReq(
                tblKey(SecIdRoleMap.IDENTITY_NAME, pk),
                identityName
            ))
            .put(putReq(
                tblKey(SecIdRoleMap.ROLE_NAME, pk),
                roleName
            ));
    }

    private static KvClient.FluentTxnOps<?> secAccessTypes(
        KvClient.FluentTxnOps<?> txn,
        String accessTypeName,
        int accessTypeValue
    )
    {
        return txn
            .put(putReq(
                tblKey(SecAccessTypes.ACCESS_TYPE_NAME, accessTypeName),
                accessTypeName
            ))
            .put(putReq(tblKey(SecAccessTypes.ACCESS_TYPE_VALUE, accessTypeName),
                Integer.toString(accessTypeValue)
            ));
    }

    private static KvClient.FluentTxnOps<?> secTypeRules(
        KvClient.FluentTxnOps<?> txn,
        String domainName,
        String typeName,
        int accessType
    )
    {
        final String pk = domainName + PRIMARY_KEY_DELI + typeName;
        return txn
            .put(putReq(
                tblKey(SecTypeRules.DOMAIN_NAME, pk),
                domainName
            ))
            .put(putReq(
                tblKey(SecTypeRules.TYPE_NAME, pk),
                typeName
            ))
            .put(putReq(tblKey(SecTypeRules.ACCESS_TYPE, pk),
                Integer.toString(accessType)
            ));
    }

    private static KvClient.FluentTxnOps<?> secDfltRoles(
        KvClient.FluentTxnOps<?> txn,
        String identityName,
        String roleName
    )
    {
        final String pk = identityName;
        return txn
            .put(putReq(
                tblKey(SecDfltRoles.IDENTITY_NAME, pk),
                identityName
            ))
            .put(putReq(
                tblKey(SecDfltRoles.ROLE_NAME, pk),
                roleName
            ));
    }

    private static KvClient.FluentTxnOps<?> secObjectProtection(
        KvClient.FluentTxnOps<?> txn,
        String objectPath,
        String creatorIdentityName,
        String ownerRoleName,
        String securityTypeName
    )
    {
        final String pk = objectPath;
        return txn
            .put(putReq(
                tblKey(SecObjectProtection.OBJECT_PATH, pk),
                objectPath
            ))
            .put(putReq(
                tblKey(SecObjectProtection.CREATOR_IDENTITY_NAME, pk),
                creatorIdentityName
            ))
            .put(putReq(
                tblKey(SecObjectProtection.OWNER_ROLE_NAME, pk),
                ownerRoleName
            ))
            .put(putReq(
                tblKey(SecObjectProtection.SECURITY_TYPE_NAME, pk),
                securityTypeName
            ));
    }

    private static KvClient.FluentTxnOps<?> secAclMap(
        KvClient.FluentTxnOps<?> txn,
        String objectPath,
        String roleName,
        int accessType
    )
    {
        final String pk = objectPath + PRIMARY_KEY_DELI + roleName;
        return txn
            .put(putReq(
                tblKey(SecAclMap.OBJECT_PATH, pk),
                objectPath
            ))
            .put(putReq(
                tblKey(SecAclMap.ROLE_NAME, pk),
                roleName
            ))
            .put(putReq(tblKey(SecAclMap.ACCESS_TYPE, pk),
                Integer.toString(accessType)
            ));
    }

    private static KvClient.FluentTxnOps<?> storPoolDefinitions(
        KvClient.FluentTxnOps<?> txn,
        String uuid,
        String poolName,
        String poolDspName
    )
    {
        final String pk = poolName;
        return txn
            .put(putReq(
                tblKey(StorPoolDefinitions.UUID, pk),
                uuid
            ))
            .put(putReq(
                tblKey(StorPoolDefinitions.POOL_NAME, pk),
                poolName
            ))
            .put(putReq(
                tblKey(StorPoolDefinitions.POOL_DSP_NAME, pk),
                poolDspName
            ));
    }

    private static KvClient.FluentTxnOps<?> resourceGroups(
        KvClient.FluentTxnOps<?> txn,
        String uuid,
        String resourceGroupName,
        String resourceGroupDspName
    )
    {
        final String pk = resourceGroupName;
        return txn
            .put(putReq(
                tblKey(ResourceGroups.UUID, pk),
                uuid
            ))
            .put(putReq(
                tblKey(ResourceGroups.RESOURCE_GROUP_NAME, pk),
                resourceGroupName
            ))
            .put(putReq(
                tblKey(ResourceGroups.RESOURCE_GROUP_DSP_NAME, pk),
                resourceGroupDspName
            ));
    }

    private static KvClient.FluentTxnOps<?> propsContainers(
        KvClient.FluentTxnOps<?> txn,
        String propsInstance,
        String propKey,
        String propValue
    )
    {
        return txn
            .put(putReq(
                GeneratedDatabaseTables.DATABASE_SCHEMA_NAME +
                    "/" + GeneratedDatabaseTables.PROPS_CONTAINERS.getName() + "/" +
                    propsInstance + PRIMARY_KEY_DELI + propKey,
                propValue
            ));
    }

    public static TxnResponse migrate(KvClient kvClient)
    {
        // push init values
        KvClient.FluentTxnOps<?> txn = kvClient.batch();
        secConfiguration(txn, "SECURITYLEVEL", "SecurityLevel", "NO_SECURITY");
        secConfiguration(txn, "AUTHREQUIRED", "AuthRequired", "false");

        secIdentities(txn, "SYSTEM", "SYSTEM", true, true);
        secIdentities(txn, "PUBLIC", "PUBLIC", true, true);

        secTypes(txn, "SYSTEM", "SYSTEM", true);
        secTypes(txn, "PUBLIC", "PUBLIC", true);
        secTypes(txn, "SHARED", "SHARED", true);
        secTypes(txn, "SYSADM", "SysAdm", true);
        secTypes(txn, "USER", "User", true);

        secRoles(txn, "SYSTEM", "SYSTEM", "SYSTEM", true, -1);
        secRoles(txn, "PUBLIC", "PUBLIC", "PUBLIC", true, 0);
        secRoles(txn, "SYSADM", "SysAdm", "SYSADM", true, -1);
        secRoles(txn, "USER", "User", "USER", true, 0);

        secIdRoleMap(txn, "SYSTEM", "SYSTEM");
        secIdRoleMap(txn, "PUBLIC", "PUBLIC");

        secAccessTypes(txn, "CONTROL", 15);
        secAccessTypes(txn, "CHANGE", 7);
        secAccessTypes(txn, "USE", 3);
        secAccessTypes(txn, "VIEW", 1);

        secTypeRules(txn, "SYSTEM", "SYSTEM", 15);
        secTypeRules(txn, "SYSTEM", "PUBLIC", 15);
        secTypeRules(txn, "SYSTEM", "SHARED", 15);
        secTypeRules(txn, "SYSTEM", "SYSADM", 15);
        secTypeRules(txn, "SYSTEM", "USER", 15);
        secTypeRules(txn, "PUBLIC", "SYSTEM", 3);
        secTypeRules(txn, "PUBLIC", "PUBLIC", 15);
        secTypeRules(txn, "PUBLIC", "SHARED", 7);
        secTypeRules(txn, "PUBLIC", "SYSADM", 3);
        secTypeRules(txn, "PUBLIC", "USER", 3);
        secTypeRules(txn, "SYSADM", "SYSTEM", 15);
        secTypeRules(txn, "SYSADM", "PUBLIC", 15);
        secTypeRules(txn, "SYSADM", "SHARED", 15);
        secTypeRules(txn, "SYSADM", "SYSADM", 15);
        secTypeRules(txn, "SYSADM", "USER", 15);
        secTypeRules(txn, "USER", "SYSTEM", 3);
        secTypeRules(txn, "USER", "PUBLIC", 7);
        secTypeRules(txn, "USER", "SHARED", 7);
        secTypeRules(txn, "USER", "SYSADM", 3);
        secTypeRules(txn, "USER", "USER", 15);

        secDfltRoles(txn, "SYSTEM", "SYSTEM");
        secDfltRoles(txn, "PUBLIC", "PUBLIC");

        secObjectProtection(txn, "/sys/controller/nodesMap", "SYSTEM", "SYSADM", "SHARED");
        secObjectProtection(txn, "/sys/controller/rscDfnMap", "SYSTEM", "SYSADM", "SHARED");
        secObjectProtection(txn, "/sys/controller/storPoolMap", "SYSTEM", "SYSADM", "SHARED");
        secObjectProtection(txn, "/sys/controller/conf", "SYSTEM", "SYSADM", "SYSTEM");                                                                                                                                                          secObjectProtection(txn, "/sys/controller/shutdown", "SYSTEM", "SYSADM", "SYSTEM");
        secObjectProtection(txn, "/storpooldefinitions/DFLTSTORPOOL", "SYSTEM", "SYSADM", "SHARED");
        secObjectProtection(txn, "/storpooldefinitions/DFLTDISKLESSSTORPOOL", "SYSTEM", "SYSADM", "SHARED");
        secObjectProtection(txn, "/resourcegroups/DFLTRSCGRP", "SYSTEM", "SYSADM", "SHARED");
        secObjectProtection(txn, "/sys/controller/rscGrpMap", "SYSTEM", "SYSTEM", "SYSTEM");
        secObjectProtection(txn, "/sys/controller/freeSpaceMgrMap", "SYSTEM", "SYSTEM", "SYSTEM");
        secObjectProtection(txn, "/sys/controller/keyValueStoreMap", "SYSTEM", "SYSTEM", "SYSTEM");

        secAclMap(txn, "/sys/controller/nodesMap", "SYSTEM", 15);
        secAclMap(txn, "/sys/controller/nodesMap", "USER", 7);
        secAclMap(txn, "/sys/controller/rscDfnMap", "SYSTEM", 15);
        secAclMap(txn, "/sys/controller/rscDfnMap", "USER", 7);
        secAclMap(txn, "/sys/controller/storPoolMap", "SYSTEM", 15);
        secAclMap(txn, "/sys/controller/storPoolMap", "USER", 7);
        secAclMap(txn, "/sys/controller/conf", "SYSTEM", 15);
        secAclMap(txn, "/storpooldefinitions/DFLTSTORPOOL", "PUBLIC", 7);
        secAclMap(txn, "/storpooldefinitions/DFLTSTORPOOL", "USER", 7);
        secAclMap(txn, "/storpooldefinitions/DFLTDISKLESSSTORPOOL", "PUBLIC", 7);
        secAclMap(txn, "/storpooldefinitions/DFLTDISKLESSSTORPOOL", "USER", 7);
        secAclMap(txn, "/sys/controller/nodesMap", "PUBLIC", 7);
        secAclMap(txn, "/sys/controller/rscDfnMap", "PUBLIC", 7);
        secAclMap(txn, "/sys/controller/storPoolMap", "PUBLIC", 7);
        secAclMap(txn, "/sys/controller/conf", "PUBLIC", 1);
        secAclMap(txn, "/resourcegroups/DFLTRSCGRP", "PUBLIC", 7);
        secAclMap(txn, "/resourcegroups/DFLTRSCGRP", "USER", 7);
        secAclMap(txn, "/sys/controller/rscGrpMap", "SYSTEM", 15);
        secAclMap(txn, "/sys/controller/freeSpaceMgrMap", "SYSTEM", 15);
        secAclMap(txn, "/sys/controller/keyValueStoreMap", "SYSTEM", 15);
        secAclMap(txn, "/sys/controller/shutdown", "SYSTEM", 15);

        storPoolDefinitions(txn, "f51611c6-528f-4793-a87a-866d09e6733a", "DFLTSTORPOOL", "DfltStorPool");
        storPoolDefinitions(txn, "622807eb-c8c4-44f0-b03d-a08173c8fa1b", "DFLTDISKLESSSTORPOOL", "DfltDisklessStorPool");

        resourceGroups(txn, "a52e934a-9fd9-44cb-9db1-716dcd13aae3", "DFLTRSCGRP", "DfltRscGrp");

        propsContainers(txn, "/CTRLCFG", "defaultDebugSslConnector", "DebugSslConnector");
        propsContainers(txn, "/CTRLCFG", "netcom/DebugSslConnector/bindaddress", "::0");
        propsContainers(txn, "/CTRLCFG", "netcom/DebugSslConnector/enabled", "true");
        propsContainers(txn, "/CTRLCFG", "netcom/DebugSslConnector/keyPasswd", "linstor");
        propsContainers(txn, "/CTRLCFG", "netcom/DebugSslConnector/keyStore", "ssl/keystore.jks");
        propsContainers(txn, "/CTRLCFG", "netcom/DebugSslConnector/keyStorePasswd", "linstor");
        propsContainers(txn, "/CTRLCFG", "netcom/DebugSslConnector/port", "3373");
        propsContainers(txn, "/CTRLCFG", "netcom/DebugSslConnector/sslProtocol", "TLSv1.2");
        propsContainers(txn, "/CTRLCFG", "netcom/DebugSslConnector/trustStore", "ssl/certificates.jks");
        propsContainers(txn, "/CTRLCFG", "netcom/DebugSslConnector/trustStorePasswd", "linstor");
        propsContainers(txn, "/CTRLCFG", "netcom/DebugSslConnector/type", "ssl");
        propsContainers(txn, "/CTRLCFG", "netcom/PlainConnector/bindaddress", "::0");
        propsContainers(txn, "/CTRLCFG", "netcom/PlainConnector/enabled", "true");
        propsContainers(txn, "/CTRLCFG", "netcom/PlainConnector/port", "3376");
        propsContainers(txn, "/CTRLCFG", "netcom/PlainConnector/type", "plain");
        propsContainers(txn, "/CTRLCFG", "netcom/SslConnector/bindaddress", "::0");
        propsContainers(txn, "/CTRLCFG", "netcom/SslConnector/enabled", "true");
        propsContainers(txn, "/CTRLCFG", "netcom/SslConnector/keyPasswd", "linstor");
        propsContainers(txn, "/CTRLCFG", "netcom/SslConnector/keyStore", "ssl/keystore.jks");
        propsContainers(txn, "/CTRLCFG", "netcom/SslConnector/keyStorePasswd", "linstor");
        propsContainers(txn, "/CTRLCFG", "netcom/SslConnector/port", "3377");
        propsContainers(txn, "/CTRLCFG", "netcom/SslConnector/sslProtocol", "TLSv1.2");
        propsContainers(txn, "/CTRLCFG", "netcom/SslConnector/trustStore", "ssl/certificates.jks");
        propsContainers(txn, "/CTRLCFG", "netcom/SslConnector/trustStorePasswd", "linstor");
        propsContainers(txn, "/CTRLCFG", "netcom/SslConnector/type", "ssl");
        propsContainers(txn, "/CTRLCFG", "defaultPlainConSvc", "PlainConnector");
        propsContainers(txn, "/CTRLCFG", "defaultSslConSvc", "SslConnector");

        String dbhistoryVersionKey = EtcdUtils.LINSTOR_PREFIX + "DBHISTORY/version";
        return txn
            .put(putReq(dbhistoryVersionKey, "1"))
            .sync();
    }
}
