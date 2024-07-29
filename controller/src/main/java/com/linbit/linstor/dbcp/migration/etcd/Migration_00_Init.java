package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.transaction.EtcdTransaction;

@SuppressWarnings("checkstyle:typename")
@EtcdMigration(
    description = "Initial data",
    version = 0
)
public class Migration_00_Init extends BaseEtcdMigration
{
    private static final String PRIMARY_KEY_DELI = ":";

    private static final String TBL_SEC_CFG = "SEC_CONFIGURATION";
    private static final String COL_SEC_CFG_ENTRY_DSP_KEY = "ENTRY_DSP_KEY";
    private static final String COL_SEC_CFG_ENTRY_VALUE = "ENTRY_VALUE";

    private static final String TBL_SEC_ID = "SEC_IDENTITIES";
    private static final String COL_SEC_ID_IDENTITY_NAME = "IDENTITY_NAME";
    private static final String COL_SEC_ID_IDENTITY_DSP_NAME = "IDENTITY_DSP_NAME";
    private static final String COL_SEC_ID_ID_ENABLED = "ID_ENABLED";
    private static final String COL_SEC_ID_ID_LOCKED = "ID_LOCKED";

    private static final String TBL_SC_TYPES = "SEC_TYPES";
    private static final String COL_SEC_TYPES_TYPE_NAME = "TYPE_NAME";
    private static final String COL_SEC_TYPES_TYPE_DSP_NAME = "TYPE_DSP_NAME";
    private static final String COL_SEC_TYPES_TYPE_ENABLED = "TYPE_ENABLED";

    private static final String TBL_SEC_ROLES = "SEC_ROLES";
    private static final String COL_SEC_ROLES_ROLE_NAME = "ROLE_NAME";
    private static final String COL_SEC_ROLES_ROLE_DSP_NAME = "ROLE_DSP_NAME";
    private static final String COL_SEC_ROLES_DOMAIN_NAME = "DOMAIN_NAME";
    private static final String COL_SEC_ROLES_ROLE_ENABLED = "ROLE_ENABLED";
    private static final String COL_SEC_ROLES_ROLE_PRIVILEGES = "ROLE_PRIVILEGES";

    private static final String TBL_SEC_ID_ROLE_MAP = "SEC_ID_ROLE_MAP";
    private static final String COL_SEC_ID_ROLE_MAP_IDENTITY_NAME = "IDENTITY_NAME";
    private static final String COL_SEC_ID_ROLE_MAP_ROLE_NAME = "ROLE_NAME";

    private static final String TBL_SEC_ACC_TYPES = "SEC_ACCESS_TYPES";
    private static final String COL_SEC_ACC_TYPES_ACCESS_TYPE_NAME = "ACCESS_TYPE_NAME";
    private static final String COL_SEC_ACC_TYPES_ACCESS_TYPE_VALUE = "ACCESS_TYPE_VALUE";

    private static final String TBL_SEC_TYPE_RULES = "SEC_TYPE_RULES";
    private static final String COL_SEC_TYPE_RULES_DOMAIN_NAME = "DOMAIN_NAME";
    private static final String COL_SEC_TYPE_RULES_TYPE_NAME = "TYPE_NAME";
    private static final String COL_SEC_TYPE_RULES_ACCESS_TYPE = "ACCESS_TYPE";

    private static final String TBL_SEC_DFLT_ROLES = "SEC_DFLT_ROLES";
    private static final String COL_SEC_DFLT_ROLES_IDENTITY_NAME = "IDENTITY_NAME";
    private static final String COL_SEC_DFLT_ROLES_ROLE_NAME = "ROLE_NAME";

    private static final String TBL_SEC_OBJ_PROT = "SEC_OBJECT_PROTECTION";
    private static final String COL_SEC_OBJ_PROT_OBJECT_PATH = "OBJECT_PATH";
    private static final String COL_SEC_OBJ_PROT_CREATOR_IDENTITY_NAME = "CREATOR_IDENTITY_NAME";
    private static final String COL_SEC_OBJ_PROT_OWNER_ROLE_NAME = "OWNER_ROLE_NAME";
    private static final String COL_SEC_OBJ_PROT_SECURITY_TYPE_NAME = "SECURITY_TYPE_NAME";

    private static final String TBL_SEC_ACL_MAP = "SEC_ACL_MAP";
    private static final String COL_SEC_ACL_MAP_OBJECT_PATH = "OBJECT_PATH";
    private static final String COL_SEC_ACL_MAP_ROLE_NAME = "ROLE_NAME";
    private static final String COL_SEC_ACL_MAP_ACCESS_TYPE = "ACCESS_TYPE";

    private static final String TBL_STOR_POOL_DFNS = "STOR_POOL_DEFINITIONS";
    private static final String COL_STOR_POOL_DFNS_UUID = "UUID";
    private static final String COL_STOR_POOL_DFNS_POOL_NAME = "POOL_NAME";
    private static final String COL_STOR_POOL_DFNS_POOL_DSP_NAME = "POOL_DSP_NAME";

    private static final String TBL_RSC_GRPS = "RESOURCE_GROUPS";
    private static final String COL_RSC_GRPS_UUID = "UUID";
    private static final String COL_RSC_GRPS_RESOURCE_GROUP_NAME = "RESOURCE_GROUP_NAME";
    private static final String COL_RSC_GRPS_RESOURCE_GROUP_DSP_NAME = "RESOURCE_GROUP_DSP_NAME";
    private static final String COL_RSC_GRPS_REPLICA_COUNT = "REPLICA_COUNT";

    private static final String TBL_PROPS_CONTAINERS = "PROPS_CONTAINERS";

    private @Nullable String etcdPrefix = null;

    private void secConfiguration(
        EtcdTransaction tx,
        String entryKey,
        String entryDspKey,
        String entryValue
    )
    {
        tx.put(buildKeyStr(TBL_SEC_CFG, COL_SEC_CFG_ENTRY_DSP_KEY, entryKey), entryDspKey);
        tx.put(buildKeyStr(TBL_SEC_CFG, COL_SEC_CFG_ENTRY_VALUE, entryKey), entryValue);
    }

    private void secIdentities(
        EtcdTransaction tx,
        String identityName,
        String identityDspName,
        boolean idEnabled,
        boolean idLocked
    )
    {
        tx.put(buildKeyStr(TBL_SEC_ID, COL_SEC_ID_IDENTITY_NAME, identityName), identityName);
        tx.put(buildKeyStr(TBL_SEC_ID, COL_SEC_ID_IDENTITY_DSP_NAME, identityName), identityDspName);
        tx.put(
            buildKeyStr(TBL_SEC_ID, COL_SEC_ID_ID_ENABLED, identityName),
            Boolean.toString(idEnabled).toUpperCase()
        );
        tx.put(
            buildKeyStr(TBL_SEC_ID, COL_SEC_ID_ID_LOCKED, identityName),
            Boolean.toString(idLocked).toUpperCase()
        );
    }

    private void secTypes(
        EtcdTransaction tx,
        String typeName,
        String typeDspName,
        boolean typeEnabled
    )
    {
        tx.put(buildKeyStr(TBL_SC_TYPES, COL_SEC_TYPES_TYPE_NAME, typeName), typeName);
        tx.put(buildKeyStr(TBL_SC_TYPES, COL_SEC_TYPES_TYPE_DSP_NAME, typeName), typeDspName);
        tx.put(
            buildKeyStr(TBL_SC_TYPES, COL_SEC_TYPES_TYPE_ENABLED, typeName),
            Boolean.toString(typeEnabled).toUpperCase()
        );
    }

    private void secRoles(
        EtcdTransaction tx,
        String roleName,
        String roleDspName,
        String domainName,
        boolean roleEnabled,
        int rolePrivileges
    )
    {
        tx.put(buildKeyStr(TBL_SEC_ROLES, COL_SEC_ROLES_ROLE_NAME, roleName), roleName);
        tx.put(buildKeyStr(TBL_SEC_ROLES, COL_SEC_ROLES_ROLE_DSP_NAME, roleName), roleDspName);
        tx.put(buildKeyStr(TBL_SEC_ROLES, COL_SEC_ROLES_DOMAIN_NAME, roleName), domainName);
        tx.put(
            buildKeyStr(TBL_SEC_ROLES, COL_SEC_ROLES_ROLE_ENABLED, roleName),
            Boolean.toString(roleEnabled).toUpperCase()
        );
        tx.put(buildKeyStr(TBL_SEC_ROLES, COL_SEC_ROLES_ROLE_PRIVILEGES, roleName), Integer.toString(rolePrivileges));
    }

    private void secIdRoleMap(
        EtcdTransaction tx,
        String identityName,
        String roleName
    )
    {
        final String pk = identityName + PRIMARY_KEY_DELI + roleName;
        tx.put(buildKeyStr(TBL_SEC_ID_ROLE_MAP, COL_SEC_ID_ROLE_MAP_IDENTITY_NAME, pk), identityName);
        tx.put(buildKeyStr(TBL_SEC_ID_ROLE_MAP, COL_SEC_ID_ROLE_MAP_ROLE_NAME, pk), roleName);
    }

    private void secAccessTypes(
        EtcdTransaction tx,
        String accessTypeName,
        int accessTypeValue
    )
    {
        tx.put(buildKeyStr(TBL_SEC_ACC_TYPES, COL_SEC_ACC_TYPES_ACCESS_TYPE_NAME, accessTypeName), accessTypeName);
        tx.put(
            buildKeyStr(TBL_SEC_ACC_TYPES, COL_SEC_ACC_TYPES_ACCESS_TYPE_VALUE, accessTypeName),
            Integer.toString(accessTypeValue)
        );
    }

    private void secTypeRules(
        EtcdTransaction tx,
        String domainName,
        String typeName,
        int accessType
    )
    {
        final String pk = domainName + PRIMARY_KEY_DELI + typeName;
        tx.put(buildKeyStr(TBL_SEC_TYPE_RULES, COL_SEC_TYPE_RULES_DOMAIN_NAME, pk), domainName);
        tx.put(buildKeyStr(TBL_SEC_TYPE_RULES, COL_SEC_TYPE_RULES_TYPE_NAME, pk), typeName);
        tx.put(buildKeyStr(TBL_SEC_TYPE_RULES, COL_SEC_TYPE_RULES_ACCESS_TYPE, pk), Integer.toString(accessType));
    }

    private void secDfltRoles(
        EtcdTransaction tx,
        String identityName,
        String roleName
    )
    {
        final String pk = identityName;
        tx.put(buildKeyStr(TBL_SEC_DFLT_ROLES, COL_SEC_DFLT_ROLES_IDENTITY_NAME, pk), identityName);
        tx.put(buildKeyStr(TBL_SEC_DFLT_ROLES, COL_SEC_DFLT_ROLES_ROLE_NAME, pk), roleName);
    }

    private void secObjectProtection(
        EtcdTransaction tx,
        String objectPath,
        String creatorIdentityName,
        String ownerRoleName,
        String securityTypeName
    )
    {
        final String pk = objectPath;
        tx.put(buildKeyStr(TBL_SEC_OBJ_PROT, COL_SEC_OBJ_PROT_OBJECT_PATH, pk), objectPath);
        tx.put(buildKeyStr(TBL_SEC_OBJ_PROT, COL_SEC_OBJ_PROT_CREATOR_IDENTITY_NAME, pk), creatorIdentityName);
        tx.put(buildKeyStr(TBL_SEC_OBJ_PROT, COL_SEC_OBJ_PROT_OWNER_ROLE_NAME, pk), ownerRoleName);
        tx.put(buildKeyStr(TBL_SEC_OBJ_PROT, COL_SEC_OBJ_PROT_SECURITY_TYPE_NAME, pk), securityTypeName);
    }

    private void secAclMap(
        EtcdTransaction tx,
        String objectPath,
        String roleName,
        int accessType
    )
    {
        final String pk = objectPath + PRIMARY_KEY_DELI + roleName;
        tx.put(buildKeyStr(TBL_SEC_ACL_MAP, COL_SEC_ACL_MAP_OBJECT_PATH, pk), objectPath);
        tx.put(buildKeyStr(TBL_SEC_ACL_MAP, COL_SEC_ACL_MAP_ROLE_NAME, pk), roleName);
        tx.put(buildKeyStr(TBL_SEC_ACL_MAP, COL_SEC_ACL_MAP_ACCESS_TYPE, pk), Integer.toString(accessType));
    }

    private void storPoolDefinitions(
        EtcdTransaction tx,
        String uuid,
        String poolName,
        String poolDspName
    )
    {
        final String pk = poolName;
        tx.put(buildKeyStr(TBL_STOR_POOL_DFNS, COL_STOR_POOL_DFNS_UUID, pk), uuid);
        tx.put(buildKeyStr(TBL_STOR_POOL_DFNS, COL_STOR_POOL_DFNS_POOL_NAME, pk), poolName);
        tx.put(buildKeyStr(TBL_STOR_POOL_DFNS, COL_STOR_POOL_DFNS_POOL_DSP_NAME, pk), poolDspName);
    }

    private void resourceGroups(
        EtcdTransaction tx,
        String uuid,
        String resourceGroupName,
        String resourceGroupDspName
    )
    {
        final String pk = resourceGroupName;
        tx.put(buildKeyStr(TBL_RSC_GRPS, COL_RSC_GRPS_UUID, pk), uuid);
        tx.put(buildKeyStr(TBL_RSC_GRPS, COL_RSC_GRPS_RESOURCE_GROUP_NAME, pk), resourceGroupName);
        tx.put(buildKeyStr(TBL_RSC_GRPS, COL_RSC_GRPS_RESOURCE_GROUP_DSP_NAME, pk), resourceGroupDspName);
        tx.put(buildKeyStr(TBL_RSC_GRPS, COL_RSC_GRPS_REPLICA_COUNT, pk), "2");
    }

    private void propsContainers(
        EtcdTransaction tx,
        String propsInstance,
        String propKey,
        String propValue
    )
    {
        tx.put(
            etcdPrefix +
                TBL_PROPS_CONTAINERS + "/" +
            propsInstance + PRIMARY_KEY_DELI + propKey,
            propValue
        );
    }

    @Override
    public void migrate(EtcdTransaction tx, final String prefix)
    {
        etcdPrefix = prefix;

        // push init values
        secConfiguration(tx, "SECURITYLEVEL", "SecurityLevel", "NO_SECURITY");
        secConfiguration(tx, "AUTHREQUIRED", "AuthRequired", "false");

        secIdentities(tx, "SYSTEM", "SYSTEM", true, true);
        secIdentities(tx, "PUBLIC", "PUBLIC", true, true);

        secTypes(tx, "SYSTEM", "SYSTEM", true);
        secTypes(tx, "PUBLIC", "PUBLIC", true);
        secTypes(tx, "SHARED", "SHARED", true);
        secTypes(tx, "SYSADM", "SysAdm", true);
        secTypes(tx, "USER", "User", true);

        secRoles(tx, "SYSTEM", "SYSTEM", "SYSTEM", true, -1);
        secRoles(tx, "PUBLIC", "PUBLIC", "PUBLIC", true, 0);
        secRoles(tx, "SYSADM", "SysAdm", "SYSADM", true, -1);
        secRoles(tx, "USER", "User", "USER", true, 0);

        secIdRoleMap(tx, "SYSTEM", "SYSTEM");
        secIdRoleMap(tx, "PUBLIC", "PUBLIC");

        secAccessTypes(tx, "CONTROL", 15);
        secAccessTypes(tx, "CHANGE", 7);
        secAccessTypes(tx, "USE", 3);
        secAccessTypes(tx, "VIEW", 1);

        secTypeRules(tx, "SYSTEM", "SYSTEM", 15);
        secTypeRules(tx, "SYSTEM", "PUBLIC", 15);
        secTypeRules(tx, "SYSTEM", "SHARED", 15);
        secTypeRules(tx, "SYSTEM", "SYSADM", 15);
        secTypeRules(tx, "SYSTEM", "USER", 15);
        secTypeRules(tx, "PUBLIC", "SYSTEM", 3);
        secTypeRules(tx, "PUBLIC", "PUBLIC", 15);
        secTypeRules(tx, "PUBLIC", "SHARED", 7);
        secTypeRules(tx, "PUBLIC", "SYSADM", 3);
        secTypeRules(tx, "PUBLIC", "USER", 3);
        secTypeRules(tx, "SYSADM", "SYSTEM", 15);
        secTypeRules(tx, "SYSADM", "PUBLIC", 15);
        secTypeRules(tx, "SYSADM", "SHARED", 15);
        secTypeRules(tx, "SYSADM", "SYSADM", 15);
        secTypeRules(tx, "SYSADM", "USER", 15);
        secTypeRules(tx, "USER", "SYSTEM", 3);
        secTypeRules(tx, "USER", "PUBLIC", 7);
        secTypeRules(tx, "USER", "SHARED", 7);
        secTypeRules(tx, "USER", "SYSADM", 3);
        secTypeRules(tx, "USER", "USER", 15);

        secDfltRoles(tx, "SYSTEM", "SYSTEM");
        secDfltRoles(tx, "PUBLIC", "PUBLIC");

        secObjectProtection(tx, "/sys/controller/nodesMap", "SYSTEM", "SYSADM", "SHARED");
        secObjectProtection(tx, "/sys/controller/rscDfnMap", "SYSTEM", "SYSADM", "SHARED");
        secObjectProtection(tx, "/sys/controller/storPoolMap", "SYSTEM", "SYSADM", "SHARED");
        secObjectProtection(tx, "/sys/controller/conf", "SYSTEM", "SYSADM", "SYSTEM");
        secObjectProtection(tx, "/sys/controller/shutdown", "SYSTEM", "SYSADM", "SYSTEM");
        secObjectProtection(tx, "/storpooldefinitions/DFLTSTORPOOL", "SYSTEM", "SYSADM", "SHARED");
        secObjectProtection(tx, "/storpooldefinitions/DFLTDISKLESSSTORPOOL", "SYSTEM", "SYSADM", "SHARED");
        secObjectProtection(tx, "/resourcegroups/DFLTRSCGRP", "SYSTEM", "SYSADM", "SHARED");
        secObjectProtection(tx, "/sys/controller/rscGrpMap", "SYSTEM", "SYSTEM", "SYSTEM");
        secObjectProtection(tx, "/sys/controller/freeSpaceMgrMap", "SYSTEM", "SYSTEM", "SYSTEM");
        secObjectProtection(tx, "/sys/controller/keyValueStoreMap", "SYSTEM", "SYSTEM", "SYSTEM");

        secAclMap(tx, "/sys/controller/nodesMap", "SYSTEM", 15);
        secAclMap(tx, "/sys/controller/nodesMap", "USER", 7);
        secAclMap(tx, "/sys/controller/rscDfnMap", "SYSTEM", 15);
        secAclMap(tx, "/sys/controller/rscDfnMap", "USER", 7);
        secAclMap(tx, "/sys/controller/storPoolMap", "SYSTEM", 15);
        secAclMap(tx, "/sys/controller/storPoolMap", "USER", 7);
        secAclMap(tx, "/sys/controller/conf", "SYSTEM", 15);
        secAclMap(tx, "/storpooldefinitions/DFLTSTORPOOL", "PUBLIC", 7);
        secAclMap(tx, "/storpooldefinitions/DFLTSTORPOOL", "USER", 7);
        secAclMap(tx, "/storpooldefinitions/DFLTDISKLESSSTORPOOL", "PUBLIC", 7);
        secAclMap(tx, "/storpooldefinitions/DFLTDISKLESSSTORPOOL", "USER", 7);
        secAclMap(tx, "/sys/controller/nodesMap", "PUBLIC", 7);
        secAclMap(tx, "/sys/controller/rscDfnMap", "PUBLIC", 7);
        secAclMap(tx, "/sys/controller/storPoolMap", "PUBLIC", 7);
        secAclMap(tx, "/sys/controller/conf", "PUBLIC", 1);
        secAclMap(tx, "/resourcegroups/DFLTRSCGRP", "PUBLIC", 7);
        secAclMap(tx, "/resourcegroups/DFLTRSCGRP", "USER", 7);
        secAclMap(tx, "/sys/controller/rscGrpMap", "SYSTEM", 15);
        secAclMap(tx, "/sys/controller/freeSpaceMgrMap", "SYSTEM", 15);
        secAclMap(tx, "/sys/controller/keyValueStoreMap", "SYSTEM", 15);
        secAclMap(tx, "/sys/controller/shutdown", "SYSTEM", 15);

        storPoolDefinitions(tx, "f51611c6-528f-4793-a87a-866d09e6733a", "DFLTSTORPOOL", "DfltStorPool");
        storPoolDefinitions(tx, "622807eb-c8c4-44f0-b03d-a08173c8fa1b", "DFLTDISKLESSSTORPOOL", "DfltDisklessStorPool");

        resourceGroups(tx, "a52e934a-9fd9-44cb-9db1-716dcd13aae3", "DFLTRSCGRP", "DfltRscGrp");

        propsContainers(tx, "/CTRLCFG", "defaultDebugSslConnector", "DebugSslConnector");
        propsContainers(tx, "/CTRLCFG", "netcom/DebugSslConnector/bindaddress", "::0");
        propsContainers(tx, "/CTRLCFG", "netcom/DebugSslConnector/enabled", "true");
        propsContainers(tx, "/CTRLCFG", "netcom/DebugSslConnector/keyPasswd", "linstor");
        propsContainers(tx, "/CTRLCFG", "netcom/DebugSslConnector/keyStore", "ssl/keystore.jks");
        propsContainers(tx, "/CTRLCFG", "netcom/DebugSslConnector/keyStorePasswd", "linstor");
        propsContainers(tx, "/CTRLCFG", "netcom/DebugSslConnector/port", "3373");
        propsContainers(tx, "/CTRLCFG", "netcom/DebugSslConnector/sslProtocol", "TLSv1.2");
        propsContainers(tx, "/CTRLCFG", "netcom/DebugSslConnector/trustStore", "ssl/certificates.jks");
        propsContainers(tx, "/CTRLCFG", "netcom/DebugSslConnector/trustStorePasswd", "linstor");
        propsContainers(tx, "/CTRLCFG", "netcom/DebugSslConnector/type", "ssl");
        propsContainers(tx, "/CTRLCFG", "netcom/PlainConnector/bindaddress", "::0");
        propsContainers(tx, "/CTRLCFG", "netcom/PlainConnector/enabled", "true");
        propsContainers(tx, "/CTRLCFG", "netcom/PlainConnector/port", "3376");
        propsContainers(tx, "/CTRLCFG", "netcom/PlainConnector/type", "plain");
        propsContainers(tx, "/CTRLCFG", "netcom/SslConnector/bindaddress", "::0");
        propsContainers(tx, "/CTRLCFG", "netcom/SslConnector/enabled", "true");
        propsContainers(tx, "/CTRLCFG", "netcom/SslConnector/keyPasswd", "linstor");
        propsContainers(tx, "/CTRLCFG", "netcom/SslConnector/keyStore", "ssl/keystore.jks");
        propsContainers(tx, "/CTRLCFG", "netcom/SslConnector/keyStorePasswd", "linstor");
        propsContainers(tx, "/CTRLCFG", "netcom/SslConnector/port", "3377");
        propsContainers(tx, "/CTRLCFG", "netcom/SslConnector/sslProtocol", "TLSv1.2");
        propsContainers(tx, "/CTRLCFG", "netcom/SslConnector/trustStore", "ssl/certificates.jks");
        propsContainers(tx, "/CTRLCFG", "netcom/SslConnector/trustStorePasswd", "linstor");
        propsContainers(tx, "/CTRLCFG", "netcom/SslConnector/type", "ssl");
        propsContainers(tx, "/CTRLCFG", "defaultPlainConSvc", "PlainConnector");
        propsContainers(tx, "/CTRLCFG", "defaultSslConSvc", "SslConnector");

        propsContainers(tx, "/CTRLCFG", "DrbdOptions/auto-quorum", "io-error");
        propsContainers(tx, "/CTRLCFG", "DrbdOptions/auto-add-quorum-tiebreaker", "True");

        tx.put(prefix + "DBHISTORY/version", "34");
    }

    @Override
    public int getNextVersion()
    {
        return 35;
    }
}
