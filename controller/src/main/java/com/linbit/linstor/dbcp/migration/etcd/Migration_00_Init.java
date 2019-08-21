package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.linstor.dbdrivers.derby.DbConstants;

import com.ibm.etcd.client.kv.KvClient;

@SuppressWarnings("checkstyle:typename")
public class Migration_00_Init
{
    public static void migrate(KvClient kvClient)
    {
        // push init values
//        kvClient.batch()
//            .put(putReq(
//                tblkey(DbConstants.TBL_SEC_CONFIGURATION, DbConstants.ENTRY_DSP_KEY, "SECURITYLEVEL"),
//                "SecurityLevel"))
//            .put(putReq(
//                tblkey(DbConstants.TBL_SEC_CONFIGURATION, DbConstants.ENTRY_VALUE, "SECURITYLEVEL"),
//                "NO_SECURITY"))
//            .put(putReq(
//                tblkey(DbConstants.TBL_SEC_CONFIGURATION, DbConstants.ENTRY_DSP_KEY, "AUTHREQUIRED"),
//                "AuthRequired"))
//            .put(putReq(
//                tblkey(DbConstants.TBL_SEC_CONFIGURATION, DbConstants.ENTRY_VALUE, "AUTHREQUIRED"),
//                "false"))
//            .put(putReq(
//                tblkey(DbConstants.TBL_SEC_IDENTITIES, DbConstants.IDENTITY_NAME, "SYSTEM"),
//                "SYSTEM"
//            ))
//            .put(putReq(
//                tblkey(DbConstants.TBL_SEC_IDENTITIES, DbConstants.IDENTITY_DSP_NAME, "SYSTEM"),
//                "SYSTEM"
//            ))
//            .put(putReq(
//                tblkey(DbConstants.TBL_SEC_IDENTITIES, DbConstants.ID_ENABLED, "SYSTEM"),
//                "TRUE"
//            ))
//            .put(putReq(
//                tblkey(DbConstants.TBL_SEC_IDENTITIES, DbConstants.ID_LOCKED, "SYSTEM"),
//                "TRUE"
//            ))
//            .put(putReq(
//                tblkey(DbConstants.TBL_SEC_ROLES, DbConstants.ROLE_NAME, "SYSTEM"),
//                "SYSTEM"
//            ))
//            .put(putReq(
//                tblkey(DbConstants.TBL_SEC_ROLES, DbConstants.ROLE_DSP_NAME, "SYSTEM"),
//                "SYSTEM"
//            ))
//            .put(putReq(
//                tblkey(DbConstants.TBL_SEC_ROLES, DbConstants.DOMAIN_NAME, "SYSTEM"),
//                "SYSTEM"
//            ))
//            .put(putReq(
//                tblkey(DbConstants.TBL_SEC_ROLES, DbConstants.ROLE_ENABLED, "SYSTEM"),
//                "TRUE"
//            ))
//            .put(putReq(
//                tblkey(DbConstants.TBL_SEC_ROLES, DbConstants.ROLE_PRIVILEGES, "SYSTEM"),
//                "-1"
//            ))
//            .put(putReq(dbhistoryVersionKey, "1"))
//            .timeout(dbTimeout)
//            .sync();
    }
}
