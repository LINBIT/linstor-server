package com.linbit.linstor.api;

import com.linbit.linstor.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ApiRcUtils
{
    public static final long MASK_TYPE   = 0xC000000000000000L;
    public static final long MASK_OP     = 0x0000000003000000L;
    public static final long MASK_OBJ    = 0x00000000003C0000L;
    public static final long MASK_ACTION = 0xC00000000000FFFFL;

    public static final Map<Long, String> RET_CODES_TYPE;
    public static final Map<Long, String> RET_CODES_OP;
    public static final Map<Long, String> RET_CODES_OBJ;
    public static final Map<Long, String> RET_CODES_ACTION;

    static
    {
        HashMap<Long, String> tmpMap = new HashMap<>();
        tmpMap.put(ApiConsts.MASK_ERROR, "Error");
        tmpMap.put(ApiConsts.MASK_WARN, "Warn");
        tmpMap.put(ApiConsts.MASK_INFO, "Info");
        tmpMap.put(ApiConsts.MASK_SUCCESS, "Success");
        RET_CODES_TYPE = Collections.unmodifiableMap(tmpMap);

        tmpMap = new HashMap<>();
        tmpMap.put(ApiConsts.MASK_CRT, "Create");
        tmpMap.put(ApiConsts.MASK_MOD, "Modify");
        tmpMap.put(ApiConsts.MASK_DEL, "Delete");
        RET_CODES_OP = Collections.unmodifiableMap(tmpMap);

        tmpMap = new HashMap<>();
        tmpMap.put(ApiConsts.MASK_NODE, "Node");
        tmpMap.put(ApiConsts.MASK_RSC_DFN, "RscDfn");
        tmpMap.put(ApiConsts.MASK_RSC, "Rsc");
        tmpMap.put(ApiConsts.MASK_VLM_DFN, "VlmDfn");
        tmpMap.put(ApiConsts.MASK_VLM, "Vlm");
        tmpMap.put(ApiConsts.MASK_STOR_POOL_DFN, "StorPoolDfn");
        tmpMap.put(ApiConsts.MASK_STOR_POOL, "StorPool");
        tmpMap.put(ApiConsts.MASK_NODE_CONN, "NodeConn");
        tmpMap.put(ApiConsts.MASK_RSC_CONN, "RscConn");
        tmpMap.put(ApiConsts.MASK_VLM_CONN, "VlmConn");
        tmpMap.put(ApiConsts.MASK_NET_IF, "NetIf");
        tmpMap.put(ApiConsts.MASK_CTRL_CONF, "ControllerConf");
        tmpMap.put(ApiConsts.MASK_KVS, "Kvs");
        tmpMap.put(ApiConsts.MASK_RSC_GRP, "RscGrp");
        tmpMap.put(ApiConsts.MASK_SNAPSHOT, "Snapshot");
        tmpMap.put(ApiConsts.MASK_STOR_POOL_DFN, "SnapshotDfn");
        tmpMap.put(ApiConsts.MASK_VLM_GRP, "VlmGrp");
        RET_CODES_OBJ = Collections.unmodifiableMap(tmpMap);

        tmpMap = new HashMap<>();
        tmpMap.put(ApiConsts.CREATED, "Created");
        tmpMap.put(ApiConsts.DELETED, "Deleted");
        tmpMap.put(ApiConsts.MODIFIED, "Modified");

        tmpMap.put(ApiConsts.FAIL_SQL, "FAIL_SQL");
        tmpMap.put(ApiConsts.FAIL_SQL_ROLLBACK, "FAIL_SQL_ROLLBACK");
        tmpMap.put(ApiConsts.FAIL_INVLD_NODE_NAME, "FAIL_INVLD_NODE_NAME");
        tmpMap.put(ApiConsts.FAIL_INVLD_NODE_TYPE, "FAIL_INVLD_NODE_TYPE");
        tmpMap.put(ApiConsts.FAIL_INVLD_RSC_NAME, "FAIL_INVLD_RSC_NAME");
        tmpMap.put(ApiConsts.FAIL_INVLD_RSC_PORT, "FAIL_INVLD_RSC_PORT");
        tmpMap.put(ApiConsts.FAIL_INVLD_NODE_ID, "FAIL_INVLD_NODE_ID");
        tmpMap.put(ApiConsts.FAIL_INVLD_VLM_NR, "FAIL_INVLD_VLM_NR");
        tmpMap.put(ApiConsts.FAIL_INVLD_VLM_SIZE, "FAIL_INVLD_VLM_SIZE");
        tmpMap.put(ApiConsts.FAIL_INVLD_MINOR_NR, "FAIL_INVLD_MINOR_NR");
        tmpMap.put(ApiConsts.FAIL_INVLD_STOR_POOL_NAME, "FAIL_INVLD_STOR_POOL_NAME");
        tmpMap.put(ApiConsts.FAIL_INVLD_NET_NAME, "FAIL_INVLD_NET_NAME");
        tmpMap.put(ApiConsts.FAIL_INVLD_NET_ADDR, "FAIL_INVLD_NET_ADDR");
        tmpMap.put(ApiConsts.FAIL_INVLD_NET_PORT, "FAIL_INVLD_NET_PORT");
        tmpMap.put(ApiConsts.FAIL_INVLD_NET_TYPE, "FAIL_INVLD_NET_TYPE");
        tmpMap.put(ApiConsts.FAIL_INVLD_PROP, "FAIL_INVLD_PROP");
        tmpMap.put(ApiConsts.FAIL_INVLD_TRANSPORT_TYPE, "FAIL_INVLD_TRANSPORT_TYPE");
        tmpMap.put(ApiConsts.FAIL_INVLD_TCP_PORT, "FAIL_INVLD_TCP_PORT");
        tmpMap.put(ApiConsts.FAIL_INVLD_CRYPT_PASSPHRASE, "FAIL_INVLD_CRYPT_PASSPHRASE");
        tmpMap.put(ApiConsts.FAIL_INVLD_ENCRYPT_TYPE, "FAIL_INVLD_ENCRYPT_TYPE");
        tmpMap.put(ApiConsts.FAIL_INVLD_SNAPSHOT_NAME, "FAIL_INVLD_SNAPSHOT_NAME");
        tmpMap.put(ApiConsts.FAIL_INVLD_PLACE_COUNT, "FAIL_INVLD_PLACE_COUNT");
        tmpMap.put(ApiConsts.FAIL_INVLD_FREE_SPACE_MGR_NAME, "FAIL_INVLD_FREE_SPACE_MGR_NAME");
        tmpMap.put(ApiConsts.FAIL_INVLD_STOR_DRIVER, "FAIL_INVLD_STOR_DRIVER");
        tmpMap.put(ApiConsts.FAIL_INVLD_DRBD_PROXY_COMPRESSION_TYPE, "FAIL_INVLD_DRBD_PROXY_COMPRESSION_TYPE");
        tmpMap.put(ApiConsts.FAIL_INVLD_KVS_NAME, "FAIL_INVLD_KVS_NAME");
        tmpMap.put(ApiConsts.FAIL_INVLD_LAYER_KIND, "FAIL_INVLD_LAYER_KIND");
        tmpMap.put(ApiConsts.FAIL_INVLD_LAYER_STACK, "FAIL_INVLD_LAYER_STACK");
        tmpMap.put(ApiConsts.FAIL_INVLD_EXT_NAME, "FAIL_INVLD_EXT_NAME");
        tmpMap.put(ApiConsts.FAIL_NOT_FOUND_NODE, "FAIL_NOT_FOUND_NODE");
        tmpMap.put(ApiConsts.FAIL_NOT_FOUND_RSC_DFN, "FAIL_NOT_FOUND_RSC_DFN");
        tmpMap.put(ApiConsts.FAIL_NOT_FOUND_RSC, "FAIL_NOT_FOUND_RSC");
        tmpMap.put(ApiConsts.FAIL_NOT_FOUND_VLM_DFN, "FAIL_NOT_FOUND_VLM_DFN");
        tmpMap.put(ApiConsts.FAIL_NOT_FOUND_VLM, "FAIL_NOT_FOUND_VLM");
        tmpMap.put(ApiConsts.FAIL_NOT_FOUND_NET_IF, "FAIL_NOT_FOUND_NET_IF");
        tmpMap.put(ApiConsts.FAIL_NOT_FOUND_NODE_CONN, "FAIL_NOT_FOUND_NODE_CONN");
        tmpMap.put(ApiConsts.FAIL_NOT_FOUND_RSC_CONN, "FAIL_NOT_FOUND_RSC_CONN");
        tmpMap.put(ApiConsts.FAIL_NOT_FOUND_VLM_CONN, "FAIL_NOT_FOUND_VLM_CONN");
        tmpMap.put(ApiConsts.FAIL_NOT_FOUND_STOR_POOL_DFN, "FAIL_NOT_FOUND_STOR_POOL_DFN");
        tmpMap.put(ApiConsts.FAIL_NOT_FOUND_STOR_POOL, "FAIL_NOT_FOUND_STOR_POOL");
        tmpMap.put(ApiConsts.FAIL_NOT_FOUND_DFLT_STOR_POOL, "FAIL_NOT_FOUND_DFLT_STOR_POOL");
        tmpMap.put(ApiConsts.FAIL_NOT_FOUND_CRYPT_KEY, "FAIL_NOT_FOUND_CRYPT_KEY");
        tmpMap.put(ApiConsts.FAIL_NOT_FOUND_SNAPSHOT_DFN, "FAIL_NOT_FOUND_SNAPSHOT_DFN");
        tmpMap.put(ApiConsts.FAIL_NOT_FOUND_SNAPSHOT_VLM_DFN, "FAIL_NOT_FOUND_SNAPSHOT_VLM_DFN");
        tmpMap.put(ApiConsts.FAIL_NOT_FOUND_SNAPSHOT, "FAIL_NOT_FOUND_SNAPSHOT");
        tmpMap.put(ApiConsts.FAIL_NOT_FOUND_KVS, "FAIL_NOT_FOUND_KVS");
        tmpMap.put(ApiConsts.FAIL_ACC_DENIED_NODE, "FAIL_ACC_DENIED_NODE");
        tmpMap.put(ApiConsts.FAIL_ACC_DENIED_RSC_DFN, "FAIL_ACC_DENIED_RSC_DFN");
        tmpMap.put(ApiConsts.FAIL_ACC_DENIED_RSC, "FAIL_ACC_DENIED_RSC");
        tmpMap.put(ApiConsts.FAIL_ACC_DENIED_VLM_DFN, "FAIL_ACC_DENIED_VLM_DFN");
        tmpMap.put(ApiConsts.FAIL_ACC_DENIED_VLM, "FAIL_ACC_DENIED_VLM");
        tmpMap.put(ApiConsts.FAIL_ACC_DENIED_STOR_POOL_DFN, "FAIL_ACC_DENIED_STOR_POOL_DFN");
        tmpMap.put(ApiConsts.FAIL_ACC_DENIED_STOR_POOL, "FAIL_ACC_DENIED_STOR_POOL");
        tmpMap.put(ApiConsts.FAIL_ACC_DENIED_NODE_CONN, "FAIL_ACC_DENIED_NODE_CONN");
        tmpMap.put(ApiConsts.FAIL_ACC_DENIED_RSC_CONN, "FAIL_ACC_DENIED_RSC_CONN");
        tmpMap.put(ApiConsts.FAIL_ACC_DENIED_VLM_CONN, "FAIL_ACC_DENIED_VLM_CONN");
        tmpMap.put(ApiConsts.FAIL_ACC_DENIED_STLT_CONN, "FAIL_ACC_DENIED_STLT_CONN");
        tmpMap.put(ApiConsts.FAIL_ACC_DENIED_CTRL_CFG, "FAIL_ACC_DENIED_CTRL_CFG");
        tmpMap.put(ApiConsts.FAIL_ACC_DENIED_COMMAND, "FAIL_ACC_DENIED_COMMAND");
        tmpMap.put(ApiConsts.FAIL_ACC_DENIED_WATCH, "FAIL_ACC_DENIED_WATCH");
        tmpMap.put(ApiConsts.FAIL_ACC_DENIED_SNAPSHOT_DFN, "FAIL_ACC_DENIED_SNAPSHOT_DFN");
        tmpMap.put(ApiConsts.FAIL_ACC_DENIED_SNAPSHOT, "FAIL_ACC_DENIED_SNAPSHOT");
        tmpMap.put(ApiConsts.FAIL_ACC_DENIED_SNAPSHOT_VLM_DFN, "FAIL_ACC_DENIED_SNAPSHOT_VLM_DFN");
        tmpMap.put(ApiConsts.FAIL_ACC_DENIED_FREE_SPACE_MGR, "FAIL_ACC_DENIED_FREE_SPACE_MGR");
        tmpMap.put(ApiConsts.FAIL_ACC_DENIED_KVS, "FAIL_ACC_DENIED_KVS");
        tmpMap.put(ApiConsts.FAIL_EXISTS_NODE, "FAIL_EXISTS_NODE");
        tmpMap.put(ApiConsts.FAIL_EXISTS_RSC_DFN, "FAIL_EXISTS_RSC_DFN");
        tmpMap.put(ApiConsts.FAIL_EXISTS_RSC, "FAIL_EXISTS_RSC");
        tmpMap.put(ApiConsts.FAIL_EXISTS_VLM_DFN, "FAIL_EXISTS_VLM_DFN");
        tmpMap.put(ApiConsts.FAIL_EXISTS_VLM, "FAIL_EXISTS_VLM");
        tmpMap.put(ApiConsts.FAIL_EXISTS_NET_IF, "FAIL_EXISTS_NET_IF");
        tmpMap.put(ApiConsts.FAIL_EXISTS_NODE_CONN, "FAIL_EXISTS_NODE_CONN");
        tmpMap.put(ApiConsts.FAIL_EXISTS_RSC_CONN, "FAIL_EXISTS_RSC_CONN");
        tmpMap.put(ApiConsts.FAIL_EXISTS_VLM_CONN, "FAIL_EXISTS_VLM_CONN");
        tmpMap.put(ApiConsts.FAIL_EXISTS_STOR_POOL_DFN, "FAIL_EXISTS_STOR_POOL_DFN");
        tmpMap.put(ApiConsts.FAIL_EXISTS_STOR_POOL, "FAIL_EXISTS_STOR_POOL");
        tmpMap.put(ApiConsts.FAIL_EXISTS_STLT_CONN, "FAIL_EXISTS_STLT_CONN");
        tmpMap.put(ApiConsts.FAIL_EXISTS_CRYPT_PASSPHRASE, "FAIL_EXISTS_CRYPT_PASSPHRASE");
        tmpMap.put(ApiConsts.FAIL_EXISTS_WATCH, "FAIL_EXISTS_WATCH");
        tmpMap.put(ApiConsts.FAIL_EXISTS_SNAPSHOT_DFN, "FAIL_EXISTS_SNAPSHOT_DFN");
        tmpMap.put(ApiConsts.FAIL_EXISTS_SNAPSHOT, "FAIL_EXISTS_SNAPSHOT");
        tmpMap.put(ApiConsts.FAIL_EXISTS_EXT_NAME, "FAIL_EXISTS_EXT_NAME");
        tmpMap.put(ApiConsts.FAIL_MISSING_PROPS, "FAIL_MISSING_PROPS");
        tmpMap.put(ApiConsts.FAIL_MISSING_PROPS_NETCOM_TYPE, "FAIL_MISSING_PROPS_NETCOM_TYPE");
        tmpMap.put(ApiConsts.FAIL_MISSING_PROPS_NETCOM_PORT, "FAIL_MISSING_PROPS_NETCOM_PORT");
        tmpMap.put(ApiConsts.FAIL_MISSING_NETCOM, "FAIL_MISSING_NETCOM");
        tmpMap.put(ApiConsts.FAIL_MISSING_PROPS_NETIF_NAME, "FAIL_MISSING_PROPS_NETIF_NAME");
        tmpMap.put(ApiConsts.FAIL_MISSING_STLT_CONN, "FAIL_MISSING_STLT_CONN");
        tmpMap.put(ApiConsts.FAIL_MISSING_EXT_NAME, "FAIL_MISSING_EXT_NAME");
        tmpMap.put(ApiConsts.FAIL_UUID_NODE, "FAIL_UUID_NODE");
        tmpMap.put(ApiConsts.FAIL_UUID_RSC_DFN, "FAIL_UUID_RSC_DFN");
        tmpMap.put(ApiConsts.FAIL_UUID_RSC, "FAIL_UUID_RSC");
        tmpMap.put(ApiConsts.FAIL_UUID_VLM_DFN, "FAIL_UUID_VLM_DFN");
        tmpMap.put(ApiConsts.FAIL_UUID_VLM, "FAIL_UUID_VLM");
        tmpMap.put(ApiConsts.FAIL_UUID_NET_IF, "FAIL_UUID_NET_IF");
        tmpMap.put(ApiConsts.FAIL_UUID_NODE_CONN, "FAIL_UUID_NODE_CONN");
        tmpMap.put(ApiConsts.FAIL_UUID_RSC_CONN, "FAIL_UUID_RSC_CONN");
        tmpMap.put(ApiConsts.FAIL_UUID_VLM_CONN, "FAIL_UUID_VLM_CONN");
        tmpMap.put(ApiConsts.FAIL_UUID_STOR_POOL_DFN, "FAIL_UUID_STOR_POOL_DFN");
        tmpMap.put(ApiConsts.FAIL_UUID_STOR_POOL, "FAIL_UUID_STOR_POOL");
        tmpMap.put(ApiConsts.FAIL_UUID_KVS, "FAIL_UUID_KVS");
        tmpMap.put(ApiConsts.FAIL_POOL_EXHAUSTED_VLM_NR, "FAIL_POOL_EXHAUSTED_VLM_NR");
        tmpMap.put(ApiConsts.FAIL_POOL_EXHAUSTED_MINOR_NR, "FAIL_POOL_EXHAUSTED_MINOR_NR");
        tmpMap.put(ApiConsts.FAIL_POOL_EXHAUSTED_TCP_PORT, "FAIL_POOL_EXHAUSTED_TCP_PORT");
        tmpMap.put(ApiConsts.FAIL_POOL_EXHAUSTED_NODE_ID, "FAIL_POOL_EXHAUSTED_NODE_ID");
        tmpMap.put(ApiConsts.FAIL_POOL_EXHAUSTED_RSC_LAYER_ID, "FAIL_POOL_EXHAUSTED_RSC_LAYER_ID");
        tmpMap.put(ApiConsts.FAIL_STLT_DOES_NOT_SUPPORT_LAYER, "FAIL_STLT_DOES_NOT_SUPPORT_LAYER");
        tmpMap.put(ApiConsts.FAIL_STLT_DOES_NOT_SUPPORT_PROVIDER, "FAIL_STLT_DOES_NOT_SUPPORT_PROVIDER");
        tmpMap.put(ApiConsts.FAIL_STOR_POOL_CONFIGURATION_ERROR, "FAIL_STOR_POOL_CONFIGURATION_ERROR");
        tmpMap.put(ApiConsts.FAIL_INSUFFICIENT_REPLICA_COUNT, "FAIL_INSUFFICIENT_REPLICA_COUNT");
        tmpMap.put(ApiConsts.FAIL_RSC_BUSY, "FAIL_RSC_BUSY");
        tmpMap.put(ApiConsts.FAIL_INSUFFICIENT_PEER_SLOTS, "FAIL_INSUFFICIENT_PEER_SLOTS");
        tmpMap.put(ApiConsts.FAIL_SNAPSHOTS_NOT_SUPPORTED, "FAIL_SNAPSHOTS_NOT_SUPPORTED");
        tmpMap.put(ApiConsts.FAIL_NOT_CONNECTED, "FAIL_NOT_CONNECTED");
        tmpMap.put(ApiConsts.FAIL_NOT_ENOUGH_NODES, "FAIL_NOT_ENOUGH_NODES");
        tmpMap.put(ApiConsts.FAIL_IN_USE, "FAIL_IN_USE");
        tmpMap.put(ApiConsts.FAIL_UNKNOWN_ERROR, "FAIL_UNKNOWN_ERROR");
        tmpMap.put(ApiConsts.FAIL_IMPL_ERROR, "FAIL_IMPL_ERROR");
        tmpMap.put(ApiConsts.WARN_INVLD_OPT_PROP_NETCOM_ENABLED, "WARN_INVLD_OPT_PROP_NETCOM_ENABLED");
        tmpMap.put(ApiConsts.WARN_NOT_CONNECTED, "WARN_NOT_CONNECTED");
        tmpMap.put(ApiConsts.WARN_STLT_NOT_UPDATED, "WARN_STLT_NOT_UPDATED");
        tmpMap.put(ApiConsts.WARN_NO_STLT_CONN_DEFINED, "WARN_NO_STLT_CONN_DEFINED");
        tmpMap.put(ApiConsts.WARN_DEL_UNSET_PROP, "WARN_DEL_UNSET_PROP");
        tmpMap.put(ApiConsts.WARN_RSC_ALREADY_DEPLOYED, "WARN_RSC_ALREADY_DEPLOYED");
        tmpMap.put(ApiConsts.WARN_RSC_ALREADY_HAS_DISK, "WARN_RSC_ALREADY_HAS_DISK");
        tmpMap.put(ApiConsts.WARN_RSC_ALREADY_DISKLESS, "WARN_RSC_ALREADY_DISKLESS");
        tmpMap.put(ApiConsts.WARN_ALL_DISKLESS, "WARN_ALL_DISKLESS");
        tmpMap.put(ApiConsts.WARN_STORAGE_ERROR, "WARN_STORAGE_ERROR");
        tmpMap.put(ApiConsts.WARN_STORAGE_KIND_ADDED, "WARN_STORAGE_KIND_ADDED");
        tmpMap.put(ApiConsts.WARN_NOT_FOUND_CRYPT_KEY, "WARN_NOT_FOUND_CRYPT_KEY");
        tmpMap.put(ApiConsts.WARN_NOT_FOUND, "WARN_NOT_FOUND");
        tmpMap.put(ApiConsts.UNKNOWN_API_CALL, "UNKNOWN_API_CALL");
        tmpMap.put(ApiConsts.API_CALL_AUTH_REQ, "API_CALL_AUTH_REQ");
        tmpMap.put(ApiConsts.API_CALL_PARSE_ERROR, "API_CALL_PARSE_ERROR");
        tmpMap.put(ApiConsts.SUCCESS_SIGN_IN, "SUCCESS_SIGN_IN");
        tmpMap.put(ApiConsts.FAIL_SIGN_IN, "FAIL_SIGN_IN");
        RET_CODES_ACTION = Collections.unmodifiableMap(tmpMap);
    }

    public static void appendReadableRetCode(StringBuilder sb, long retCode)
    {
        ResolvedRetCode resolvedRetCode = extract(retCode);

        sb.append(resolvedRetCode.strType)
            .append(" ")
            .append(resolvedRetCode.strOp)
            .append(" ")
            .append(resolvedRetCode.strObj)
            .append(" ")
            .append(resolvedRetCode.strAction)
            .append(" [")
            .append(Long.toHexString(retCode))
            .append("]");
    }

    public static ResolvedRetCode extract(long retCode)
    {
        ResolvedRetCode resolvedRetCode = new ResolvedRetCode();

        resolvedRetCode.lType   = retCode & MASK_TYPE;
        resolvedRetCode.strType = RET_CODES_TYPE.get(resolvedRetCode.lType);

        resolvedRetCode.lOp   = retCode & MASK_OP;
        resolvedRetCode.strOp = RET_CODES_OP.get(resolvedRetCode.lOp);

        resolvedRetCode.lObj   = retCode & MASK_OBJ;
        resolvedRetCode.strObj = RET_CODES_OBJ.get(resolvedRetCode.lObj);

        resolvedRetCode.lAction   = retCode & MASK_ACTION;
        resolvedRetCode.strAction = RET_CODES_ACTION.get(resolvedRetCode.lAction);

        return resolvedRetCode;
    }

    public static class ResolvedRetCode
    {
        @Nullable String strType; // error, warn, info, success
        @Nullable String strOp;   // create, modify, delete
        @Nullable String strObj;  // node, rscDfn, rscn, ...
        @Nullable String strAction; // actual code. FAIL_SQL, ...

        long lType;   // only the type-mask of the original return code
        long lOp;     // only the op-mask of the original return code
        long lObj;    // only the obj-mask of the original return code
        long lAction; // only the action-mask of the original return code

        long origRetCode;
    }

    public static boolean isError(ApiCallRc apiCallRc)
    {
        return apiCallRc.stream().anyMatch(ApiRcUtils::entryIsError);
    }

    private static boolean entryIsError(ApiCallRc.RcEntry rcEntry)
    {
        return
            (rcEntry.getReturnCode() & ApiConsts.MASK_ERROR) == ApiConsts.MASK_ERROR ||
                (rcEntry.getReturnCode() & ApiConsts.MASK_WARN) == ApiConsts.MASK_WARN;
    }

    private ApiRcUtils()
    {
    }
}
