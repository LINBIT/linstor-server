package com.linbit.linstor.api;

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
        tmpMap.put(ApiConsts.FAIL_MISSING_PROPS, "FAIL_MISSING_PROPS");
        tmpMap.put(ApiConsts.FAIL_MISSING_PROPS_NETCOM_TYPE, "FAIL_MISSING_PROPS_NETCOM_TYPE");
        tmpMap.put(ApiConsts.FAIL_MISSING_PROPS_NETCOM_PORT, "FAIL_MISSING_PROPS_NETCOM_PORT");
        tmpMap.put(ApiConsts.FAIL_MISSING_NETCOM, "FAIL_MISSING_NETCOM");
        tmpMap.put(ApiConsts.FAIL_MISSING_PROPS_NETIF_NAME, "FAIL_MISSING_PROPS_NETIF_NAME");
        tmpMap.put(ApiConsts.FAIL_MISSING_STLT_CONN, "FAIL_MISSING_STLT_CONN");
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
        tmpMap.put(ApiConsts.FAIL_IN_USE, "FAIL_IN_USE");
        tmpMap.put(ApiConsts.FAIL_UNKNOWN_ERROR, "FAIL_UNKNOWN_ERROR");
        tmpMap.put(ApiConsts.FAIL_IMPL_ERROR, "FAIL_IMPL_ERROR");
        tmpMap.put(ApiConsts.WARN_INVLD_OPT_PROP_NETCOM_ENABLED, "WARN_INVLD_OPT_PROP_NETCOM_ENABLED");
        tmpMap.put(ApiConsts.WARN_NOT_CONNECTED, "WARN_NOT_CONNECTED");
        tmpMap.put(ApiConsts.WARN_NOT_FOUND, "WARN_NOT_FOUND");
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
        String strType; // error, warn, info, success
        String strOp;   // create, modify, delete
        String strObj;  // node, rscDfn, rscn, ...
        String strAction; // actual code. FAIL_SQL, ...

        long lType;   // only the type-mask of the original return code
        long lOp;     // only the op-mask of the original return code
        long lObj;    // only the obj-mask of the original return code
        long lAction; // only the action-mask of the original return code

        long origRetCode;
    }
}
