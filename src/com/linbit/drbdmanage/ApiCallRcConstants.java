package com.linbit.drbdmanage;

public class ApiCallRcConstants
{
    // Mask for return codes that describe an error
    public static final long MASK_ERROR = 0xC000000000000000L;

    // Mask for return codes that describe a warning
    public static final long MASK_WARN  = 0x8000000000000000L;

    // Mask for return codes that describe contain detail information
    // about the result of an operation
    public static final long MASK_INFO  = 0x4000000000000000L;
    /*
     * The most significant 2 bits are reserved for MASK_ERROR, MASK_WARN,
     * MASK_INFO and MASK_SUCCESS
     */

    /*
     * The next 4 significant bits are reserved for type (Node, ResDfn, Res,
     * VolDfn, Vol, NetInterface, ...)
     */
    public static final long MASK_NODE          = 0x3C00000000000000L;
    public static final long MASK_RSC_DFN       = 0x3800000000000000L;
    public static final long MASK_RSC           = 0x3400000000000000L;
    public static final long MASK_VLM_DFN       = 0x3000000000000000L;
    public static final long MASK_VLM           = 0x2C00000000000000L;
    public static final long MASK_NODE_CONN     = 0x2800000000000000L;
    public static final long MASK_RSC_CONN      = 0x2400000000000000L;
    public static final long MASK_VLM_CONN      = 0x2000000000000000L;
    public static final long MASK_NET_IF        = 0x1C00000000000000L;
    public static final long MASK_STOR_POOL_DFN = 0x1800000000000000L;
    public static final long MASK_STOR_POOL     = 0x1400000000000000L;
    /*
     *  unused type masks:
      0x10_00000000000000L;
      0x0C_00000000000000L;
      0x08_00000000000000L;
      0x04_00000000000000L;
      0x00_00000000000000L; // this should be avoided
   */

    /*
     * Codes 1-9: success
     */
    public static final long CREATED                    = 1 | MASK_INFO;
    public static final long DELETED                    = 2 | MASK_INFO;

    /*
     * Codes 10-99: special cases (like "failed to delete non existent data")
     */
    public static final long CRT_FAIL_IMPL_ERROR        = 10 | MASK_ERROR;
    public static final long DEL_NOT_FOUND              = 11 | MASK_WARN;

    /*
     * Codes 100-199: creation failures
     */
    /*
     * Codes 100-109: sql creation failures
     */
    public static final long CRT_FAIL_SQL               = 100 | MASK_ERROR;
    public static final long CRT_FAIL_SQL_ROLLBACK      = 101 | MASK_ERROR;

    /*
     * Codes 110-119: invalid * creation failures
     */
    public static final long CRT_FAIL_INVLD_NODE_NAME       = 110 | MASK_ERROR;
    public static final long CRT_FAIL_INVLD_NODE_TYPE       = 111 | MASK_ERROR;
    public static final long CRT_FAIL_INVLD_RSC_NAME        = 112 | MASK_ERROR;
    public static final long CRT_FAIL_INVLD_NODE_ID         = 113 | MASK_ERROR;
    public static final long CRT_FAIL_INVLD_VLM_NR          = 114 | MASK_ERROR;
    public static final long CRT_FAIL_INVLD_VLM_SIZE        = 115 | MASK_ERROR;
    public static final long CRT_FAIL_INVLD_MINOR_NR        = 116 | MASK_ERROR;
    public static final long CRT_FAIL_INVLD_STOR_POOL_NAME  = 117 | MASK_ERROR;

    /*
     * Codes 120-139: dependency not found creation failures
     */
    public static final long CRT_FAIL_NOT_FOUND_NODE            = 120 | MASK_ERROR;
    public static final long CRT_FAIL_NOT_FOUND_RSC_DFN         = 121 | MASK_ERROR;
    public static final long CRT_FAIL_NOT_FOUND_RSC             = 122 | MASK_ERROR;
    public static final long CRT_FAIL_NOT_FOUND_VLM_DFN         = 123 | MASK_ERROR;
    public static final long CRT_FAIL_NOT_FOUND_VLM             = 124 | MASK_ERROR;
    public static final long CRT_FAIL_NOT_FOUND_NET_IF          = 125 | MASK_ERROR;
    public static final long CRT_FAIL_NOT_FOUND_NODE_CONN       = 126 | MASK_ERROR;
    public static final long CRT_FAIL_NOT_FOUND_RSC_CONN        = 127 | MASK_ERROR;
    public static final long CRT_FAIL_NOT_FOUND_VLM_CONN        = 128 | MASK_ERROR;
    public static final long CRT_FAIL_NOT_FOUND_STOR_POOL_DFN   = 129 | MASK_ERROR;
    public static final long CRT_FAIL_NOT_FOUND_STOR_POOL       = 130 | MASK_ERROR;

    /*
     * Codes 140-149: access denied creation failures
     */
    public static final long CRT_FAIL_ACC_DENIED_NODE           = 140 | MASK_ERROR;
    public static final long CRT_FAIL_ACC_DENIED_RSC_DFN        = 141 | MASK_ERROR;
    public static final long CRT_FAIL_ACC_DENIED_RSC            = 142 | MASK_ERROR;
    public static final long CRT_FAIL_ACC_DENIED_VLM_DFN        = 143 | MASK_ERROR;
    public static final long CRT_FAIL_ACC_DENIED_VLM            = 144 | MASK_ERROR;
    public static final long CRT_FAIL_ACC_DENIED_STOR_POOL_DFN  = 145 | MASK_ERROR;
    public static final long CRT_FAIL_ACC_DENIED_STOR_POOL      = 146 | MASK_ERROR;

    /*
     * Codes 150-169: data already exists creation failures
     */
    public static final long CRT_FAIL_EXISTS_NODE           = 150 | MASK_ERROR;
    public static final long CRT_FAIL_EXISTS_RSC_DFN        = 151 | MASK_ERROR;
    public static final long CRT_FAIL_EXISTS_RSC            = 152 | MASK_ERROR;
    public static final long CRT_FAIL_EXISTS_VLM_DFN        = 153 | MASK_ERROR;
    public static final long CRT_FAIL_EXISTS_VLM            = 154 | MASK_ERROR;
    public static final long CRT_FAIL_EXISTS_NET_IF         = 155 | MASK_ERROR;
    public static final long CRT_FAIL_EXISTS_NODE_CONN      = 156 | MASK_ERROR;
    public static final long CRT_FAIL_EXISTS_RSC_CONN       = 157 | MASK_ERROR;
    public static final long CRT_FAIL_EXISTS_VLM_CONN       = 158 | MASK_ERROR;
    public static final long CRT_FAIL_EXISTS_STOR_POOL_DFN  = 159 | MASK_ERROR;
    public static final long CRT_FAIL_EXISTS_STOR_POOL      = 160 | MASK_ERROR;

    /*
     * Codes 200-299: deletion failures
     * sub-codes same as for creation failures, except data exists, which can only yield in IMPL_ERROR
     */
    public static final long DEL_FAIL_SQL               = 200 | MASK_ERROR;
    public static final long DEL_FAIL_SQL_ROLLBACK      = 201 | MASK_ERROR;

    public static final long DEL_FAIL_INVLD_NODE_NAME       = 210 | MASK_ERROR;
    public static final long DEL_FAIL_INVLD_NODE_TYPE       = 211 | MASK_ERROR;
    public static final long DEL_FAIL_INVLD_RSC_NAME        = 212 | MASK_ERROR;
    public static final long DEL_FAIL_INVLD_NODE_ID         = 213 | MASK_ERROR;
    public static final long DEL_FAIL_INVLD_VLM_NR          = 214 | MASK_ERROR;
    public static final long DEL_FAIL_INVLD_VLM_SIZE        = 215 | MASK_ERROR;
    public static final long DEL_FAIL_INVLD_MINOR_NR        = 216 | MASK_ERROR;
    public static final long DEL_FAIL_INVLD_STOR_POOL_NAME  = 217 | MASK_ERROR;

    public static final long DEL_FAIL_NOT_FOUND_NODE            = 220 | MASK_ERROR;
    public static final long DEL_FAIL_NOT_FOUND_RSC_DFN         = 221 | MASK_ERROR;
    public static final long DEL_FAIL_NOT_FOUND_RSC             = 222 | MASK_ERROR;
    public static final long DEL_FAIL_NOT_FOUND_VLM_DFN         = 223 | MASK_ERROR;
    public static final long DEL_FAIL_NOT_FOUND_VLM             = 224 | MASK_ERROR;
    public static final long DEL_FAIL_NOT_FOUND_NET_IF          = 225 | MASK_ERROR;
    public static final long DEL_FAIL_NOT_FOUND_NODE_CONN       = 226 | MASK_ERROR;
    public static final long DEL_FAIL_NOT_FOUND_RSC_CONN        = 227 | MASK_ERROR;
    public static final long DEL_FAIL_NOT_FOUND_VLM_CONN        = 228 | MASK_ERROR;
    public static final long DEL_FAIL_NOT_FOUND_STOR_POOL_DFN   = 229 | MASK_ERROR;
    public static final long DEL_FAIL_NOT_FOUND_STOR_POOL       = 230 | MASK_ERROR;

    public static final long DEL_FAIL_ACC_DENIED_NODE           = 240 | MASK_ERROR;
    public static final long DEL_FAIL_ACC_DENIED_RSC_DFN        = 241 | MASK_ERROR;
    public static final long DEL_FAIL_ACC_DENIED_RSC            = 242 | MASK_ERROR;
    public static final long DEL_FAIL_ACC_DENIED_VLM_DFN        = 243 | MASK_ERROR;
    public static final long DEL_FAIL_ACC_DENIED_VLM            = 244 | MASK_ERROR;
    public static final long DEL_FAIL_ACC_DENIED_STOR_POOL_DFN  = 245 | MASK_ERROR;
    public static final long DEL_FAIL_ACC_DENIED_STOR_POOL      = 246 | MASK_ERROR;

    public static final long DEL_FAIL_IN_USE                = 298 | MASK_ERROR;
    public static final long DEL_FAIL_IMPL_ERROR            = 299 | MASK_ERROR;

    /*
     * Node return codes
     */
    public static final long RC_NODE_CREATED                    = MASK_NODE | CREATED;
    public static final long RC_NODE_DELETED                    = MASK_NODE | DELETED;

    public static final long RC_NODE_DEL_NOT_FOUND              = MASK_NODE | DEL_NOT_FOUND;

    public static final long RC_NODE_CRT_FAIL_SQL               = MASK_NODE | CRT_FAIL_SQL;
    public static final long RC_NODE_CRT_FAIL_SQL_ROLLBACK      = MASK_NODE | CRT_FAIL_SQL_ROLLBACK;
    public static final long RC_NODE_CRT_FAIL_INVLD_NODE_NAME   = MASK_NODE | CRT_FAIL_INVLD_NODE_NAME;
    public static final long RC_NODE_CRT_FAIL_INVLD_NODE_TYPE   = MASK_NODE | CRT_FAIL_INVLD_NODE_TYPE;
    public static final long RC_NODE_CRT_FAIL_EXISTS_NODE       = MASK_NODE | CRT_FAIL_EXISTS_NODE;
    public static final long RC_NODE_CRT_FAIL_ACC_DENIED_NODE   = MASK_NODE | CRT_FAIL_ACC_DENIED_NODE;

    public static final long RC_NODE_DEL_FAIL_SQL               = MASK_NODE | DEL_FAIL_SQL;
    public static final long RC_NODE_DEL_FAIL_SQL_ROLLBACK      = MASK_NODE | DEL_FAIL_SQL_ROLLBACK;
    public static final long RC_NODE_DEL_FAIL_INVALID_NODE_NAME = MASK_NODE | DEL_FAIL_INVLD_NODE_NAME;
    public static final long RC_NODE_DEL_FAIL_ACC_DENIED_NODE   = MASK_NODE | DEL_FAIL_ACC_DENIED_NODE;
    public static final long RC_NODE_DEL_FAIL_IMPL_ERROR        = MASK_NODE | DEL_FAIL_IMPL_ERROR;

    /*
     * ResourceDefinition return codes
     */
    public static final long RC_RSC_DFN_CREATED                     = MASK_RSC_DFN | CREATED;
    public static final long RC_RSC_DFN_DELETED                     = MASK_RSC_DFN | DELETED;

    public static final long RC_RSC_DFN_DEL_NOT_FOUND               = MASK_RSC_DFN | DEL_NOT_FOUND;

    public static final long RC_RSC_DFN_CRT_FAIL_INVLD_RSC_NAME     = MASK_RSC_DFN | CRT_FAIL_INVLD_RSC_NAME;
    public static final long RC_RSC_DFN_CRT_FAIL_INVLD_VLM_NR       = MASK_RSC_DFN | CRT_FAIL_INVLD_VLM_NR;
    public static final long RC_RSC_DFN_CRT_FAIL_INVLD_MINOR_NR     = MASK_RSC_DFN | CRT_FAIL_INVLD_MINOR_NR;
    public static final long RC_RSC_DFN_CRT_FAIL_INVLD_VLM_SIZE     = MASK_RSC_DFN | CRT_FAIL_INVLD_VLM_SIZE;
    public static final long RC_RSC_DFN_CRT_FAIL_SQL                = MASK_RSC_DFN | CRT_FAIL_SQL;
    public static final long RC_RSC_DFN_CRT_FAIL_SQL_ROLLBACK       = MASK_RSC_DFN | CRT_FAIL_SQL_ROLLBACK;
    public static final long RC_RSC_DFN_CRT_FAIL_ACC_DENIED_RSC_DFN = MASK_RSC_DFN | CRT_FAIL_ACC_DENIED_RSC_DFN;
    public static final long RC_RSC_DFN_CRT_FAIL_ACC_DENIED_VLM_DFN = MASK_RSC_DFN | CRT_FAIL_ACC_DENIED_VLM_DFN;
    public static final long RC_RSC_DFN_CRT_FAIL_EXISTS_RSC_DFN     = MASK_RSC_DFN | CRT_FAIL_EXISTS_RSC_DFN;
    public static final long RC_RSC_DFN_CRT_FAIL_EXISTS_VLM_DFN     = MASK_RSC_DFN | CRT_FAIL_EXISTS_VLM_DFN;

    public static final long RC_RSC_DFN_DEL_FAIL_INVLD_RSC_NAME     = MASK_RSC_DFN | DEL_FAIL_INVLD_RSC_NAME;
    public static final long RC_RSC_DFN_DEL_FAIL_SQL                = MASK_RSC_DFN | DEL_FAIL_SQL;
    public static final long RC_RSC_DFN_DEL_FAIL_SQL_ROLLBACK       = MASK_RSC_DFN | DEL_FAIL_SQL_ROLLBACK;
    public static final long RC_RSC_DFN_DEL_FAIL_ACC_DENIED_RSC_DFN = MASK_RSC_DFN | DEL_FAIL_ACC_DENIED_RSC_DFN;
    public static final long RC_RSC_DFN_DEL_FAIL_IMPL_ERROR         = MASK_RSC_DFN | DEL_FAIL_IMPL_ERROR;


    /*
     * Resource return codes
     */
    public static final long RC_RSC_CREATED                     = MASK_RSC | CREATED;
    public static final long RC_RSC_DELETED                     = MASK_RSC | DELETED;

    public static final long RC_RSC_DEL_NOT_FOUND               = MASK_RSC | DEL_NOT_FOUND;

    public static final long RC_RSC_CRT_FAIL_SQL                = MASK_RSC | CRT_FAIL_SQL;
    public static final long RC_RSC_CRT_FAIL_SQL_ROLLBACK       = MASK_RSC | CRT_FAIL_SQL_ROLLBACK;

    public static final long RC_RSC_CRT_FAIL_INVALID_NODE_NAME      = MASK_RSC | CRT_FAIL_INVLD_NODE_NAME;
    public static final long RC_RSC_CRT_FAIL_INVALID_RSC_NAME       = MASK_RSC | CRT_FAIL_INVLD_RSC_NAME;
    public static final long RC_RSC_CRT_FAIL_INVALID_NODE_ID        = MASK_RSC | CRT_FAIL_INVLD_NODE_ID;
    public static final long RC_RSC_CRT_FAIL_INVALID_VLM_NR         = MASK_RSC | CRT_FAIL_INVLD_VLM_NR;
    public static final long RC_RSC_CRT_FAIL_INVALID_STOR_POOL_NAME = MASK_RSC | CRT_FAIL_INVLD_STOR_POOL_NAME;

    public static final long RC_RSC_CRT_FAIL_NOT_FOUND_NODE             = MASK_RSC | CRT_FAIL_NOT_FOUND_NODE;
    public static final long RC_RSC_CRT_FAIL_NOT_FOUND_RSC_DFN          = MASK_RSC | CRT_FAIL_NOT_FOUND_RSC_DFN;
    public static final long RC_RSC_CRT_FAIL_NOT_FOUND_STOR_POOL_DFN    = MASK_RSC | CRT_FAIL_NOT_FOUND_STOR_POOL_DFN;
    public static final long RC_RSC_CRT_FAIL_NOT_FOUND_STOR_POOL        = MASK_RSC | CRT_FAIL_NOT_FOUND_STOR_POOL;

    public static final long RC_RSC_CRT_FAIL_ACC_DENIED_NODE            = MASK_RSC | CRT_FAIL_ACC_DENIED_NODE;
    public static final long RC_RSC_CRT_FAIL_ACC_DENIED_RSC_DFN         = MASK_RSC | CRT_FAIL_ACC_DENIED_RSC_DFN;
    public static final long RC_RSC_CRT_FAIL_ACC_DENIED_RSC             = MASK_RSC | CRT_FAIL_ACC_DENIED_RSC;
    public static final long RC_RSC_CRT_FAIL_ACC_DENIED_VLM_DFN         = MASK_RSC | CRT_FAIL_ACC_DENIED_VLM_DFN;
    public static final long RC_RSC_CRT_FAIL_ACC_DENIED_VLM             = MASK_RSC | CRT_FAIL_ACC_DENIED_VLM;
    public static final long RC_RSC_CRT_FAIL_ACC_DENIED_STOR_POOL_DFN   = MASK_RSC | CRT_FAIL_ACC_DENIED_STOR_POOL_DFN;
    public static final long RC_RSC_CRT_FAIL_ACC_DENIED_STOR_POOL       = MASK_RSC | CRT_FAIL_ACC_DENIED_STOR_POOL;

    public static final long RC_RSC_CRT_FAIL_EXISTS_NODE        = MASK_RSC | CRT_FAIL_EXISTS_NODE;
    public static final long RC_RSC_CRT_FAIL_EXISTS_RSC         = MASK_RSC | CRT_FAIL_EXISTS_RSC;
    public static final long RC_RSC_CRT_FAIL_IMPL_ERROR         = MASK_RSC | CRT_FAIL_IMPL_ERROR;

    public static final long RC_RSC_DEL_FAIL_SQL                = MASK_RSC | DEL_FAIL_SQL;
    public static final long RC_RSC_DEL_FAIL_SQL_ROLLBACK       = MASK_RSC | DEL_FAIL_SQL_ROLLBACK;

    public static final long RC_RSC_DEL_FAIL_INVALID_NODE_NAME  = MASK_RSC | DEL_FAIL_INVLD_NODE_NAME;
    public static final long RC_RSC_DEL_FAIL_INVALID_RSC_NAME   = MASK_RSC | DEL_FAIL_INVLD_RSC_NAME;

    public static final long RC_RSC_DEL_FAIL_NOT_FOUND_NODE     = MASK_RSC | DEL_FAIL_NOT_FOUND_NODE;
    public static final long RC_RSC_DEL_FAIL_NOT_FOUND_RSC_DFN  = MASK_RSC | DEL_FAIL_NOT_FOUND_RSC_DFN;

    public static final long RC_RSC_DEL_FAIL_ACC_DENIED_NODE    = MASK_RSC | DEL_FAIL_ACC_DENIED_NODE;
    public static final long RC_RSC_DEL_FAIL_ACC_DENIED_RSC_DFN = MASK_RSC | DEL_FAIL_ACC_DENIED_RSC_DFN;
    public static final long RC_RSC_DEL_FAIL_ACC_DENIED_RSC     = MASK_RSC | DEL_FAIL_ACC_DENIED_RSC;
    public static final long RC_RSC_DEL_FAIL_ACC_DENIED_VLM_DFN = MASK_RSC | DEL_FAIL_ACC_DENIED_VLM_DFN;

    public static final long RC_RSC_DEL_FAIL_IMPL_ERROR         = MASK_RSC | DEL_FAIL_IMPL_ERROR;


    /*
     * VolumeDefinition return codes
     */
    public static final long RC_VLM_DFN_CREATED = MASK_VLM_DFN | CREATED;
    public static final long RC_VLM_DFN_DELETED = MASK_VLM_DFN | DELETED;

    /*
     * Volume return codes
     */
    public static final long RC_VLM_CREATED = MASK_VLM | CREATED;
    public static final long RC_VLM_DELETED = MASK_VLM | DELETED;

    /*
     * NodeConnection return codes
     */
    public static final long RC_NODE_CONN_CREATED   = MASK_NODE_CONN | CREATED;
    public static final long RC_NODE_CONN_DELETED   = MASK_NODE_CONN | DELETED;

    /*
     * ResourceConnection return codes
     */
    public static final long RC_RSC_CONN_CREATED    = MASK_RSC_CONN | CREATED;
    public static final long RC_RSC_CONN_DELETED    = MASK_RSC_CONN | DELETED;

    /*
     * VolumeConnection return codes
     */
    public static final long RC_VLM_CONN_CREATED    = MASK_VLM_CONN | CREATED;
    public static final long RC_VLM_CONN_DELETED    = MASK_VLM_CONN | DELETED;

    /*
     * NetInterface return codes
     */
    public static final long RC_NET_IF_CREATED  = MASK_NET_IF | CREATED;
    public static final long RC_NET_IF_DELETED  = MASK_NET_IF | DELETED;

    /*
     * StorPoolDefinition return codes
     */
    public static final long RC_STOR_POOL_DFN_CREATED                           = MASK_STOR_POOL_DFN | CREATED;
    public static final long RC_STOR_POOL_DFN_DELETED                           = MASK_STOR_POOL_DFN | DELETED;

    public static final long RC_STOR_POOL_DFN_DEL_NOT_FOUND                     = MASK_STOR_POOL_DFN | DEL_NOT_FOUND;

    public static final long RC_STOR_POOL_DFN_CRT_FAIL_SQL                      = MASK_STOR_POOL_DFN | CRT_FAIL_SQL;
    public static final long RC_STOR_POOL_DFN_CRT_FAIL_SQL_ROLLBACK             = MASK_STOR_POOL_DFN | CRT_FAIL_SQL_ROLLBACK;
    public static final long RC_STOR_POOL_DFN_CRT_FAIL_INVLD_STOR_POOL_NAME     = MASK_STOR_POOL_DFN | CRT_FAIL_INVLD_STOR_POOL_NAME;
    public static final long RC_STOR_POOL_DFN_CRT_FAIL_ACC_DENIED_STOR_POOL_DFN = MASK_STOR_POOL_DFN | CRT_FAIL_ACC_DENIED_STOR_POOL_DFN;
    public static final long RC_STOR_POOL_DFN_CRT_FAIL_EXISTS_STOR_POOL_DFN     = MASK_STOR_POOL_DFN | CRT_FAIL_EXISTS_STOR_POOL_DFN;

    public static final long RC_STOR_POOL_DFN_DEL_FAIL_SQL                      = MASK_STOR_POOL_DFN | DEL_FAIL_SQL;
    public static final long RC_STOR_POOL_DFN_DEL_FAIL_SQL_ROLLBACK             = MASK_STOR_POOL_DFN | DEL_FAIL_SQL_ROLLBACK;
    public static final long RC_STOR_POOL_DFN_DEL_FAIL_INVLD_STOR_POOL_NAME     = MASK_STOR_POOL_DFN | DEL_FAIL_INVLD_STOR_POOL_NAME;
    public static final long RC_STOR_POOL_DFN_DEL_FAIL_ACC_DENIED_STOR_POOL_DFN = MASK_STOR_POOL_DFN | DEL_FAIL_ACC_DENIED_STOR_POOL_DFN;
    public static final long RC_STOR_POOL_DFN_DEL_FAIL_IMPL_ERROR               = MASK_STOR_POOL_DFN | DEL_FAIL_IMPL_ERROR;

    /*
     * StorPool return codes
     */
    public static final long RC_STOR_POOL_CREATED                           = MASK_STOR_POOL | CREATED;
    public static final long RC_STOR_POOL_DELETED                           = MASK_STOR_POOL | DELETED;

    public static final long RC_STOR_POOL_DEL_NOT_FOUND                     = MASK_STOR_POOL | DEL_NOT_FOUND;

    public static final long RC_STOR_POOL_CRT_FAIL_SQL                      = MASK_STOR_POOL | CRT_FAIL_SQL;
    public static final long RC_STOR_POOL_CRT_FAIL_SQL_ROLLBACK             = MASK_STOR_POOL | CRT_FAIL_SQL_ROLLBACK;
    public static final long RC_STOR_POOL_CRT_FAIL_NOT_FOUND_NODE           = MASK_STOR_POOL | CRT_FAIL_NOT_FOUND_NODE;
    public static final long RC_STOR_POOL_CRT_FAIL_NOT_FOUND_STOR_POOL_DFN  = MASK_STOR_POOL | CRT_FAIL_NOT_FOUND_STOR_POOL_DFN;
    public static final long RC_STOR_POOL_CRT_FAIL_INVLD_NODE_NAME          = MASK_STOR_POOL | CRT_FAIL_INVLD_NODE_NAME;
    public static final long RC_STOR_POOL_CRT_FAIL_INVLD_STOR_POOL_NAME     = MASK_STOR_POOL | CRT_FAIL_INVLD_STOR_POOL_NAME;
    public static final long RC_STOR_POOL_CRT_FAIL_ACC_DENIED_NODE          = MASK_STOR_POOL | CRT_FAIL_ACC_DENIED_NODE;
    public static final long RC_STOR_POOL_CRT_FAIL_ACC_DENIED_STOR_POOL_DFN = MASK_STOR_POOL | CRT_FAIL_ACC_DENIED_STOR_POOL_DFN;
    public static final long RC_STOR_POOL_CRT_FAIL_ACC_DENIED_STOR_POOL     = MASK_STOR_POOL | CRT_FAIL_ACC_DENIED_STOR_POOL;
    public static final long RC_STOR_POOL_CRT_FAIL_EXISTS_STOR_POOL         = MASK_STOR_POOL | CRT_FAIL_EXISTS_STOR_POOL;
    public static final long RC_STOR_POOL_CRT_FAIL_IMPL_ERROR               = MASK_STOR_POOL | CRT_FAIL_IMPL_ERROR;

    public static final long RC_STOR_POOL_DEL_FAIL_SQL                      = MASK_STOR_POOL | DEL_FAIL_SQL;
    public static final long RC_STOR_POOL_DEL_FAIL_SQL_ROLLBACK             = MASK_STOR_POOL | DEL_FAIL_SQL_ROLLBACK;
    public static final long RC_STOR_POOL_DEL_FAIL_NOT_FOUND_NODE           = MASK_STOR_POOL | DEL_FAIL_NOT_FOUND_NODE;
    public static final long RC_STOR_POOL_DEL_FAIL_NOT_FOUND_STOR_POOL_DFN  = MASK_STOR_POOL | DEL_FAIL_NOT_FOUND_STOR_POOL_DFN;
    public static final long RC_STOR_POOL_DEL_FAIL_INVLD_STOR_POOL_NAME     = MASK_STOR_POOL | DEL_FAIL_INVLD_STOR_POOL_NAME;
    public static final long RC_STOR_POOL_DEL_FAIL_INVLD_NODE_NAME          = MASK_STOR_POOL | DEL_FAIL_INVLD_NODE_NAME;
    public static final long RC_STOR_POOL_DEL_FAIL_ACC_DENIED_NODE          = MASK_STOR_POOL | DEL_FAIL_ACC_DENIED_NODE;
    public static final long RC_STOR_POOL_DEL_FAIL_ACC_DENIED_STOR_POOL_DFN = MASK_STOR_POOL | DEL_FAIL_ACC_DENIED_STOR_POOL_DFN;
    public static final long RC_STOR_POOL_DEL_FAIL_ACC_DENIED_STOR_POOL     = MASK_STOR_POOL | DEL_FAIL_ACC_DENIED_STOR_POOL;
    public static final long RC_STOR_POOL_DEL_FAIL_IN_USE                   = MASK_STOR_POOL | DEL_FAIL_IN_USE;
    public static final long RC_STOR_POOL_DEL_FAIL_IMPL_ERROR               = MASK_STOR_POOL | DEL_FAIL_IMPL_ERROR;


    // API error codes
    public static final long RC_SIGNIN_PASS = 0x0000000000000100L;
    public static final long RC_SIGNIN_FAIL = 0xC000000000000100L;
}
