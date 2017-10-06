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
     * VolDfn, Vol, ConDfn, NetInterface, ...)
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
     * Node return codes
     */
    public static final long RC_NODE_CREATED            = 1 | MASK_NODE | MASK_INFO;
    public static final long RC_NODE_DELETED            = 2 | MASK_NODE | MASK_INFO;

    public static final long RC_NODE_DEL_NOT_FOUND      = 10 | MASK_NODE | MASK_WARN;

    public static final long RC_NODE_CREATION_FAILED    = RC_NODE_CREATED | MASK_ERROR;
    public static final long RC_NODE_DELETION_FAILED    = RC_NODE_DELETED | MASK_ERROR;

    /*
     * ResourceDefinition return codes
     */
    public static final long RC_RSC_DFN_CREATED         = 1 | MASK_RSC_DFN | MASK_INFO;
    public static final long RC_RSC_DFN_DELETED         = 2 | MASK_RSC_DFN | MASK_INFO;

    public static final long RC_RSC_DFN_NOT_FOUND       = 10 | MASK_RSC_DFN | MASK_WARN;

    public static final long RC_RSC_DFN_CREATION_FAILED = 100 | MASK_RSC_DFN | MASK_ERROR;
    public static final long RC_RSC_DFN_DELETION_FAILED = 200 | MASK_RSC_DFN | MASK_ERROR;

    /*
     * Resource return codes
     */
    public static final long RC_RSC_CREATED                     = 1 | MASK_RSC | MASK_INFO;
    public static final long RC_RSC_DELETED                     = 2 | MASK_RSC | MASK_INFO;

    public static final long RC_RSC_CRT_FAIL_SQL                = 100 | MASK_RSC | MASK_ERROR;
    public static final long RC_RSC_CRT_FAIL_SQL_ROLLBACK       = 101 | MASK_RSC | MASK_ERROR;

    public static final long RC_RSC_CRT_FAIL_INVALID_NODE_NAME  = 110 | MASK_RSC | MASK_ERROR;
    public static final long RC_RSC_CRT_FAIL_INVALID_RSC_NAME   = 111 | MASK_RSC | MASK_ERROR;
    public static final long RC_RSC_CRT_FAIL_INVALID_NODE_ID    = 112 | MASK_RSC | MASK_ERROR;
    public static final long RC_RSC_CRT_FAIL_INVALID_VLM_NR     = 113 | MASK_RSC | MASK_ERROR;

    public static final long RC_RSC_CRT_FAIL_NODE_NOT_FOUND     = 120 | MASK_RSC | MASK_ERROR;
    public static final long RC_RSC_CRT_FAIL_RSC_DFN_NOT_FOUND  = 121 | MASK_RSC | MASK_ERROR;

    public static final long RC_RSC_CRT_FAIL_ACC_DENIED_NODE    = 130 | MASK_RSC | MASK_ERROR;
    public static final long RC_RSC_CRT_FAIL_ACC_DENIED_RSC_DFN = 131 | MASK_RSC | MASK_ERROR;
    public static final long RC_RSC_CRT_FAIL_ACC_DENIED_RSC     = 132 | MASK_RSC | MASK_ERROR;
    public static final long RC_RSC_CRT_FAIL_ACC_DENIED_VLM_DFN = 133 | MASK_RSC | MASK_ERROR;
    public static final long RC_RSC_CRT_FAIL_ACC_DENIED_VLM     = 134 | MASK_RSC | MASK_ERROR;

    public static final long RC_RSC_CRT_FAIL_RSC_EXISTS         = 140 | MASK_RSC | MASK_ERROR;
    public static final long RC_RSC_CRT_FAIL_NODE_EXISTS        = 141 | MASK_RSC | MASK_ERROR;

    public static final long RC_RSC_DEL_FAIL                    = 200 | MASK_RSC | MASK_ERROR;

    /*
     * VolumeDefinition return codes
     */
    public static final long RC_VLM_DFN_CREATED = 1 | MASK_VLM_DFN;

    /*
     * Volume return codes
     */

    /*
     * NodeConnection return codes
     */

    /*
     * ResourceConnection return codes
     */

    /*
     * VolumeConnection return codes
     */

    /*
     * NetInterface return codes
     */

    /*
     * StorPoolDefinition return codes
     */

    /*
     * StorPool return codes
     */

}
