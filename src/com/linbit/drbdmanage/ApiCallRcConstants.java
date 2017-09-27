package com.linbit.drbdmanage;

public class ApiCallRcConstants
{
    /*
     * The most significant 2 bits are reserved for MASK_ERROR, MASK_WARN,
     * MASK_INFO and MASK_SUCCESS
     */

    /*
     * The next 4 significant bits are reserved for type (Node, ResDfn, Res,
     * VolDfn, Vol, ConDfn, NetInterface, ...)
     */
    private static final long MASK_NODE                     = 0x3C_00000000000000L;
    private static final long MASK_RESOURCE_DEFINITION      = 0x38_00000000000000L;
    private static final long MASK_RESOURCE                 = 0x34_00000000000000L;
    private static final long MASK_VOLUME_DEFINITION        = 0x30_00000000000000L;
    private static final long MASK_VOLUME                   = 0x2C_00000000000000L;
    private static final long MASK_CONNECTION_DEFINITION    = 0x28_00000000000000L;
    private static final long MASK_NET_INTERFACE            = 0x24_00000000000000L;
    private static final long MASK_STOR_POOL_DEFINITION     = 0x20_00000000000000L;
    private static final long MASK_STOR_POOL                = 0x1C_00000000000000L;
    /*
     *  unused type masks:
      0x18_00000000000000L;
      0x14_00000000000000L;
      0x10_00000000000000L;
      0x0C_00000000000000L;
      0x08_00000000000000L;
      0x04_00000000000000L;
      0x00_00000000000000L; // caution with this one
   */


    /*
     * Node return codes
     */
    public static final long RC_NODE_CREATED = 1 | MASK_NODE;

    public static final long RC_NODE_CREATION_FAILED = RC_NODE_CREATED | ApiCallRc.MASK_ERROR;

    /*
     * ResourceDefinition return codes
     */
    public static final long RC_RESOURCE_DEFINITION_CREATED = 1 | MASK_RESOURCE_DEFINITION;

    public static final long RC_RESOURCE_DEFINITION_CREATION_FAILED = RC_RESOURCE_DEFINITION_CREATED | ApiCallRc.MASK_ERROR;

    /*
     * Resource return codes
     */

    /*
     * VolumeDefinition return codes
     */
    public static final long RC_VOLUME_DEFINITION_CREATED = 1 | MASK_VOLUME_DEFINITION;

    /*
     * Volume return codes
     */

    /*
     * NodeConnectionDefinition return codes
     */

    /*
     * ResourceConnectionDefinition return codes
     */

    /*
     * VolumeConnectionDefinition return codes
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
