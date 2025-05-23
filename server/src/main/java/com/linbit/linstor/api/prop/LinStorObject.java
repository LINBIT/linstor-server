package com.linbit.linstor.api.prop;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiConsts;

public enum LinStorObject
{
    // ctrl- & stlt-path do not need trailing slashes, because unlike all the others they are not prefixes
    // but the full path
    CTRL(ApiConsts.MASK_CTRL_CONF, "/CTRL"),
    STLT(ApiConsts.MASK_CTRL_CONF, "/STLT"), // TODO: migration for adding leading slash

    NODE(ApiConsts.MASK_NODE, "/NODES/"),
    NODE_CONN(ApiConsts.MASK_NODE_CONN, "/NODE_CONNS/"),
    RSC_GRP(ApiConsts.MASK_RSC_GRP, "/RSC_GRPS/"),
    RSC_DFN(ApiConsts.MASK_RSC_DFN, "/RSC_DFNS/"),
    RSC(ApiConsts.MASK_RSC, "/RSCS/"),
    RSC_CONN(ApiConsts.MASK_RSC_CONN, "/RSC_CONNS/"),
    VLM_GRP(ApiConsts.MASK_VLM_GRP, "/VLM_GRPS/"),
    VLM_DFN(ApiConsts.MASK_VLM_DFN, "/VLM_DFNS/"),
    VLM(ApiConsts.MASK_VLM, "/VLMS/"),
    VLM_CONN(ApiConsts.MASK_VLM_CONN, "/VLM_CONNS/"),
    STOR_POOL(ApiConsts.MASK_STOR_POOL, "/STOR_POOLS/"),
    STOR_POOL_DFN(ApiConsts.MASK_STOR_POOL_DFN, "/STOR_POOL_DFNS/"),
    SNAP_DFN(0, "/SNAP_DFNS/"),
    SNAP_DFN_RSC_DFN(0, "/SNAP_DFNS_RSC_DFN/"),
    SNAP(ApiConsts.MASK_SNAPSHOT, "/SNAPS/"),
    SNAP_RSC(ApiConsts.MASK_SNAPSHOT, "/SNAPS_RSC/"),
    SNAP_VLM(ApiConsts.MASK_SNAPSHOT, "/SNAP_VLMS/"),
    SNAP_VLM_VLM(ApiConsts.MASK_SNAPSHOT, "/SNAP_VLMS_VLM/"),
    SNAP_VLM_DFN(0, "/SNAP_VLM_DFNS/"),
    SNAP_VLM_DFN_VLM_DFN(0, "/SNAP_VLM_DFNS_VLM_DFN/"),
    KVS(ApiConsts.MASK_KVS, "/KVS/"),

    // the following objects do not have their own props-containers and therefore need no props-path
    NET_IF(ApiConsts.MASK_NET_IF, null),
    // The various DRBD Proxy configuration sections are considered separate objects for the purposes of setting
    // properties, even though the properties are stored in the resource definition
    DRBD_PROXY(0, null),
    DRBD_PROXY_ZSTD(0, null),
    DRBD_PROXY_ZLIB(0, null),
    DRBD_PROXY_LZMA(0, null),
    DRBD_PROXY_LZ4(0, null),
    EMPTY_RO_PROPS( 0, null);

    public final long apiMask;
    public final @Nullable String path;

    LinStorObject(long apiMaskRef, @Nullable String pathRef)
    {
        apiMask = apiMaskRef;
        path = pathRef;
    }

    public static @Nullable LinStorObject drbdProxyCompressionObject(String compressionType)
    {
        LinStorObject linStorObject;
        switch (compressionType)
        {
            case ApiConsts.VAL_DRBD_PROXY_COMPRESSION_ZSTD:
                linStorObject = LinStorObject.DRBD_PROXY_ZSTD;
                break;
            case ApiConsts.VAL_DRBD_PROXY_COMPRESSION_ZLIB:
                linStorObject = LinStorObject.DRBD_PROXY_ZLIB;
                break;
            case ApiConsts.VAL_DRBD_PROXY_COMPRESSION_LZMA:
                linStorObject = LinStorObject.DRBD_PROXY_LZMA;
                break;
            case ApiConsts.VAL_DRBD_PROXY_COMPRESSION_LZ4:
                linStorObject = LinStorObject.DRBD_PROXY_LZ4;
                break;
            default:
                linStorObject = null;
                break;
        }
        return linStorObject;
    }
}
