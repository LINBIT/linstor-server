package com.linbit.linstor;

import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.identifier.ExternalFileName;
import com.linbit.linstor.core.identifier.KeyValueStoreName;
import com.linbit.linstor.core.identifier.NetInterfaceName;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.identifier.ResourceGroupName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.ScheduleName;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.NetInterface.EncryptionType;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.types.LsIpAddress;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.kinds.RaidLevel;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.event.Level;

public class LinstorParsingUtils
{
    private LinstorParsingUtils()
    {
    }

    /**
     * Returns the given String as a {@link NodeName} if possible. If the String is not a valid
     * {@link NodeName} an exception is thrown.
     *
     * @param nodeNameStr
     * @return
     */
    public static NodeName asNodeName(String nodeNameStr)
    {
        NodeName nodeName;
        try
        {
            nodeName = new NodeName(nodeNameStr);
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_INVLD_NODE_NAME, "The given node name '" + nodeNameStr + "' is invalid."
            ), invalidNameExc);
        }
        return nodeName;
    }

    public static Node.Type asNodeType(String nodeTypeStr)
    {
        Node.Type nodeType;
        try
        {
            nodeType = Node.Type.valueOfIgnoreCase(nodeTypeStr, Node.Type.SATELLITE);
        }
        catch (IllegalArgumentException illegalArgExc)
        {
            throw new ApiRcException(
                ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.FAIL_INVLD_NODE_TYPE,
                        "The specified node type '" + nodeTypeStr + "' is invalid."
                    )
                    .setCorrection(
                        "Valid node types are:\n" +
                            Node.Type.CONTROLLER.name() + "\n" +
                            Node.Type.SATELLITE.name() + "\n" +
                            Node.Type.COMBINED.name() + "\n" +
                            Node.Type.AUXILIARY.name() + "\n"
                    )
                    .build(),
                illegalArgExc
            );
        }
        return nodeType;
    }

    public static EncryptionType asEncryptionType(String encryptionTypeStr)
    {
        EncryptionType encryptionType;
        try
        {
            encryptionType = EncryptionType.valueOfIgnoreCase(encryptionTypeStr);
        }
        catch (Exception exc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_INVLD_NET_TYPE,
                "The given encryption type '" + encryptionTypeStr + "' is invalid."
            ), exc);
        }
        return encryptionType;
    }


    /**
     * Returns the given String as a {@link NetInterfaceName} if possible. If the String is not a valid
     * {@link NetInterfaceName} an exception is thrown.
     *
     * @param netIfNameStr
     * @return
     */
    public static NetInterfaceName asNetInterfaceName(String netIfNameStr)
    {
        NetInterfaceName netInterfaceName;
        try
        {
            netInterfaceName = new NetInterfaceName(netIfNameStr);
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_INVLD_NET_NAME,
                "The specified net interface name '" + netIfNameStr + "' is invalid."
            ), invalidNameExc);
        }
        return netInterfaceName;
    }

    /**
     * Returns the given String as a {@link LsIpAddress} if possible. If the String is not a valid
     * {@link LsIpAddress} an exception is thrown.
     *
     * @param ipAddrStr
     * @return
     */
    public static LsIpAddress asLsIpAddress(String ipAddrStr)
    {
        LsIpAddress lsIpAddress;
        try
        {
            lsIpAddress = new LsIpAddress(ipAddrStr);
        }
        catch (InvalidIpAddressException | NullPointerException invalidIpExc)
        {
            throw new ApiRcException(ApiCallRcImpl
                .entryBuilder(ApiConsts.FAIL_INVLD_NET_ADDR, "Failed to parse IP address")
                .setCause("The specified IP address is not valid")
                .setDetails("The specified input '" + ipAddrStr + "' is not a valid IP address.")
                .setCorrection("Specify a valid IPv4 or IPv6 address.")
                .build(),
                invalidIpExc
            );
        }
        return lsIpAddress;
    }

    /**
     * Returns the given String as a {@link LsIpAddress} if possible. If the String is not a valid
     * {@link LsIpAddress} an exception is thrown.
     *
     * @return
     */
    public static TcpPortNumber asTcpPortNumber(int port)
    {
        TcpPortNumber tcpPortNumber;
        try
        {
            tcpPortNumber = new TcpPortNumber(port);
        }
        catch (Exception exc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_INVLD_NET_PORT,
                "The given portNumber '" + port + "' is invalid."
            ), exc);
        }
        return tcpPortNumber;
    }

    /**
     * Returns the given String as a {@link ResourceName} if possible. If the String is not a valid
     * {@link ResourceName} an exception is thrown.
     *
     * @param rscNameStr
     * @return
     */
    public static ResourceName asRscName(String rscNameStr)
    {
        ResourceName resourceName;
        try
        {
            resourceName = new ResourceName(rscNameStr);
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_INVLD_RSC_NAME,
                "The specified resource name '" + rscNameStr + "' is invalid."
            ), invalidNameExc);
        }
        return resourceName;
    }

    /**
     * Returns the given int as a {@link VolumeNumber} if possible. If the int is not a valid
     * {@link VolumeNumber} an exception is thrown.
     *
     * @param vlmNr
     * @return
     */
    public static VolumeNumber asVlmNr(int vlmNr)
    {
        VolumeNumber volumeNumber;
        try
        {
            volumeNumber = new VolumeNumber(vlmNr);
        }
        catch (ValueOutOfRangeException valOutOfRangeExc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_INVLD_VLM_NR,
                "The given volume number '" + vlmNr + "' is invalid. Valid range from " + VolumeNumber.VOLUME_NR_MIN +
                    " to " + VolumeNumber.VOLUME_NR_MAX
            ), valOutOfRangeExc);
        }
        return volumeNumber;
    }

    /**
     * Returns the given String as a {@link StorPoolName} if possible. If the String is not a valid
     * {@link StorPoolName} an exception is thrown.
     *
     * @param storPoolNameStr
     * @return
     */
    public static StorPoolName asStorPoolName(String storPoolNameStr)
    {
        StorPoolName storPoolName;
        try
        {
            storPoolName = new StorPoolName(storPoolNameStr);
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_INVLD_STOR_POOL_NAME,
                "The given storage pool name '" + storPoolNameStr + "' is invalid."
            ), invalidNameExc);
        }
        return storPoolName;
    }

    /**
     * Returns the given String as a {@link SnapshotName} if possible. If the String is not a valid
     * {@link SnapshotName} an exception is thrown.
     */
    public static SnapshotName asSnapshotName(String snapshotNameStr)
    {
        SnapshotName snapshotName;
        try
        {
            snapshotName = new SnapshotName(snapshotNameStr);
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_INVLD_SNAPSHOT_NAME,
                "The given snapshot name '" + snapshotNameStr + "' is invalid."
            ), invalidNameExc);
        }
        return snapshotName;
    }

    public static SharedStorPoolName asSharedStorPoolName(String sharedStorPoolNameStr)
    {
        SharedStorPoolName sharedStorPoolName;
        try
        {
            sharedStorPoolName = new SharedStorPoolName(sharedStorPoolNameStr);
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_INVLD_FREE_SPACE_MGR_NAME,
                "The given free space manager name '" + sharedStorPoolNameStr + "' is invalid."
            ), invalidNameExc);
        }
        return sharedStorPoolName;
    }

    /**
     * Returns the given String as a {@link KeyValueStoreName} if possible. If the String is not a valid
     * {@link KeyValueStoreName} an exception is thrown.
     */
    public static KeyValueStoreName asKvsName(String kvsNameStr)
    {
        KeyValueStoreName kvsName;
        try
        {
            kvsName = new KeyValueStoreName(kvsNameStr);
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_KVS_NAME, "The given keyValueStore name '" + kvsNameStr + "' is invalid."
            ), invalidNameExc);
        }
        return kvsName;
    }

    public static @Nullable DeviceLayerKind asDeviceLayerKindOrNull(final String layerName)
    {
        DeviceLayerKind kind;
        switch (layerName.toUpperCase())
        {
            case "DRBD":
                kind = DeviceLayerKind.DRBD;
                break;
            case "CRYPT": // fall-through
            case "CRYPT_SETUP": // fall-through
            case "LUKS":
                kind = DeviceLayerKind.LUKS;
                break;
            case "STORAGE": // fall-through
            case "LVM": // fall-through
            case "SPDK": // fall-through
            case "ZFS":
                kind = DeviceLayerKind.STORAGE;
                break;
            case "NVME":
                kind = DeviceLayerKind.NVME;
                break;
            case "WRITECACHE":
                kind = DeviceLayerKind.WRITECACHE;
                break;
            case "CACHE":
                kind = DeviceLayerKind.CACHE;
                break;
            case "BCACHE":
                kind = DeviceLayerKind.BCACHE;
                break;
            default:
                kind = null;
                break;
        }
        return kind;
    }

    public static DeviceLayerKind asDeviceLayerKind(final String layerName)
    {
        DeviceLayerKind kind = asDeviceLayerKindOrNull(layerName);
        if (kind == null)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_LAYER_KIND,
                    "Given layer kind '" + layerName + "' is invalid"
                )
            );
        }
        return kind;
    }

    public static List<DeviceLayerKind> asDeviceLayerKind(List<String> layerStackStrListRef)
    {
        List<DeviceLayerKind> ret = new ArrayList<>();
        if (layerStackStrListRef != null)
        {
            for (String layerStr : layerStackStrListRef)
            {
                ret.add(asDeviceLayerKind(layerStr));
            }
        }
        return ret;
    }

    public static DeviceProviderKind asProviderKind(String stringRef)
    {
        DeviceProviderKind kind;

        if (stringRef == null)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_LAYER_KIND,
                    "Given provider kind is null."
                )
            );
        }

        switch (stringRef.toUpperCase())
        {
            case "DRBD_DISKLESS":
                // fall-through
            case "DRBDDISKLESS":
                // fall-through
            case "DISKLESS":
                kind = DeviceProviderKind.DISKLESS;
                break;
            case "LVM":
                kind = DeviceProviderKind.LVM;
                break;
            case "LVMTHIN":
                // fall-through
            case "LVM_THIN":
                kind = DeviceProviderKind.LVM_THIN;
                break;
            case "ZFS":
                kind = DeviceProviderKind.ZFS;
                break;
            case "ZFSTHIN":
                // fall-through
            case "ZFS_THIN":
                kind = DeviceProviderKind.ZFS_THIN;
                break;
            case "FILE":
                kind = DeviceProviderKind.FILE;
                break;
            case "FILE_THIN":
                kind = DeviceProviderKind.FILE_THIN;
                break;
            case "SPDK":
                kind = DeviceProviderKind.SPDK;
                break;
            case "REMOTE_SPDK":
                kind = DeviceProviderKind.REMOTE_SPDK;
                break;
            case "EBS_TARGET":
                kind = DeviceProviderKind.EBS_TARGET;
                break;
            case "EBS_INIT":
                kind = DeviceProviderKind.EBS_INIT;
                break;
            case "STORAGE_SPACES":
            case "STORAGE_SPACES_TARGET":
                kind = DeviceProviderKind.STORAGE_SPACES;
                break;
            case "STORAGE_SPACES_THIN":
            case "STORAGE_SPACES_THIN_TARGET":
                kind = DeviceProviderKind.STORAGE_SPACES_THIN;
                break;
            default:
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_INVLD_LAYER_KIND,
                        "Given provider kind '" + stringRef + "' is invalid"
                    )
                );
        }
        return kind;
    }

    public static List<DeviceProviderKind> asProviderKind(List<String> providerStackStrListRef)
    {
        List<DeviceProviderKind> ret = new ArrayList<>();
        for (String providerStr : providerStackStrListRef)
        {
            ret.add(asProviderKind(providerStr));
        }
        return ret;
    }

    public static RaidLevel asRaidLevel(String raidLevelStr)
    {
        if (raidLevelStr == null)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_UNKNOWN_ERROR,
                    "Given RAID level string is null."
                )
            );
        }

        return RaidLevel.valueOf(raidLevelStr.toUpperCase());
    }

    public static ResourceGroupName asRscGrpName(String rscGrpNameStr)
    {
        ResourceGroupName rscGrpName;
        try
        {
            rscGrpName = new ResourceGroupName(rscGrpNameStr);
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_RSC_GRP_NAME,
                    "The given resource group name '" + rscGrpNameStr + "' is invalid."
                ),
                invalidNameExc
            );
        }
        return rscGrpName;
    }

    public static RemoteName asRemoteName(String remoteNameStr)
    {
        RemoteName remoteName;
        try
        {
            remoteName = new RemoteName(remoteNameStr);
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_REMOTE_NAME,
                    "The given remote name '" + remoteNameStr + "' is invalid."
                ),
                invalidNameExc
            );
        }
        return remoteName;
    }

    public static ScheduleName asScheduleName(String scheduleNameStr)
    {
        ScheduleName scheduleName;
        try
        {
            scheduleName = new ScheduleName(scheduleNameStr);
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_SCHEDULE_NAME,
                    "The given schedule name '" + scheduleNameStr + "' is invalid."
                ),
                invalidNameExc
            );
        }
        return scheduleName;
    }

    public static Level asLogLevel(String logLevelRef)
    {
        Level logLevel;
        if (logLevelRef == null || logLevelRef.isEmpty())
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_CONF,
                    "Given loglevel is null."
                )
            );
        }

        switch (logLevelRef.toUpperCase())
        {
            case "ERROR":
                logLevel = Level.ERROR;
                break;
            case "WARN":
                logLevel = Level.WARN;
                break;
            case "INFO":
                logLevel = Level.INFO;
                break;
            case "DEBUG":
                logLevel = Level.DEBUG;
                break;
            case "TRACE":
                logLevel = Level.TRACE;
                break;
            default:
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_INVLD_CONF,
                        "Given loglevel '" + logLevelRef + "' is invalid"
                    )
                );
        }
        return logLevel;
    }

    public static ExternalFileName asExtFileName(String pathRef)
    {
        ExternalFileName extFileName;
        try
        {
            extFileName = new ExternalFileName(pathRef);
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_EXT_FILE_NAME,
                    "The given external file name '" + pathRef + "' is invalid."
                ),
                invalidNameExc
            );
        }
        return extFileName;
    }
}
