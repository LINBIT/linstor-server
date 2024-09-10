package com.linbit.linstor.api.protobuf;

import com.linbit.ImplementationError;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.RscDfnLayerDataApi;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.interfaces.VlmDfnLayerDataApi;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.api.pojo.BCacheRscPojo;
import com.linbit.linstor.api.pojo.BCacheRscPojo.BCacheVlmPojo;
import com.linbit.linstor.api.pojo.CacheRscPojo;
import com.linbit.linstor.api.pojo.CacheRscPojo.CacheVlmPojo;
import com.linbit.linstor.api.pojo.DrbdRscPojo;
import com.linbit.linstor.api.pojo.DrbdRscPojo.DrbdRscDfnPojo;
import com.linbit.linstor.api.pojo.DrbdRscPojo.DrbdVlmDfnPojo;
import com.linbit.linstor.api.pojo.DrbdRscPojo.DrbdVlmPojo;
import com.linbit.linstor.api.pojo.LuksRscPojo;
import com.linbit.linstor.api.pojo.LuksRscPojo.LuksVlmPojo;
import com.linbit.linstor.api.pojo.NvmeRscPojo;
import com.linbit.linstor.api.pojo.NvmeRscPojo.NvmeVlmPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo.DisklessVlmPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo.EbsVlmPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo.ExosVlmPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo.FileThinVlmPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo.FileVlmPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo.LvmThinVlmPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo.LvmVlmPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo.RemoteSpdkVlmPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo.SpdkVlmPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo.StorageSpacesThinVlmPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo.StorageSpacesVlmPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo.ZfsThinVlmPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo.ZfsVlmPojo;
import com.linbit.linstor.api.pojo.WritecacheRscPojo;
import com.linbit.linstor.api.pojo.WritecacheRscPojo.WritecacheVlmPojo;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apis.StorPoolApi;
import com.linbit.linstor.layer.LayerIgnoreReason;
import com.linbit.linstor.proto.common.BCacheRscOuterClass.BCacheVlm;
import com.linbit.linstor.proto.common.CacheRscOuterClass.CacheVlm;
import com.linbit.linstor.proto.common.DrbdRscOuterClass.DrbdRsc;
import com.linbit.linstor.proto.common.DrbdRscOuterClass.DrbdRscDfn;
import com.linbit.linstor.proto.common.DrbdRscOuterClass.DrbdVlm;
import com.linbit.linstor.proto.common.DrbdRscOuterClass.DrbdVlmDfn;
import com.linbit.linstor.proto.common.LayerTypeOuterClass.LayerType;
import com.linbit.linstor.proto.common.LuksRscOuterClass.LuksVlm;
import com.linbit.linstor.proto.common.NvmeRscOuterClass.NvmeVlm;
import com.linbit.linstor.proto.common.RscDfnOuterClass.RscDfn;
import com.linbit.linstor.proto.common.RscDfnOuterClass.RscDfnLayerData;
import com.linbit.linstor.proto.common.RscLayerDataOuterClass.RscLayerData;
import com.linbit.linstor.proto.common.StorageRscOuterClass.StorageVlm;
import com.linbit.linstor.proto.common.StorageRscOuterClass.StorageVlmDfn;
import com.linbit.linstor.proto.common.VlmDfnOuterClass.VlmDfnLayerData;
import com.linbit.linstor.proto.common.WritecacheRscOuterClass.WritecacheVlm;
import com.linbit.utils.Pair;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class ProtoLayerUtils
{
    public static RscLayerDataApi extractRscLayerData(
        RscLayerData protoRscData,
        long fullSyncId,
        long updateId
    )
    {
        RscLayerDataApi ret;
        switch (protoRscData.getLayerType())
        {
            case DRBD:
                {
                    if (protoRscData.hasDrbd())
                    {
                        DrbdRsc protoDrbdRsc = protoRscData.getDrbd();

                        DrbdRscDfn protoDrbdRscDfn = protoDrbdRsc.getDrbdRscDfn();
                        // rscDfn does not have to be the same reference, just contain the data
                        DrbdRscDfnPojo drbdRscDfnPojo = extractDrbdRscDfn(protoDrbdRscDfn);
                        DrbdRscPojo drbdRscPojo = new DrbdRscPojo(
                            protoRscData.getId(),
                            new ArrayList<>(),
                            protoRscData.getRscNameSuffix(),
                            drbdRscDfnPojo,
                            protoDrbdRsc.getNodeId(),
                            (short) protoDrbdRsc.getPeersSlots(),
                            protoDrbdRsc.getAlStripes(),
                            protoDrbdRsc.getAlSize(),
                            protoDrbdRsc.getFlags(),
                            new ArrayList<>(),
                            protoRscData.getSuspend(),
                            null,
                            null,
                            LayerIgnoreReason.valueOf(protoRscData.getIgnoreReason())
                        );

                        for (DrbdVlm protoDrbdVlm : protoDrbdRsc.getDrbdVlmsList())
                        {
                            drbdRscPojo.getVolumeList().add(extractDrbdVlm(protoDrbdVlm, fullSyncId, updateId));
                        }

                        ret = drbdRscPojo;
                    }
                    else
                    {
                        ret = null;
                    }
                }
                break;
            case LUKS:
                {
                    if (protoRscData.hasLuks())
                    {
                        LuksRscPojo luksRscPojo = new LuksRscPojo(
                            protoRscData.getId(),
                            new ArrayList<>(),
                            protoRscData.getRscNameSuffix(),
                            new ArrayList<>(),
                            protoRscData.getSuspend(),
                            LayerIgnoreReason.valueOf(protoRscData.getIgnoreReason())
                        );
                        for (LuksVlm protoLuksVlm : protoRscData.getLuks().getLuksVlmsList())
                        {
                            luksRscPojo.getVolumeList().add(extractLuksVlm(protoLuksVlm));
                        }

                        ret = luksRscPojo;
                    }
                    else
                    {
                        ret = null;
                    }
                }
                break;
            case STORAGE:
                {
                    if (protoRscData.hasStorage())
                    {
                        StorageRscPojo storageRscPojo = new StorageRscPojo(
                            protoRscData.getId(),
                            new ArrayList<>(),
                            protoRscData.getRscNameSuffix(),
                            new ArrayList<>(),
                            protoRscData.getSuspend(),
                            LayerIgnoreReason.valueOf(protoRscData.getIgnoreReason())
                        );
                        for (StorageVlm protoVlm : protoRscData.getStorage().getStorageVlmsList())
                        {
                            storageRscPojo.getVolumeList().add(
                                extractStorageVlm(
                                    protoVlm,
                                    fullSyncId,
                                    updateId
                                )
                            );
                        }

                        ret = storageRscPojo;
                    }
                    else
                    {
                        ret = null;
                    }
                }
                break;
            case NVME:
            {
                if (protoRscData.hasNvme())
                {
                    NvmeRscPojo nvmeRscPojo = new NvmeRscPojo(
                        protoRscData.getId(),
                        new ArrayList<>(),
                        protoRscData.getRscNameSuffix(),
                        new ArrayList<>(),
                        protoRscData.getSuspend(),
                        LayerIgnoreReason.valueOf(protoRscData.getIgnoreReason())
                    );
                    for (NvmeVlm protoVlm : protoRscData.getNvme().getNvmeVlmsList())
                    {
                        nvmeRscPojo.getVolumeList().add(extractNvmeVlm(protoVlm, fullSyncId, updateId));
                    }

                    ret = nvmeRscPojo;
                }
                else
                {
                    ret = null;
                }
            }
            break;
            case WRITECACHE:
            {
                if (protoRscData.hasWritecache())
                {
                    WritecacheRscPojo writecacheRscPojo = new WritecacheRscPojo(
                        protoRscData.getId(),
                        new ArrayList<>(),
                        protoRscData.getRscNameSuffix(),
                        new ArrayList<>(),
                        protoRscData.getSuspend(),
                        LayerIgnoreReason.valueOf(protoRscData.getIgnoreReason())
                    );
                    List<WritecacheVlmPojo> volumeList = writecacheRscPojo.getVolumeList();
                    for (WritecacheVlm protoVlm : protoRscData.getWritecache().getVlmsList())
                    {
                        volumeList.add(extractWritecacheVlm(protoVlm, fullSyncId, updateId));
                    }
                    ret = writecacheRscPojo;
                }
                else
                {
                    ret = null;
                }
            }
            break;
            case CACHE:
            {
                if (protoRscData.hasCache())
                {
                    CacheRscPojo cacheRscPojo = new CacheRscPojo(
                        protoRscData.getId(),
                        new ArrayList<>(),
                        protoRscData.getRscNameSuffix(),
                        new ArrayList<>(),
                        protoRscData.getSuspend(),
                        LayerIgnoreReason.valueOf(protoRscData.getIgnoreReason())
                    );
                    List<CacheVlmPojo> volumeList = cacheRscPojo.getVolumeList();
                    for (CacheVlm protoVlm : protoRscData.getCache().getVlmsList())
                    {
                        volumeList.add(extractCacheVlm(protoVlm, fullSyncId, updateId));
                    }
                    ret = cacheRscPojo;
                }
                else
                {
                    ret = null;
                }
            }
            break;
        case BCACHE:
        {
            if (protoRscData.hasBcache())
            {
                BCacheRscPojo bcacheRscPojo = new BCacheRscPojo(
                    protoRscData.getId(),
                    new ArrayList<>(),
                    protoRscData.getRscNameSuffix(),
                    new ArrayList<>(),
                    protoRscData.getSuspend(),
                    LayerIgnoreReason.valueOf(protoRscData.getIgnoreReason())
                );
                List<BCacheVlmPojo> volumeList = bcacheRscPojo.getVolumeList();
                for (BCacheVlm protoVlm : protoRscData.getBcache().getVlmsList())
                {
                    volumeList.add(extractBCacheVlm(protoVlm, fullSyncId, updateId));
                }
                ret = bcacheRscPojo;
            }
            else
            {
                ret = null;
            }
        }
            break;
            case UNKNOWN_LAYER: // fall-through
            case UNRECOGNIZED: // fall-through
            default:
                throw new ImplementationError(
                    "Unexpected layer type in proto message: " +
                        protoRscData.getLayerType() +
                        " Layered rsc id: " + protoRscData.getId()
                );
        }

        if (ret != null)
        {
            for (RscLayerData childrenProto : protoRscData.getChildrenList())
            {
                ret.getChildren().add(extractRscLayerData(childrenProto, fullSyncId, updateId));
            }
        }

        return ret;
    }

    public static List<String> layerTypeList2LayerStringList(List<LayerType> layerTypeList)
    {
        List<String> ret = new ArrayList<>();
        for (LayerType type : layerTypeList)
        {
            ret.add(layerType2layerString(type));
        }
        return ret;
    }

    public static String layerType2layerString(LayerType layerType)
    {
        String str;
        switch (layerType)
        {
            case DRBD:
                str = "DRBD";
                break;
            case LUKS:
                str = "LUKS";
                break;
            case STORAGE:
                str = "STORAGE";
                break;
            case NVME:
                str = "NVME";
                break;
            case WRITECACHE:
                str = "WRITECACHE";
                break;
            case CACHE:
                str = "CACHE";
                break;
            case BCACHE:
                str = "BCACHE";
                break;
            case UNKNOWN_LAYER: // fall-through
            case UNRECOGNIZED: // fall-through
            default:
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_INVLD_LAYER_KIND,
                        "Given layer type '" + layerType + "' is invalid"
                    )
                );
        }

        return str;
    }

    public static List<Pair<String, RscDfnLayerDataApi>> extractRscDfnLayerData(RscDfn rscDfnRef)
    {
        List<Pair<String, RscDfnLayerDataApi>> ret = new ArrayList<>();

        for (RscDfnLayerData rscDfnLayerData : rscDfnRef.getLayerDataList())
        {
            RscDfnLayerDataApi rscDfnLayerDataApi;
            switch (rscDfnLayerData.getLayerType())
            {
                case DRBD:
                    if (rscDfnLayerData.hasDrbd())
                    {
                        rscDfnLayerDataApi = extractDrbdRscDfn(rscDfnLayerData.getDrbd());
                    }
                    else
                    {
                        rscDfnLayerDataApi = null;
                    }
                    break;
                case STORAGE: // fall-through
                case LUKS: // fall-through
                case NVME: // fall-through
                case WRITECACHE: // fall-through
                case CACHE:// fall-through
                case BCACHE:
                    rscDfnLayerDataApi = null;
                    break;
                case UNKNOWN_LAYER: // fall-through
                case UNRECOGNIZED: // fall-through
                default:
                    throw new ImplementationError(
                        "Unknown resource definition layer (proto) kind: " + rscDfnLayerData.getLayerType()
                    );
            }
            ret.add(new Pair<>(layerType2layerString(rscDfnLayerData.getLayerType()), rscDfnLayerDataApi));
        }
        return ret;

    }

    public static List<Pair<String, VlmDfnLayerDataApi>> extractVlmDfnLayerData(
        List<VlmDfnLayerData> layerDataListRef
    )
    {
        List<Pair<String, VlmDfnLayerDataApi>> ret = new ArrayList<>();

        for (VlmDfnLayerData vlmDfnLayerData : layerDataListRef)
        {
            VlmDfnLayerDataApi vlmDfnLayerDataApi;
            switch (vlmDfnLayerData.getLayerType())
            {
                case DRBD:
                    if (vlmDfnLayerData.hasDrbd())
                    {
                        vlmDfnLayerDataApi = extractDrbdVlmDfn(vlmDfnLayerData.getDrbd());
                    }
                    else
                    {
                        vlmDfnLayerDataApi = null;
                    }
                    break;
                case STORAGE:
                    if (vlmDfnLayerData.hasStorage())
                    {
                        vlmDfnLayerDataApi = extractStorageVlmDfn(vlmDfnLayerData.getStorage());
                    }
                    else
                    {
                        vlmDfnLayerDataApi = null;
                    }
                    break;
                case LUKS: // fall-through
                case NVME:// fall-through
                case WRITECACHE: // fall-through
                case CACHE:// fall-through
                case BCACHE:
                    vlmDfnLayerDataApi = null;
                    break;
                case UNKNOWN_LAYER: // fall-through
                case UNRECOGNIZED: // fall-through
                default:
                    throw new ImplementationError(
                        "Unknown volume definition layer (proto) kind: " + vlmDfnLayerData.getLayerType()
                    );
            }
            ret.add(new Pair<>(layerType2layerString(vlmDfnLayerData.getLayerType()), vlmDfnLayerDataApi));
        }
        return ret;
    }

    private static DrbdRscDfnPojo extractDrbdRscDfn(DrbdRscDfn protoDrbdRscDfn)
    {
        DrbdRscDfnPojo drbdRscDfnPojo = new DrbdRscDfnPojo(
            protoDrbdRscDfn.getRscNameSuffix(),
            (short) protoDrbdRscDfn.getPeersSlots(),
            protoDrbdRscDfn.getAlStripes(),
            protoDrbdRscDfn.getAlSize(),
            protoDrbdRscDfn.getPort(),
            protoDrbdRscDfn.getTransportType(),
            protoDrbdRscDfn.getSecret(),
            protoDrbdRscDfn.getDown()
        );
        return drbdRscDfnPojo;
    }

    private static DrbdVlmPojo extractDrbdVlm(DrbdVlm protoDrbdVlm, long fullSyncId, long updateId)
    {
        DrbdVlmDfn protoDrbdVlmDfn = protoDrbdVlm.getDrbdVlmDfn();
        return new DrbdVlmPojo(
            extractDrbdVlmDfn(protoDrbdVlmDfn),
            protoDrbdVlm.getDevicePath(),
            protoDrbdVlm.getBackingDevice(),
            protoDrbdVlm.hasExternalMetaDataStorPool() ?
                protoDrbdVlm.getExternalMetaDataStorPool() :
                null,
            protoDrbdVlm.getMetaDisk(),
            protoDrbdVlm.getAllocatedSize(),
            protoDrbdVlm.getUsableSize(),
            protoDrbdVlm.getDiskState(),
            protoDrbdVlm.getDiscGran(),
            false
        );
    }

    private static DrbdVlmDfnPojo extractDrbdVlmDfn(DrbdVlmDfn protoDrbdVlmDfn)
    {
        return new DrbdVlmDfnPojo(
            protoDrbdVlmDfn.getRscNameSuffix(),
            protoDrbdVlmDfn.getVlmNr(),
            protoDrbdVlmDfn.getMinor()
        );
    }

    private static LuksVlmPojo extractLuksVlm(LuksVlm protoLuksVlm)
    {
        @Nullable byte[] modifyPassword = protoLuksVlm.hasModifyPassword() ?
            ProtoDeserializationUtils.extractByteArray(protoLuksVlm.getModifyPassword()) : null;
        return new LuksVlmPojo(
            protoLuksVlm.getVlmNr(),
            ProtoDeserializationUtils.extractByteArray(protoLuksVlm.getEncryptedPassword()),
            protoLuksVlm.getDevicePath(),
            protoLuksVlm.getDataDevice(),
            protoLuksVlm.getAllocatedSize(),
            protoLuksVlm.getUsableSize(),
            protoLuksVlm.getOpened(),
            protoLuksVlm.getDiskState(),
            protoLuksVlm.getDiscGran(),
            false,
            modifyPassword
        );
    }

    private static VlmLayerDataApi extractStorageVlm(
        StorageVlm protoVlm,
        long fullSyncId,
        long updateId
    )
        throws ImplementationError
    {
        VlmLayerDataApi ret;
        int vlmNr = protoVlm.getVlmNr();
        String devicePath = protoVlm.getDevicePath();
        long allocatedSize = protoVlm.getAllocatedSize();
        long usableSize = protoVlm.getUsableSize();
        String diskState = protoVlm.getDiskState();
        long discGran = protoVlm.getDiscGran();
        boolean exists = protoVlm.getExists();
        StorPoolApi storPoolApi = ProtoDeserializationUtils.parseStorPool(
            protoVlm.getStoragePool(),
            fullSyncId,
            updateId
        );
        switch (storPoolApi.getDeviceProviderKind())
        {
            case DISKLESS:
                ret = new DisklessVlmPojo(
                    vlmNr,
                    devicePath,
                    allocatedSize,
                    usableSize,
                    null,
                    null,
                    null,
                    discGran,
                    storPoolApi,
                    exists
                );
                break;
            case LVM:
                ret = new LvmVlmPojo(
                    vlmNr,
                    devicePath,
                    allocatedSize,
                    usableSize,
                    null,
                    null,
                    diskState,
                    discGran,
                    storPoolApi,
                    exists
                );
                break;
            case LVM_THIN:
                ret = new LvmThinVlmPojo(
                    vlmNr,
                    devicePath,
                    allocatedSize,
                    usableSize,
                    null,
                    null,
                    diskState,
                    discGran,
                    storPoolApi,
                    exists
                );
                break;
            case STORAGE_SPACES:
                ret = new StorageSpacesVlmPojo(
                    vlmNr,
                    devicePath,
                    allocatedSize,
                    usableSize,
                    null,
                    null,
                    diskState,
                    storPoolApi,
                    exists
                );
                break;
            case STORAGE_SPACES_THIN:
                ret = new StorageSpacesThinVlmPojo(
                    vlmNr,
                    devicePath,
                    allocatedSize,
                    usableSize,
                    null,
                    null,
                    diskState,
                    storPoolApi,
                    exists
                );
                break;
            case ZFS:
                ret = new ZfsVlmPojo(
                    vlmNr,
                    devicePath,
                    allocatedSize,
                    usableSize,
                    null,
                    null,
                    diskState,
                    discGran,
                    storPoolApi,
                    exists,
                    protoVlm.getZfs().getExtentSize()
                );
                break;
            case ZFS_THIN:
                ret = new ZfsThinVlmPojo(
                    vlmNr,
                    devicePath,
                    allocatedSize,
                    usableSize,
                    null,
                    null,
                    diskState,
                    discGran,
                    storPoolApi,
                    exists,
                    protoVlm.getZfsThin().getExtentSize()
                );
                break;
            case FILE:
                ret = new FileVlmPojo(
                    vlmNr,
                    devicePath,
                    allocatedSize,
                    usableSize,
                    null,
                    null,
                    diskState,
                    discGran,
                    storPoolApi,
                    exists
                );
                break;
            case FILE_THIN:
                ret = new FileThinVlmPojo(
                    vlmNr,
                    devicePath,
                    allocatedSize,
                    usableSize,
                    null,
                    null,
                    diskState,
                    discGran,
                    storPoolApi,
                    exists
                );
                break;
            case SPDK:
                ret = new SpdkVlmPojo(
                    vlmNr,
                    devicePath,
                    allocatedSize,
                    usableSize,
                    null,
                    null,
                    diskState,
                    discGran,
                    storPoolApi,
                    exists
                );
                break;
            case REMOTE_SPDK:
                ret = new RemoteSpdkVlmPojo(
                    vlmNr,
                    devicePath,
                    allocatedSize,
                    usableSize,
                    null,
                    null,
                    diskState,
                    discGran,
                    storPoolApi,
                    exists
                );
                break;
            case EXOS:
                ret = new ExosVlmPojo(
                    vlmNr,
                    devicePath,
                    allocatedSize,
                    usableSize,
                    null,
                    null,
                    diskState,
                    discGran,
                    storPoolApi,
                    exists
                );
                break;
            case EBS_INIT:
            case EBS_TARGET:
                ret = new EbsVlmPojo(
                    vlmNr,
                    devicePath,
                    allocatedSize,
                    usableSize,
                    null,
                    null,
                    diskState,
                    discGran,
                    storPoolApi,
                    exists
                );
                break;
            case FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER: // fall-through
            default:
                throw new ImplementationError(
                    "Unexpected provider type in proto message: " +
                        storPoolApi.getDeviceProviderKind() +
                        " vlm nr :" + protoVlm.getVlmNr()
                );
        }
        return ret;
    }

    private static VlmDfnLayerDataApi extractStorageVlmDfn(StorageVlmDfn storageVlmDfnRef)
    {
        VlmDfnLayerDataApi vlmDfnApi;
        switch (storageVlmDfnRef.getProviderKind())
        {
            case DISKLESS: // fall-trough
            case LVM: // fall-trough
            case LVM_THIN: // fall-trough
            case SPDK: // fall-trough
            case REMOTE_SPDK: // fall-trough
            case ZFS: // fall-trough
            case ZFS_THIN: // fall-trough
            case FILE: // fall-through
            case FILE_THIN: // fall-through
            case EBS_INIT: // fall-through
            case EBS_TARGET: // fall-through
            case EXOS:
            case STORAGE_SPACES:
            case STORAGE_SPACES_THIN:
                vlmDfnApi = null;
                break;
            case UNKNOWN_PROVIDER: // fall-through
            case UNRECOGNIZED: // fall-through
            default:
                throw new ImplementationError(
                    "Unexpected provider: " + storageVlmDfnRef.getProviderKind()
                );
        }
        return vlmDfnApi;
    }

    private static NvmeVlmPojo extractNvmeVlm(NvmeVlm protoNvmeVlm, long fullSyncId, long updateId)
    {
        return new NvmeVlmPojo(
            protoNvmeVlm.getVlmNr(),
            protoNvmeVlm.getDevicePath(),
            protoNvmeVlm.getBackingDevice(),
            protoNvmeVlm.getAllocatedSize(),
            protoNvmeVlm.getUsableSize(),
            protoNvmeVlm.getDiskState(),
            protoNvmeVlm.getDiscGran(),
            false
        );
    }

    private static WritecacheVlmPojo extractWritecacheVlm(WritecacheVlm protoVlm, long fullSyncId, long updateId)
    {
        return new WritecacheVlmPojo(
            protoVlm.getVlmNr(),
            protoVlm.getDevicePath(),
            protoVlm.getDataDevice(),
            protoVlm.getCacheDevice(),
            protoVlm.getCacheStorPoolName(),
            protoVlm.getAllocatedSize(),
            protoVlm.getUsableSize(),
            protoVlm.getDiskState(),
            protoVlm.getDiscGran(),
            false
        );
    }

    private static CacheVlmPojo extractCacheVlm(CacheVlm protoVlm, long fullSyncId, long updateId)
    {
        return new CacheVlmPojo(
            protoVlm.getVlmNr(),
            protoVlm.getDevicePath(),
            protoVlm.getDataDevice(),
            protoVlm.getCacheDevice(),
            protoVlm.getMetaDevice(),
            protoVlm.getCacheStorPoolName(),
            protoVlm.getMetaStorPoolName(),
            protoVlm.getAllocatedSize(),
            protoVlm.getUsableSize(),
            protoVlm.getDiskState(),
            protoVlm.getDiscGran(),
            false
        );
    }

    private static BCacheVlmPojo extractBCacheVlm(BCacheVlm protoVlm, long fullSyncId, long updateId)
    {
        UUID uuid = null;
        if (protoVlm.getDeviceUuid() != null && !protoVlm.getDeviceUuid().isEmpty())
        {
            uuid = UUID.fromString(protoVlm.getDeviceUuid());
        }
        return new BCacheVlmPojo(
            protoVlm.getVlmNr(),
            protoVlm.getDevicePath(),
            protoVlm.getDataDevice(),
            protoVlm.getCacheDevice(),
            protoVlm.getCacheStorPoolName(),
            protoVlm.getAllocatedSize(),
            protoVlm.getUsableSize(),
            protoVlm.getDiskState(),
            protoVlm.getDiscGran(),
            uuid,
            false
        );
    }

    private ProtoLayerUtils()
    {
    }
}
