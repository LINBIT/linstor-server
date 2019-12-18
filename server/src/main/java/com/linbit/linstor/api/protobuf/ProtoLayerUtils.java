package com.linbit.linstor.api.protobuf;

import com.linbit.ImplementationError;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.RscDfnLayerDataApi;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.interfaces.VlmDfnLayerDataApi;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
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
import com.linbit.linstor.api.pojo.StorageRscPojo.FileThinVlmPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo.FileVlmPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo.LvmThinVlmPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo.LvmVlmPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo.SpdkVlmPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo.ZfsThinVlmPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo.ZfsVlmPojo;
import com.linbit.linstor.api.pojo.WritecacheRscPojo;
import com.linbit.linstor.api.pojo.WritecacheRscPojo.WritecacheVlmPojo;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apis.StorPoolApi;
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

import java.util.ArrayList;
import java.util.List;

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
                            protoRscData.getSuspend()
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
                            protoRscData.getSuspend()
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
                            protoRscData.getSuspend()
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
                        protoRscData.getSuspend()
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
                        protoRscData.getSuspend()
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
                case LUKS:
                    rscDfnLayerDataApi = null;
                    break;
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
                case STORAGE:
                    rscDfnLayerDataApi = null;
                    break;
                case NVME:
                    rscDfnLayerDataApi = null;
                    break;
                case WRITECACHE:
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
                case LUKS:
                    vlmDfnLayerDataApi = null;
                    break;
                case NVME:
                    vlmDfnLayerDataApi = null;
                    break;
                case WRITECACHE:
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
            protoDrbdVlm.getDiskState()
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
        return new LuksVlmPojo(
            protoLuksVlm.getVlmNr(),
            ProtoDeserializationUtils.extractByteArray(protoLuksVlm.getEncryptedPassword()),
            protoLuksVlm.getDevicePath(),
            protoLuksVlm.getBackingDevice(),
            protoLuksVlm.getAllocatedSize(),
            protoLuksVlm.getUsableSize(),
            protoLuksVlm.getOpened(),
            protoLuksVlm.getDiskState()
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
        StorPoolApi storPoolApi = ProtoDeserializationUtils.parseStorPool(
            protoVlm.getStoragePool(),
            fullSyncId,
            updateId
        );
        switch (storPoolApi.getDeviceProviderKind())
        {
            case DISKLESS:
                ret = new DisklessVlmPojo(vlmNr, devicePath, allocatedSize, usableSize, null, storPoolApi);
                break;
            case LVM:
                ret = new LvmVlmPojo(vlmNr, devicePath, allocatedSize, usableSize, diskState, storPoolApi);
                break;
            case LVM_THIN:
                ret = new LvmThinVlmPojo(vlmNr, devicePath, allocatedSize, usableSize, diskState, storPoolApi);
                break;
            case ZFS:
                ret = new ZfsVlmPojo(vlmNr, devicePath, allocatedSize, usableSize, diskState, storPoolApi);
                break;
            case ZFS_THIN:
                ret = new ZfsThinVlmPojo(vlmNr, devicePath, allocatedSize, usableSize, diskState, storPoolApi);
                break;
            case FILE:
                ret = new FileVlmPojo(vlmNr, devicePath, allocatedSize, usableSize, diskState, storPoolApi);
                break;
            case FILE_THIN:
                ret = new FileThinVlmPojo(vlmNr, devicePath, allocatedSize, usableSize, diskState, storPoolApi);
                break;
            case SPDK:
                ret = new SpdkVlmPojo(vlmNr, devicePath, allocatedSize, usableSize, diskState, storPoolApi);
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
            case ZFS: // fall-trough
            case ZFS_THIN: // fall-trough
            case FILE: // fall-through
            case FILE_THIN:
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
            protoNvmeVlm.getDiskState()
        );
    }

    private static WritecacheVlmPojo extractWritecacheVlm(WritecacheVlm protoVlm, long fullSyncId, long updateId)
    {
        return new WritecacheVlmPojo(
            protoVlm.getVlmNr(),
            protoVlm.getDevicePathData(),
            protoVlm.getDevicePathCache(),
            protoVlm.getCacheStorPoolName(),
            protoVlm.getAllocatedSize(),
            protoVlm.getUsableSize(),
            protoVlm.getDiskState()
        );
    }

    private ProtoLayerUtils()
    {
    }
}
