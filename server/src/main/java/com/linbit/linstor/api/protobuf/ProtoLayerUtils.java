package com.linbit.linstor.api.protobuf;

import com.linbit.ImplementationError;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.RscDfnLayerDataApi;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.interfaces.VlmDfnLayerDataApi;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.api.pojo.CryptSetupRscPojo;
import com.linbit.linstor.api.pojo.CryptSetupRscPojo.CryptVlmPojo;
import com.linbit.linstor.api.pojo.DrbdRscPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo.DrbdDisklessVlmPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo.LvmVlmPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo.LvmThinVlmPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo.SwordfishInitiatorVlmPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo.SwordfishTargetVlmPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo.SwordfishVlmDfnPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo.ZfsVlmPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo.ZfsThinVlmPojo;
import com.linbit.linstor.api.pojo.DrbdRscPojo.DrbdRscDfnPojo;
import com.linbit.linstor.api.pojo.DrbdRscPojo.DrbdVlmDfnPojo;
import com.linbit.linstor.api.pojo.DrbdRscPojo.DrbdVlmPojo;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.proto.common.CryptRscOuterClass.CryptVlm;
import com.linbit.linstor.proto.common.DrbdRscOuterClass.DrbdRsc;
import com.linbit.linstor.proto.common.DrbdRscOuterClass.DrbdRscDfn;
import com.linbit.linstor.proto.common.DrbdRscOuterClass.DrbdVlm;
import com.linbit.linstor.proto.common.DrbdRscOuterClass.DrbdVlmDfn;
import com.linbit.linstor.proto.common.RscDfnOuterClass.RscDfn;
import com.linbit.linstor.proto.common.RscDfnOuterClass.RscDfnLayerData;
import com.linbit.linstor.proto.common.RscLayerDataOuterClass.RscLayerData;
import com.linbit.linstor.proto.common.StorageRscOuterClass.StorageVlm;
import com.linbit.linstor.proto.common.StorageRscOuterClass.SwordfishVlmDfn;
import com.linbit.linstor.proto.common.TypesOuterClass;
import com.linbit.linstor.proto.common.VlmDfnOuterClass.VlmDfnLayerData;
import com.linbit.linstor.proto.common.VlmOuterClass.VlmLayerData;
import com.linbit.utils.Pair;

import java.util.ArrayList;
import java.util.List;

public class ProtoLayerUtils
{
    public static RscLayerDataApi extractRscLayerData(RscLayerData protoRscData)
    {
        RscLayerDataApi ret;
        switch (protoRscData.getLayerSpecificPayloadCase())
        {
            case DRBD:
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
                        new ArrayList<>()
                    );

                    for (DrbdVlm protoDrbdVlm : protoDrbdRsc.getDrbdVlmsList())
                    {
                        drbdRscPojo.getVolumeList().add(extractDrbdVlm(protoDrbdVlm));
                    }

                    ret = drbdRscPojo;
                }
                break;
            case CRYPT:
                {
                    CryptSetupRscPojo cryptRscPojo = new CryptSetupRscPojo(
                        protoRscData.getId(),
                        new ArrayList<>(),
                        protoRscData.getRscNameSuffix(),
                        new ArrayList<>()
                    );
                    for (CryptVlm protoCryptVlm : protoRscData.getCrypt().getCryptVlmsList())
                    {
                        cryptRscPojo.getVolumeList().add(extractCryptVlm(protoCryptVlm));
                    }

                    ret = cryptRscPojo;
                }
                break;
            case STORAGE:
                {
                    StorageRscPojo storageRscPojo = new StorageRscPojo(
                        protoRscData.getId(),
                        new ArrayList<>(),
                        protoRscData.getRscNameSuffix(),
                        new ArrayList<>()
                    );
                    for (StorageVlm protoVlm : protoRscData.getStorage().getStorageVlmsList())
                    {
                        storageRscPojo.getVolumeList().add(extractStorageVolume(protoVlm));
                    }

                    ret = storageRscPojo;
                }
                break;
            case LAYERSPECIFICPAYLOAD_NOT_SET:
                ret = null;
                break;
            default:
                throw new ImplementationError(
                    "Unexpected layer type in proto message: " +
                        protoRscData.getLayerSpecificPayloadCase() +
                        " Layered rsc id: " + protoRscData.getId()
                );
        }

        if (ret != null)
        {
            for (RscLayerData childrenProto : protoRscData.getChildrenList())
            {
                ret.getChildren().add(extractRscLayerData(childrenProto));
            }
        }

        return ret;
    }

    public static String layerType2layerString(TypesOuterClass.Types.LayerType layerType)
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

    public static List<Pair<String, VlmLayerDataApi>> extractVlmLayerData(
        List<VlmLayerData> vlmLayerDataList
    )
    {
        List<Pair<String, VlmLayerDataApi>> ret = new ArrayList<>();

        for (VlmLayerData vlmLayerData : vlmLayerDataList)
        {
            Pair<String, VlmLayerDataApi> pair = new Pair<>();
            pair.objA = layerType2layerString(vlmLayerData.getLayerType());

            VlmLayerDataApi vlmLayerDataApi;

            switch (vlmLayerData.getDataCase())
            {
                case CRYPT:
                    vlmLayerDataApi = extractCryptVlm(vlmLayerData.getCrypt());
                    break;
                case DATA_NOT_SET:
                    vlmLayerDataApi = null;
                    break;
                case DRBD:
                    vlmLayerDataApi = extractDrbdVlm(vlmLayerData.getDrbd());
                    break;
                case STORAGE:
                    vlmLayerDataApi = extractStorageVolume(vlmLayerData.getStorage());
                    break;
                default:
                    throw new ImplementationError(
                        "Unknown VlmLayerData (proto) kind: " + vlmLayerData.getDataCase()
                    );
            }
            pair.objB = vlmLayerDataApi;
            ret.add(pair);
        }
        return ret;
    }

    public static List<Pair<String, RscDfnLayerDataApi>> extractRscDfnLayerData(RscDfn rscDfnRef)
    {
        List<Pair<String, RscDfnLayerDataApi>> ret = new ArrayList<>();

        for (RscDfnLayerData rscDfnLayerData : rscDfnRef.getLayerDataList())
        {
            RscDfnLayerDataApi rscDfnLayerDataApi;
            switch (rscDfnLayerData.getDataCase())
            {
                case DATA_NOT_SET:
                    rscDfnLayerDataApi = null;
                    break;
                case DRBD:
                    rscDfnLayerDataApi = extractDrbdRscDfn(rscDfnLayerData.getDrbd());
                    break;
                default:
                    throw new ImplementationError(
                        "Unknown resource definition layer (proto) kind: " + rscDfnLayerData.getDataCase()
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
            switch (vlmDfnLayerData.getDataCase())
            {
                case DATA_NOT_SET:
                    vlmDfnLayerDataApi = null;
                    break;
                case DRBD:
                    vlmDfnLayerDataApi = extractDrbdVlmDfn(vlmDfnLayerData.getDrbd());
                    break;
                default:
                    throw new ImplementationError(
                        "Unknown volume definition layer (proto) kind: " + vlmDfnLayerData.getDataCase()
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

    private static DrbdVlmPojo extractDrbdVlm(DrbdVlm protoDrbdVlm)
    {
        DrbdVlmDfn protoDrbdVlmDfn = protoDrbdVlm.getDrbdVlmDfn();
        return new DrbdVlmPojo(
            extractDrbdVlmDfn(protoDrbdVlmDfn),
            protoDrbdVlm.getDevicePath(),
            protoDrbdVlm.getBackingDevice(),
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

    private static CryptVlmPojo extractCryptVlm(CryptVlm protoCryptVlm)
    {
        return new CryptVlmPojo(
            protoCryptVlm.getVlmNr(),
            ProtoDeserializationUtils.extractByteArray(protoCryptVlm.getEncryptedPassword()),
            protoCryptVlm.getDevicePath(),
            protoCryptVlm.getBackingDevice(),
            protoCryptVlm.getAllocatedSize(),
            protoCryptVlm.getUsableSize(),
            protoCryptVlm.getOpened(),
            protoCryptVlm.getDiskState()
        );
    }


    private static VlmLayerDataApi extractStorageVolume(
        StorageVlm protoVlm
    )
        throws ImplementationError
    {
        VlmLayerDataApi ret;
        int vlmNr = protoVlm.getVlmNr();
        String devicePath = protoVlm.getDevicePath();
        long allocatedSize = protoVlm.getAllocatedSize();
        long usableSize = protoVlm.getUsableSize();
        String diskState = protoVlm.getDiskState();
        switch (protoVlm.getProviderCase())
        {
            case DRBD_DISKLESS:
                ret = new DrbdDisklessVlmPojo(vlmNr, devicePath, allocatedSize, usableSize, null);
                break;
            case LVM:
                ret = new LvmVlmPojo(vlmNr, devicePath, allocatedSize, usableSize, diskState);
                break;
            case LVM_THIN:
                ret = new LvmThinVlmPojo(vlmNr, devicePath, allocatedSize, usableSize, diskState);
                break;
            case SF_INIT:
                {
                    SwordfishVlmDfn protoSfVlmDfn = protoVlm.getSfInit().getSfVlmDfn();
                    ret = new SwordfishInitiatorVlmPojo(
                        new SwordfishVlmDfnPojo(
                            protoSfVlmDfn.getRscNameSuffix(),
                            protoSfVlmDfn.getVlmNr(),
                            protoSfVlmDfn.getVlmOdata()
                        ),
                        devicePath,
                        allocatedSize,
                        usableSize,
                        diskState
                    );
                }
                break;
            case SF_TARGET:
                {
                    SwordfishVlmDfn protoSfVlmDfn = protoVlm.getSfTarget().getSfVlmDfn();
                    ret = new SwordfishTargetVlmPojo(
                        new SwordfishVlmDfnPojo(
                            protoSfVlmDfn.getRscNameSuffix(),
                            protoSfVlmDfn.getVlmNr(),
                            protoSfVlmDfn.getVlmOdata()
                        ),
                        allocatedSize,
                        usableSize
                    );
                }
                break;
            case ZFS:
                ret = new ZfsVlmPojo(vlmNr, devicePath, allocatedSize, usableSize, diskState);
                break;
            case ZFS_THIN:
                ret = new ZfsThinVlmPojo(vlmNr, devicePath, allocatedSize, usableSize, diskState);
                break;
            case PROVIDER_NOT_SET:
            default:
                throw new ImplementationError(
                    "Unexpected provider type in proto message: " +
                        protoVlm.getProviderCase() +
                        " vlm nr :" + protoVlm.getVlmNr()
                );
        }
        return ret;
    }
}
