package com.linbit.linstor.api.protobuf;

import com.linbit.ImplementationError;
import com.linbit.linstor.api.interfaces.RscLayerDataPojo;
import com.linbit.linstor.api.interfaces.VlmLayerDataPojo;
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
import com.linbit.linstor.proto.common.CryptRscOuterClass.CryptVlm;
import com.linbit.linstor.proto.common.DrbdRscOuterClass.DrbdRsc;
import com.linbit.linstor.proto.common.DrbdRscOuterClass.DrbdRscDfn;
import com.linbit.linstor.proto.common.DrbdRscOuterClass.DrbdVlm;
import com.linbit.linstor.proto.common.DrbdRscOuterClass.DrbdVlmDfn;
import com.linbit.linstor.proto.common.RscLayerDataOuterClass.RscLayerData;
import com.linbit.linstor.proto.common.StorageRscOuterClass.StorageVlm;
import com.linbit.linstor.proto.common.StorageRscOuterClass.SwordfishVlmDfn;

import java.util.ArrayList;

public class ProtoRscLayerUtils
{
    public static RscLayerDataPojo extractLayerData(RscLayerData protoRscData)
    {
        RscLayerDataPojo ret;
        switch (protoRscData.getLayerSpecificPayloadCase())
        {
            case DRBD:
                {
                    DrbdRsc protoDrbdRsc = protoRscData.getDrbd();

                    DrbdRscDfn protoDrbdRscDfn = protoDrbdRsc.getDrbdRscDfn();
                    // rscDfn does not have to be the same reference, just contain the data
                    DrbdRscDfnPojo drbdRscDfnPojo = new DrbdRscDfnPojo(
                        protoDrbdRscDfn.getRscNameSuffix(),
                        (short) protoDrbdRscDfn.getPeersSlots(),
                        protoDrbdRscDfn.getAlStripes(),
                        protoDrbdRscDfn.getAlSize(),
                        protoDrbdRscDfn.getPort(),
                        protoDrbdRscDfn.getTransportType(),
                        protoDrbdRscDfn.getSecret()
                    );
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
                        DrbdVlmDfn protoDrbdVlmDfn = protoDrbdVlm.getDrbdVlmDfn();
                        drbdRscPojo.getVolumeList().add(
                            new DrbdVlmPojo(
                                new DrbdVlmDfnPojo(
                                    protoDrbdVlmDfn.getRscNameSuffix(),
                                    protoDrbdVlmDfn.getVlmNr(),
                                    protoDrbdVlmDfn.getMinor()
                                ),
                                protoDrbdVlm.getDevicePath(),
                                protoDrbdVlm.getBackingDevice(),
                                protoDrbdVlm.getMetaDisk(),
                                protoDrbdVlm.getAllocatedSize(),
                                protoDrbdVlm.getUsableSize()
                            )
                        );
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
                        cryptRscPojo.getVolumeList().add(
                            new CryptVlmPojo(
                                protoCryptVlm.getVlmNr(),
                                ProtoDeserializationUtils.extractByteArray(protoCryptVlm.getEncryptedPassword()),
                                protoCryptVlm.getDevicePath(),
                                protoCryptVlm.getBackingDevice(),
                                protoCryptVlm.getAllocatedSize(),
                                protoCryptVlm.getUsableSize()
                            )
                        );
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
            default:
            case LAYERSPECIFICPAYLOAD_NOT_SET:
                throw new ImplementationError(
                    "Unexpected layer type in proto message: " +
                        protoRscData.getLayerSpecificPayloadCase() +
                        " Layered rsc id: " + protoRscData.getId()
                );
        }

        for (RscLayerData childrenProto : protoRscData.getChildrenList())
        {
            ret.getChildren().add(extractLayerData(childrenProto));
        }

        return ret;
    }

    private static VlmLayerDataPojo extractStorageVolume(
        StorageVlm protoVlm
    )
        throws ImplementationError
    {
        VlmLayerDataPojo ret;
        int vlmNr = protoVlm.getVlmNr();
        String devicePath = protoVlm.getDevicePath();
        long allocatedSize = protoVlm.getAllocatedSize();
        long usableSize = protoVlm.getUsableSize();
        switch (protoVlm.getProviderCase())
        {
            case DRBD_DISKLESS:
                ret = new DrbdDisklessVlmPojo(vlmNr, devicePath, allocatedSize, usableSize);
                break;
            case LVM:
                ret = new LvmVlmPojo(vlmNr, devicePath, allocatedSize, usableSize);
                break;
            case LVM_THIN:
                ret = new LvmThinVlmPojo(vlmNr, devicePath, allocatedSize, usableSize);
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
                        usableSize
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
                ret = new ZfsVlmPojo(vlmNr, devicePath, allocatedSize, usableSize);
                break;
            case ZFS_THIN:
                ret = new ZfsThinVlmPojo(vlmNr, devicePath, allocatedSize, usableSize);
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
