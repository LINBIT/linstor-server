package com.linbit.linstor.api.protobuf;

import com.linbit.ImplementationError;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.pojo.StorPoolPojo;
import com.linbit.linstor.core.objects.StorPool.StorPoolApi;
import com.linbit.linstor.proto.common.ApiCallResponseOuterClass;
import com.linbit.linstor.proto.common.LayerTypeOuterClass.LayerType;
import com.linbit.linstor.proto.common.LayerTypeWrapperOuterClass.LayerTypeWrapper;
import com.linbit.linstor.proto.common.ProviderTypeOuterClass.ProviderType;
import com.linbit.linstor.proto.common.ProviderTypeWrapperOuterClass.ProviderTypeWrapper;
import com.linbit.linstor.proto.common.StorPoolOuterClass.StorPool;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.utils.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;

public class ProtoDeserializationUtils
{
    public static ApiCallRc.RcEntry parseApiCallRc(
        ApiCallResponseOuterClass.ApiCallResponse apiCallResponse,
        String messagePrefix
    )
    {
        ApiCallRcImpl.EntryBuilder entryBuilder = ApiCallRcImpl
            .entryBuilder(
                apiCallResponse.getRetCode(),
                messagePrefix + apiCallResponse.getMessage()
            );

        if (!StringUtils.isEmpty(apiCallResponse.getCause()))
        {
            entryBuilder.setCause(apiCallResponse.getCause());
        }
        if (!StringUtils.isEmpty(apiCallResponse.getCorrection()))
        {
            entryBuilder.setCorrection(apiCallResponse.getCorrection());
        }
        if (!StringUtils.isEmpty(apiCallResponse.getDetails()))
        {
            entryBuilder.setDetails(apiCallResponse.getDetails());
        }

        entryBuilder.putAllObjRefs(apiCallResponse.getObjRefsMap());

        entryBuilder.addAllErrorIds(apiCallResponse.getErrorReportIdsList());

        return entryBuilder.build();
    }

    public static ApiCallRc parseApiCallRcList(
        List<ApiCallResponseOuterClass.ApiCallResponse> apiCallRcs
    )
    {
        return new ApiCallRcImpl(apiCallRcs.stream()
            .map(apiCallResponse -> parseApiCallRc(apiCallResponse, ""))
            .collect(Collectors.toList()));
    }

    public static byte[] extractByteArray(ByteString protoBytes)
    {
        byte[] arr = new byte[protoBytes.size()];
        protoBytes.copyTo(arr, 0);
        return arr;
    }

    public static List<DeviceProviderKind> parseDeviceProviderKind(List<ProviderType> providerTypeList)
    {
        List<DeviceProviderKind> providerKindList = new ArrayList<>();
        for (ProviderType providerType : providerTypeList)
        {
            providerKindList.add(parseDeviceProviderKind(providerType));
        }
        return providerKindList;
    }

    public static List<DeviceProviderKind> parseDeviceProviderKindWrapper(
        List<ProviderTypeWrapper> providerTypeWrapperList
    )
    {
        List<DeviceProviderKind> providerKindList = new ArrayList<>();
        for (ProviderTypeWrapper providerTypeWrapper : providerTypeWrapperList)
        {
            providerKindList.add(parseDeviceProviderKind(providerTypeWrapper.getType()));
        }
        return providerKindList;
    }


    public static DeviceProviderKind parseDeviceProviderKind(ProviderType providerKindRef)
    {
        DeviceProviderKind kind = null;
        if (providerKindRef != null)
        {
            switch (providerKindRef)
            {
                case DISKLESS:
                    kind = DeviceProviderKind.DISKLESS;
                    break;
                case LVM:
                    kind = DeviceProviderKind.LVM;
                    break;
                case LVM_THIN:
                    kind = DeviceProviderKind.LVM_THIN;
                    break;
                case SWORDFISH_INITIATOR:
                    kind = DeviceProviderKind.SWORDFISH_INITIATOR;
                    break;
                case SWORDFISH_TARGET:
                    kind = DeviceProviderKind.SWORDFISH_TARGET;
                    break;
                case ZFS:
                    kind = DeviceProviderKind.ZFS;
                    break;
                case ZFS_THIN:
                    kind = DeviceProviderKind.ZFS_THIN;
                    break;
                case FILE:
                    kind = DeviceProviderKind.FILE;
                    break;
                case FILE_THIN:
                    kind = DeviceProviderKind.FILE_THIN;
                    break;
                case UNKNOWN_PROVIDER: // fall-through
                default:
                    throw new ImplementationError("Unknown (proto) ProviderType: " + providerKindRef);
            }
        }
        return kind;
    }

    public static List<DeviceLayerKind> parseDeviceLayerKindList(List<LayerType> layerTypeList)
    {
        List<DeviceLayerKind> devLayerKindList = new ArrayList<>();
        for (LayerType layerType : layerTypeList)
        {
            devLayerKindList.add(parseDeviceLayerKind(layerType));
        }
        return devLayerKindList;
    }

    public static List<DeviceLayerKind> parseDeviceLayerKindWrapper(List<LayerTypeWrapper> layerTypeWrapperList)
    {
        List<DeviceLayerKind> devLayerKindList = new ArrayList<>();
        for (LayerTypeWrapper layerTypeWrapper : layerTypeWrapperList)
        {
            devLayerKindList.add(parseDeviceLayerKind(layerTypeWrapper.getType()));
        }
        return devLayerKindList;
    }

    public static DeviceLayerKind parseDeviceLayerKind(LayerType layerTypeRef)
    {
        DeviceLayerKind kind = null;
        switch (layerTypeRef)
        {
            case DRBD:
                kind = DeviceLayerKind.DRBD;
                break;
            case LUKS:
                kind = DeviceLayerKind.LUKS;
                break;
            case STORAGE:
                kind = DeviceLayerKind.STORAGE;
                break;
            case NVME:
                kind = DeviceLayerKind.NVME;
                break;
            case UNKNOWN_LAYER: // fall-trough
            default:
                throw new ImplementationError("Unknown (proto) LayerType: " + layerTypeRef);
        }
        return kind;
    }

    public static StorPoolApi parseStorPool(StorPool storPoolProto, long fullSyncId, long updateId)
    {
        return new StorPoolPojo(
            UUID.fromString(storPoolProto.getStorPoolUuid()),
            UUID.fromString(storPoolProto.getNodeUuid()),
            storPoolProto.getNodeName(),
            storPoolProto.getStorPoolName(),
            UUID.fromString(storPoolProto.getStorPoolDfnUuid()),
            parseDeviceProviderKind(storPoolProto.getProviderKind()),
            storPoolProto.getPropsMap(),
            storPoolProto.getStorPoolDfnPropsMap(),
            storPoolProto.getStaticTraitsMap(),
            fullSyncId,
            updateId,
            storPoolProto.getFreeSpaceMgrName(),
            Optional.ofNullable(
                storPoolProto.hasFreeSpace() && storPoolProto.getFreeSpace().hasFreeCapacity() ?
                    storPoolProto.getFreeSpace().getFreeCapacity() :
                    null
            ),
            Optional.ofNullable(
                storPoolProto.hasFreeSpace() && storPoolProto.getFreeSpace().hasTotalCapacity() ?
                    storPoolProto.getFreeSpace().getTotalCapacity() :
                    null
            ),
            null
        );
    }

    private ProtoDeserializationUtils()
    {
    }
}
