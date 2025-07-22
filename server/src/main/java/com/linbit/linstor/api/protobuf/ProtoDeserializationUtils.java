package com.linbit.linstor.api.protobuf;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.pojo.RequestFilePojo;
import com.linbit.linstor.api.pojo.RscGrpPojo;
import com.linbit.linstor.api.pojo.StorPoolPojo;
import com.linbit.linstor.api.pojo.VlmGrpPojo;
import com.linbit.linstor.api.pojo.builder.AutoSelectFilterBuilder;
import com.linbit.linstor.api.prop.BooleanProperty;
import com.linbit.linstor.api.prop.BooleanTrueFalseProperty;
import com.linbit.linstor.api.prop.LongProperty;
import com.linbit.linstor.api.prop.NumericOrSymbolProperty;
import com.linbit.linstor.api.prop.Property;
import com.linbit.linstor.api.prop.RangeFloatProperty;
import com.linbit.linstor.api.prop.RangeProperty;
import com.linbit.linstor.api.prop.RegexProperty;
import com.linbit.linstor.api.prop.StringProperty;
import com.linbit.linstor.core.apis.StorPoolApi;
import com.linbit.linstor.core.apis.VolumeGroupApi;
import com.linbit.linstor.core.objects.VolumeGroup;
import com.linbit.linstor.proto.common.ApiCallResponseOuterClass;
import com.linbit.linstor.proto.common.ExternalToolsOuterClass.ExternalToolsInfo;
import com.linbit.linstor.proto.common.ExternalToolsOuterClass.ExternalToolsInfo.ExternalTools;
import com.linbit.linstor.proto.common.LayerTypeOuterClass;
import com.linbit.linstor.proto.common.PropertyOuterClass;
import com.linbit.linstor.proto.common.ProviderTypeOuterClass.ProviderType;
import com.linbit.linstor.proto.common.RscGrpOuterClass.RscGrp;
import com.linbit.linstor.proto.common.StorPoolOuterClass;
import com.linbit.linstor.proto.common.VlmGrpOuterClass.VlmGrp;
import com.linbit.linstor.proto.requests.MsgReqSosReportFilesOuterClass.ReqFile;
import com.linbit.linstor.proto.responses.MsgApiRcResponseOuterClass.MsgApiRcResponse;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;
import com.linbit.utils.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
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

    public static ApiCallRc parseApiCallRcList(
        List<ApiCallResponseOuterClass.ApiCallResponse> apiCallRcs,
        final String messagePrefix
    )
    {
        return new ApiCallRcImpl(apiCallRcs.stream()
            .map(apiCallResponse -> parseApiCallRc(apiCallResponse, messagePrefix))
            .collect(Collectors.toList()));
    }

    public static ApiCallRc parseApiCallAnswerMsg(
        final ByteArrayInputStream protobufMsg,
        final String messagePrefix) throws IOException
    {
        MsgApiRcResponse apiCallResponse = MsgApiRcResponse.parseDelimitedFrom(protobufMsg);
        return parseApiCallRcList(apiCallResponse.getResponsesList(), messagePrefix);
    }

    public static byte[] extractByteArray(ByteString protoBytes)
    {
        byte[] arr = new byte[protoBytes.size()];
        protoBytes.copyTo(arr, 0);
        return arr;
    }

    public static List<DeviceProviderKind> parseDeviceProviderKind(
        List<ProviderType> providerTypeList
    )
    {
        List<DeviceProviderKind> providerKindList = new ArrayList<>();
        for (ProviderType providerType : providerTypeList)
        {
            providerKindList.add(parseDeviceProviderKind(providerType));
        }
        return providerKindList;
    }

    public static DeviceProviderKind parseDeviceProviderKind(
        ProviderType providerKindRef
    )
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
                case SPDK:
                    kind = DeviceProviderKind.SPDK;
                    break;
                case REMOTE_SPDK:
                    kind = DeviceProviderKind.REMOTE_SPDK;
                    break;
                case EBS_INIT:
                    kind = DeviceProviderKind.EBS_INIT;
                    break;
                case EBS_TARGET:
                    kind = DeviceProviderKind.EBS_TARGET;
                    break;
                case STORAGE_SPACES:
                    kind = DeviceProviderKind.STORAGE_SPACES;
                    break;
                case STORAGE_SPACES_THIN:
                    kind = DeviceProviderKind.STORAGE_SPACES_THIN;
                    break;
                case UNKNOWN_PROVIDER: // fall-through
                case UNRECOGNIZED: // fall-through
                default:
                    throw new ImplementationError("Unknown (proto) ProviderType: " + providerKindRef);
            }
        }
        return kind;
    }

    public static List<DeviceLayerKind> parseDeviceLayerKindList(List<LayerTypeOuterClass.LayerType> layerTypeList)
    {
        return parseDeviceLayerKindList(layerTypeList, true);
    }

    public static List<DeviceLayerKind> parseDeviceLayerKindList(
        List<LayerTypeOuterClass.LayerType> layerTypeList,
        boolean throwIfUnknown
    )
    {
        List<DeviceLayerKind> devLayerKindList = new ArrayList<>();
        for (LayerTypeOuterClass.LayerType layerType : layerTypeList)
        {
            devLayerKindList.add(parseDeviceLayerKind(layerType, throwIfUnknown));
        }
        return devLayerKindList;
    }

    public static @Nullable DeviceLayerKind parseDeviceLayerKind(LayerTypeOuterClass.LayerType layerTypeRef)
    {
        return parseDeviceLayerKind(layerTypeRef, true);
    }

    public static @Nullable DeviceLayerKind parseDeviceLayerKind(
        LayerTypeOuterClass.LayerType layerTypeRef,
        boolean throwIfUnknown
    )
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
            case WRITECACHE:
                kind = DeviceLayerKind.WRITECACHE;
                break;
            case CACHE:
                kind = DeviceLayerKind.CACHE;
                break;
            case BCACHE:
                kind = DeviceLayerKind.BCACHE;
                break;
            case UNKNOWN_LAYER: // fall-through
            case UNRECOGNIZED: // fall-through
            default:
                if (throwIfUnknown)
                {
                    throw new ImplementationError("Unknown (proto) LayerType: " + layerTypeRef);
                }
                break;
        }
        return kind;
    }

    public static ArrayList<ExtToolsInfo> parseExternalToolInfoList(
        List<ExternalToolsInfo> extToolsListRef,
        boolean throwIfUnknown
    )
    {
        ArrayList<ExtToolsInfo> ret = new ArrayList<>();
        for (ExternalToolsInfo protoLayerInfo : extToolsListRef)
        {
            ExtTools kind = parseExtToolsKind(protoLayerInfo.getExtTool(), throwIfUnknown);
            if (kind != null)
            {
                Integer major = null;
                Integer minor = null;
                Integer patch = null;

                boolean isSupported = protoLayerInfo.getSupported();

                if (isSupported)
                {
                    major = protoLayerInfo.getVersionMajor();
                    minor = protoLayerInfo.getVersionMinor();
                    patch = protoLayerInfo.getVersionPatch();
                }

                ret.add(
                    new ExtToolsInfo(
                        kind,
                        isSupported,
                        major,
                        minor,
                        patch,
                        protoLayerInfo.getReasonsForNotSupportedList()
                    )
                );
            }
        }
        return ret;
    }

    public static StorPoolApi parseStorPool(StorPoolOuterClass.StorPool storPoolProto, long fullSyncId, long updateId)
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
            Optional.<Long>ofNullable(
                storPoolProto.hasFreeSpace() && storPoolProto.getFreeSpace().hasFreeCapacity() ?
                    storPoolProto.getFreeSpace().getFreeCapacity() :
                    null
            ),
            Optional.<Long>ofNullable(
                storPoolProto.hasFreeSpace() && storPoolProto.getFreeSpace().hasTotalCapacity() ?
                    storPoolProto.getFreeSpace().getTotalCapacity() :
                    null
            ),
            storPoolProto.getOversubscriptionRatio(),
            null, // we do not care what we receive from the satellite/controller here. This info is only interesting
            // for user / plugins
            null, // same as above
            null,
            storPoolProto.getIsPmem(),
            storPoolProto.getIsVdo(),
            storPoolProto.getIsExternalLocking()
        );
    }

    private ProtoDeserializationUtils()
    {
    }

    public static RscGrpPojo parseRscGrp(RscGrp rscGrpProto)
    {
        return new RscGrpPojo(
            UUID.fromString(rscGrpProto.getUuid()),
            rscGrpProto.getName(),
            rscGrpProto.getDescription(),
            rscGrpProto.getRscDfnPropsMap(),
            parseVlmGrpList(rscGrpProto.getVlmGrpList()),
            // satellite does not need the autoSelectFilter anyways
            new AutoSelectFilterBuilder().build(),
            null // satellite does not need the peer slots on RG level. RD level always has it
        );
    }

    public static List<VolumeGroupApi> parseVlmGrpList(List<VlmGrp> vlmGrpProtoList)
    {
        List<VolumeGroupApi> ret = new ArrayList<>();
        for (VlmGrp vlmGrpProto : vlmGrpProtoList)
        {
            ret.add(parseVlmGrp(vlmGrpProto));
        }
        return ret;
    }

    public static VlmGrpPojo parseVlmGrp(VlmGrp vlmGrpProto)
    {
        return new VlmGrpPojo(
            UUID.fromString(vlmGrpProto.getUuid()),
            vlmGrpProto.getVlmNr(),
            vlmGrpProto.getVlmDfnPropsMap(),
            FlagsHelper.fromStringList(VolumeGroup.Flags.class, vlmGrpProto.getFlagsList())
        );
    }

    public static @Nullable ExtTools parseExtToolsKind(ExternalTools extToolsRef, boolean throwIfUnknown)
    {
        ExtTools tool = null;
        switch (extToolsRef)
        {
            case DRBD9:
                tool = ExtTools.DRBD9_KERNEL;
                break;
            case DRBD_UTILS:
                tool = ExtTools.DRBD9_UTILS;
                break;
            case DRBD_PROXY:
                tool = ExtTools.DRBD_PROXY;
                break;
            case CRYPT_SETUP:
                tool = ExtTools.CRYPT_SETUP;
                break;
            case LVM:
                tool = ExtTools.LVM;
                break;
            case LVM_THIN:
                tool = ExtTools.LVM_THIN;
                break;
            case THIN_SEND_RECV:
                tool = ExtTools.THIN_SEND_RECV;
                break;
            case NVME:
                tool = ExtTools.NVME;
                break;
            case ZFS_KMOD:
                tool = ExtTools.ZFS_KMOD;
                break;
            case ZFS_UTILS:
                tool = ExtTools.ZFS_UTILS;
                break;
            case SPDK:
                tool = ExtTools.SPDK;
                break;
            case DM_WRITECACHE:
                tool = ExtTools.DM_WRITECACHE;
                break;
            case DM_CACHE:
                tool = ExtTools.DM_CACHE;
                break;
            case BCACHE_TOOLS:
                tool = ExtTools.BCACHE_TOOLS;
                break;
            case LOSETUP:
                tool = ExtTools.LOSETUP;
                break;
            case ZSTD:
                tool = ExtTools.ZSTD;
                break;
            case SOCAT:
                tool = ExtTools.SOCAT;
                break;
            case COREUTILS_LINUX:
                tool = ExtTools.COREUTILS_LINUX;
                break;
            case UDEVADM:
                tool = ExtTools.UDEVADM;
                break;
            case LSSCSI:
                tool = ExtTools.LSSCSI;
                break;
            case SAS_PHY:
                tool = ExtTools.SAS_PHY;
                break;
            case SAS_DEVICE:
                tool = ExtTools.SAS_DEVICE;
                break;
            case EBS_INIT:
                tool = ExtTools.EBS_INIT;
                break;
            case EBS_TARGET:
                tool = ExtTools.EBS_TARGET;
                break;
            case STORAGE_SPACES:
            case STORAGE_SPACES_THIN:
                tool = ExtTools.STORAGE_SPACES;
                break;
            case UNKNOWN: // fall-through
            case UNRECOGNIZED: // fall-through
            default:
                if (throwIfUnknown)
                {
                    throw new ImplementationError("Unknown (proto) Externaltool: " + extToolsRef);
                }
                break;
        }
        return tool;
    }

    public static List<RequestFilePojo> parseRequestedSosFiles(List<ReqFile> filesListRef)
    {
        List<RequestFilePojo> ret = new ArrayList<>();
        for (ReqFile proto : filesListRef)
        {
            ret.add(new RequestFilePojo(proto.getName(), proto.getOffset(), proto.getLength()));
        }
        return ret;
    }

    public static List<Property> parseProperties(
        List<PropertyOuterClass.Property> propertiesListRef
    )
    {
        List<Property> ret = new ArrayList<>();
        for (PropertyOuterClass.Property protoProp : propertiesListRef)
        {
            final String name = protoProp.getName();
            final String key = protoProp.getKey();
            final boolean internal = protoProp.getInternal();
            final String info = protoProp.getInfo();
            final String unit = protoProp.getUnit();
            final String dflt = protoProp.getDfltValue();
            Property prop;
            switch (protoProp.getPropType())
            {
                case BOOLEAN:
                    prop = new BooleanProperty(name, key, internal, info, unit, dflt);
                    break;
                case BOOLEAN_TRUE_FALSE:
                    prop = new BooleanTrueFalseProperty(name, key, internal, info, unit, dflt);
                    break;
                case LONG:
                    prop = new LongProperty(name, key, internal, info, unit, dflt);
                    break;
                case NUMERIC:
                    prop = new RangeProperty(
                        name,
                        key,
                        protoProp.getMin(),
                        protoProp.getMax(),
                        internal,
                        info,
                        unit,
                        dflt
                    );
                    break;
                case NUMERIC_OR_SYMBOL:
                    prop = new NumericOrSymbolProperty(
                        name,
                        key,
                        protoProp.getMin(),
                        protoProp.getMax(),
                        protoProp.getRegex(),
                        internal,
                        info,
                        unit,
                        dflt
                    );
                    break;
                case RANGE_FLOAT:
                    prop = new RangeFloatProperty(
                        name,
                        key,
                        protoProp.getMinFloat(),
                        protoProp.getMaxFloat(),
                        internal,
                        info,
                        unit,
                        dflt
                    );
                    break;
                case SYMBOL: // fall-through
                case REGEX:
                    prop = new RegexProperty(name, key, protoProp.getRegex(), internal, info, unit, dflt);
                    break;
                case STRING:
                    prop = new StringProperty(name, key, internal, info, unit, dflt);
                    break;
                case UNRECOGNIZED: // fall-through
                default:
                    throw new ImplementationError("Unknown property type: " + protoProp.getPropType().name());
            }
            ret.add(prop);
        }
        return ret;
    }

}
