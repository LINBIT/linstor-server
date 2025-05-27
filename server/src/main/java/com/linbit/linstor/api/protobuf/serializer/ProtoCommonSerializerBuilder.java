package com.linbit.linstor.api.protobuf.serializer;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.interfaces.RscDfnLayerDataApi;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.interfaces.VlmDfnLayerDataApi;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer.CommonSerializerBuilder;
import com.linbit.linstor.api.pojo.BCacheRscPojo;
import com.linbit.linstor.api.pojo.BCacheRscPojo.BCacheVlmPojo;
import com.linbit.linstor.api.pojo.CacheRscPojo;
import com.linbit.linstor.api.pojo.CacheRscPojo.CacheVlmPojo;
import com.linbit.linstor.api.pojo.DrbdRscPojo;
import com.linbit.linstor.api.pojo.DrbdRscPojo.DrbdRscDfnPojo;
import com.linbit.linstor.api.pojo.DrbdRscPojo.DrbdVlmDfnPojo;
import com.linbit.linstor.api.pojo.DrbdRscPojo.DrbdVlmPojo;
import com.linbit.linstor.api.pojo.FileInfoPojo;
import com.linbit.linstor.api.pojo.FilePojo;
import com.linbit.linstor.api.pojo.LuksRscPojo;
import com.linbit.linstor.api.pojo.LuksRscPojo.LuksVlmPojo;
import com.linbit.linstor.api.pojo.NvmeRscPojo;
import com.linbit.linstor.api.pojo.NvmeRscPojo.NvmeVlmPojo;
import com.linbit.linstor.api.pojo.RequestFilePojo;
import com.linbit.linstor.api.pojo.StorageRscPojo;
import com.linbit.linstor.api.pojo.WritecacheRscPojo;
import com.linbit.linstor.api.pojo.WritecacheRscPojo.WritecacheVlmPojo;
import com.linbit.linstor.api.prop.NumericOrSymbolProperty;
import com.linbit.linstor.api.prop.Property;
import com.linbit.linstor.api.prop.RangeFloatProperty;
import com.linbit.linstor.api.prop.RangeProperty;
import com.linbit.linstor.api.prop.RegexProperty;
import com.linbit.linstor.api.prop.WhitelistProps;
import com.linbit.linstor.core.apis.ResourceApi;
import com.linbit.linstor.core.apis.ResourceDefinitionApi;
import com.linbit.linstor.core.apis.ResourceGroupApi;
import com.linbit.linstor.core.apis.StorPoolApi;
import com.linbit.linstor.core.apis.StorPoolDefinitionApi;
import com.linbit.linstor.core.apis.VolumeApi;
import com.linbit.linstor.core.apis.VolumeDefinitionApi;
import com.linbit.linstor.core.apis.VolumeGroupApi;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.FreeSpaceTracker;
import com.linbit.linstor.core.objects.NetInterface;
import com.linbit.linstor.core.objects.NetInterface.EncryptionType;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceConnection;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.event.EventIdentifier;
import com.linbit.linstor.event.common.ResourceState;
import com.linbit.linstor.layer.LayerIgnoreReason;
import com.linbit.linstor.logging.ErrorReport;
import com.linbit.linstor.logging.ErrorReportResult;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.proto.MsgHeaderOuterClass;
import com.linbit.linstor.proto.common.ApiCallResponseOuterClass;
import com.linbit.linstor.proto.common.BCacheRscOuterClass.BCacheRsc;
import com.linbit.linstor.proto.common.BCacheRscOuterClass.BCacheVlm;
import com.linbit.linstor.proto.common.CacheRscOuterClass.CacheRsc;
import com.linbit.linstor.proto.common.CacheRscOuterClass.CacheVlm;
import com.linbit.linstor.proto.common.DrbdRscOuterClass.DrbdRsc;
import com.linbit.linstor.proto.common.DrbdRscOuterClass.DrbdRscDfn;
import com.linbit.linstor.proto.common.DrbdRscOuterClass.DrbdVlm;
import com.linbit.linstor.proto.common.DrbdRscOuterClass.DrbdVlmDfn;
import com.linbit.linstor.proto.common.DrbdRscOuterClass.DrbdVlmDfn.Builder;
import com.linbit.linstor.proto.common.ExternalToolsOuterClass.ExternalToolsInfo;
import com.linbit.linstor.proto.common.ExternalToolsOuterClass.ExternalToolsInfo.ExternalTools;
import com.linbit.linstor.proto.common.FilterOuterClass;
import com.linbit.linstor.proto.common.LayerTypeOuterClass.LayerType;
import com.linbit.linstor.proto.common.LuksRscOuterClass.LuksRsc;
import com.linbit.linstor.proto.common.LuksRscOuterClass.LuksVlm;
import com.linbit.linstor.proto.common.NetInterfaceOuterClass;
import com.linbit.linstor.proto.common.NodeOuterClass;
import com.linbit.linstor.proto.common.NvmeRscOuterClass.NvmeRsc;
import com.linbit.linstor.proto.common.NvmeRscOuterClass.NvmeVlm;
import com.linbit.linstor.proto.common.PropertyOuterClass;
import com.linbit.linstor.proto.common.PropertyOuterClass.Property.PropertyType;
import com.linbit.linstor.proto.common.ProviderTypeOuterClass.ProviderType;
import com.linbit.linstor.proto.common.RscConnOuterClass;
import com.linbit.linstor.proto.common.RscConnOuterClass.RscConn;
import com.linbit.linstor.proto.common.RscDfnOuterClass;
import com.linbit.linstor.proto.common.RscDfnOuterClass.RscDfnLayerData;
import com.linbit.linstor.proto.common.RscGrpOuterClass.RscGrp;
import com.linbit.linstor.proto.common.RscLayerDataOuterClass;
import com.linbit.linstor.proto.common.RscLayerDataOuterClass.RscLayerData;
import com.linbit.linstor.proto.common.RscOuterClass;
import com.linbit.linstor.proto.common.StltConfigOuterClass;
import com.linbit.linstor.proto.common.StltConfigOuterClass.StltConfig;
import com.linbit.linstor.proto.common.StorPoolDfnOuterClass;
import com.linbit.linstor.proto.common.StorPoolFreeSpaceOuterClass.StorPoolFreeSpace;
import com.linbit.linstor.proto.common.StorPoolOuterClass;
import com.linbit.linstor.proto.common.StorageRscOuterClass;
import com.linbit.linstor.proto.common.StorageRscOuterClass.DisklessVlm;
import com.linbit.linstor.proto.common.StorageRscOuterClass.EbsVlm;
import com.linbit.linstor.proto.common.StorageRscOuterClass.ExosVlm;
import com.linbit.linstor.proto.common.StorageRscOuterClass.FileThinVlm;
import com.linbit.linstor.proto.common.StorageRscOuterClass.FileVlm;
import com.linbit.linstor.proto.common.StorageRscOuterClass.LvmThinVlm;
import com.linbit.linstor.proto.common.StorageRscOuterClass.LvmVlm;
import com.linbit.linstor.proto.common.StorageRscOuterClass.RemoteSpdkVlm;
import com.linbit.linstor.proto.common.StorageRscOuterClass.SpdkVlm;
import com.linbit.linstor.proto.common.StorageRscOuterClass.StorageRsc;
import com.linbit.linstor.proto.common.StorageRscOuterClass.StorageSpacesThinVlm;
import com.linbit.linstor.proto.common.StorageRscOuterClass.StorageSpacesVlm;
import com.linbit.linstor.proto.common.StorageRscOuterClass.StorageVlm;
import com.linbit.linstor.proto.common.StorageRscOuterClass.ZfsThinVlm;
import com.linbit.linstor.proto.common.StorageRscOuterClass.ZfsVlm;
import com.linbit.linstor.proto.common.VlmDfnOuterClass;
import com.linbit.linstor.proto.common.VlmDfnOuterClass.VlmDfn;
import com.linbit.linstor.proto.common.VlmDfnOuterClass.VlmDfnLayerData;
import com.linbit.linstor.proto.common.VlmGrpOuterClass.VlmGrp;
import com.linbit.linstor.proto.common.VlmOuterClass;
import com.linbit.linstor.proto.common.VlmOuterClass.Vlm;
import com.linbit.linstor.proto.common.WritecacheRscOuterClass.WritecacheRsc;
import com.linbit.linstor.proto.common.WritecacheRscOuterClass.WritecacheVlm;
import com.linbit.linstor.proto.eventdata.EventConnStateOuterClass;
import com.linbit.linstor.proto.eventdata.EventDonePercentageOuterClass;
import com.linbit.linstor.proto.eventdata.EventReplicationStateOuterClass;
import com.linbit.linstor.proto.eventdata.EventRscStateOuterClass;
import com.linbit.linstor.proto.eventdata.EventRscStateOuterClass.EventRscState.InUse;
import com.linbit.linstor.proto.eventdata.EventRscStateOuterClass.PeerState;
import com.linbit.linstor.proto.eventdata.EventVlmDiskStateOuterClass;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntAuthResponseOuterClass.MsgIntAuthResponse;
import com.linbit.linstor.proto.requests.MsgDelErrorReportsOuterClass.MsgDelErrorReports;
import com.linbit.linstor.proto.requests.MsgReqErrorReportOuterClass.MsgReqErrorReport;
import com.linbit.linstor.proto.requests.MsgReqSosReportFilesOuterClass.MsgReqSosReportFiles;
import com.linbit.linstor.proto.requests.MsgReqSosReportFilesOuterClass.ReqFile;
import com.linbit.linstor.proto.requests.MsgReqSosReportListOuterClass.MsgReqSosReportList;
import com.linbit.linstor.proto.responses.FileOuterClass;
import com.linbit.linstor.proto.responses.MsgApiRcResponseOuterClass.MsgApiRcResponse;
import com.linbit.linstor.proto.responses.MsgErrorReportOuterClass.MsgErrorReport;
import com.linbit.linstor.proto.responses.MsgEventOuterClass;
import com.linbit.linstor.proto.responses.MsgReqSosCleanupOuterClass.MsgReqSosCleanup;
import com.linbit.linstor.proto.responses.MsgSosReportFilesReplyOuterClass.MsgSosReportFilesReply;
import com.linbit.linstor.proto.responses.MsgSosReportListReplyOuterClass.FileInfo;
import com.linbit.linstor.proto.responses.MsgSosReportListReplyOuterClass.MsgSosReportListReply;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;
import com.linbit.utils.Pair;
import com.linbit.utils.TimeUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;

public class ProtoCommonSerializerBuilder implements CommonSerializer.CommonSerializerBuilder
{
    protected final ErrorReporter errorReporter;
    protected final AccessContext serializerCtx;
    protected final ByteArrayOutputStream baos;
    private boolean exceptionoccurred;

    public ProtoCommonSerializerBuilder(
        final ErrorReporter errReporterRef,
        final AccessContext serializerCtxRef,
        @Nullable String msgContent,
        @Nullable Long apiCallId,
        boolean isAnswer
    )
    {
        this.errorReporter = errReporterRef;
        this.serializerCtx = serializerCtxRef;

        baos = new ByteArrayOutputStream();
        exceptionoccurred = false;
        if (msgContent != null || apiCallId != null)

        {
            try
            {
                header(msgContent, apiCallId, isAnswer);
            }
            catch (IOException exc)
            {
                errorReporter.reportError(exc);
                exceptionoccurred = true;
            }
        }
    }

    @Override
    public byte[] build()
    {
        byte[] ret;
        if (exceptionoccurred)
        {
            ret = new byte[0]; // do not send corrupted data
        }
        else
        {
            ret = baos.toByteArray();
        }
        return ret;
    }

    private void header(@Nullable String msgContent, Long apiCallId, boolean isAnswer) throws IOException
    {
        MsgHeaderOuterClass.MsgHeader.Builder headerBuilder = MsgHeaderOuterClass.MsgHeader.newBuilder();
        if (apiCallId == null)
        {
            headerBuilder
                .setMsgType(MsgHeaderOuterClass.MsgHeader.MsgType.ONEWAY)
                .setMsgContent(msgContent);
        }
        else
        if (msgContent == null)
        {
            headerBuilder
                .setMsgType(MsgHeaderOuterClass.MsgHeader.MsgType.COMPLETE)
                .setApiCallId(apiCallId);
        }
        else
        if (isAnswer)
        {
            headerBuilder
                .setMsgType(MsgHeaderOuterClass.MsgHeader.MsgType.ANSWER)
                .setApiCallId(apiCallId)
                .setMsgContent(msgContent);
        }
        else
        {
            headerBuilder
                .setMsgType(MsgHeaderOuterClass.MsgHeader.MsgType.API_CALL)
                .setApiCallId(apiCallId)
                .setMsgContent(msgContent);
        }
        headerBuilder
            .build()
            .writeDelimitedTo(baos);
    }

    protected void handleIOException(IOException exc)
    {
        errorReporter.reportError(exc);
        exceptionoccurred = true;
    }

    protected void handleAccessDeniedException(AccessDeniedException accDeniedExc)
    {
        errorReporter.reportError(
            new ImplementationError(
                "ProtoInterComSerializer has not enough privileges to serialize node",
                accDeniedExc
            )
        );
        exceptionoccurred = true;
    }

    @Override
    public CommonSerializer.CommonSerializerBuilder bytes(byte[] bytes)
    {
        try
        {
            baos.write(bytes);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public CommonSerializerBuilder authError(ApiCallRcImpl apiCallRc)
    {
        try
        {
            MsgIntAuthResponse.newBuilder()
                .setSuccess(false)
                .addAllResponses(serializeApiCallRc(apiCallRc))
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public CommonSerializerBuilder authSuccess(
        long expectedFullSyncIdRef,
        int[] stltVersionRef,
        String uname,
        Collection<ExtToolsInfo> extToolsList,
        ApiCallRc responses,
        String configDir,
        boolean debugConsoleEnabled,
        boolean logPrintStackTrace,
        String logDirectory,
        String logLevel,
        @Nullable String logLevelLinstorPrm,
        @Nullable String stltOverrideNodeNamePrm,
        boolean remoteSpdk,
        boolean ebs,
        @Nullable Pattern drbdKeepResPatternPrm,
        String netBindAddress,
        Integer netPort,
        String netType,
        Set<String> extFileWhitelist,
        WhitelistProps whitelistProps
    )
    {
        try
        {
            String logLevelLinstor = logLevelLinstorPrm == null || logLevelLinstorPrm.isEmpty() ?
                logLevel : logLevelLinstorPrm;

            String stltOverrideNodeName = stltOverrideNodeNamePrm != null ? stltOverrideNodeNamePrm : "";

            Pattern drbdKeepResPattern = drbdKeepResPatternPrm != null ? drbdKeepResPatternPrm : Pattern.compile("");

            MsgIntAuthResponse.newBuilder()
                .setSuccess(true)
                .setExpectedFullSyncId(expectedFullSyncIdRef)
                .setLinstorVersionMajor(stltVersionRef[0])
                .setLinstorVersionMinor(stltVersionRef[1])
                .setLinstorVersionPatch(stltVersionRef[2])
                .addAllResponses(serializeApiCallRc(responses))
                .addAllExtToolsInfo(asExternalToolsList(extToolsList))
                .setStltConfig(
                    stltConfig(
                        configDir,
                        debugConsoleEnabled,
                        logPrintStackTrace,
                        logDirectory,
                        logLevel,
                        logLevelLinstor,
                        stltOverrideNodeName,
                        remoteSpdk,
                        ebs,
                        drbdKeepResPattern,
                        netBindAddress,
                        netPort,
                        netType,
                        extFileWhitelist
                    )
                )
                .setNodeUname(uname)
                .addAllProperties(serializeDynamicProperties(whitelistProps))
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    private ArrayList<ExternalToolsInfo> asExternalToolsList(Collection<ExtToolsInfo> layerInfoListRef)
    {
        ArrayList<ExternalToolsInfo> ret = new ArrayList<>();
        for (ExtToolsInfo info : layerInfoListRef)
        {
            ExternalToolsInfo.Builder builder = ExternalToolsInfo.newBuilder();
            builder
                .setExtTool(asExtTools(info.getTool()))
                .setSupported(info.isSupported());
            if (info.isSupported())
            {
                if (info.getVersionMajor() != null)
                {
                    builder.setVersionMajor(info.getVersionMajor());
                }
                if (info.getVersionMinor() != null)
                {
                    builder.setVersionMinor(info.getVersionMinor());
                }
                if (info.getVersionPatch() != null)
                {
                    builder.setVersionPatch(info.getVersionPatch());
                }
            }
            else
            {
                builder.addAllReasonsForNotSupported(info.getNotSupportedReasons());
            }
            ret.add(builder.build());
        }
        return ret;
    }

    private ArrayList<PropertyOuterClass.Property> serializeDynamicProperties(WhitelistProps whitelistPropsRef)
    {
        Map<String, Property> dynamicProps = whitelistPropsRef.getDynamicProps();
        ArrayList<PropertyOuterClass.Property> protoProps = new ArrayList<>();
        for (Property prop : dynamicProps.values())
        {
            PropertyOuterClass.Property.Builder builder = PropertyOuterClass.Property.newBuilder();
            builder.setKey(prop.getKey());
            builder.setName(prop.getName());
            if (prop.getInfo() != null)
            {
                builder.setInfo(prop.getInfo());
            }
            builder.setInternal(prop.isInternal());
            if (prop.getUnit() != null)
            {
                builder.setUnit(prop.getUnit());
            }
            if (prop.getDflt() != null)
            {
                builder.setDfltValue(prop.getDflt());
            }
            switch (prop.getType())
            {
                case BOOLEAN:
                    builder.setPropType(PropertyType.BOOLEAN);
                    break;
                case BOOLEAN_TRUE_FALSE:
                    builder.setPropType(PropertyType.BOOLEAN_TRUE_FALSE);
                    break;
                case STRING:
                    builder.setPropType(PropertyType.STRING);
                    break;
                case LONG:
                    builder.setPropType(PropertyType.LONG);
                    break;
                case NUMERIC_OR_SYMBOL:
                    builder.setPropType(PropertyType.NUMERIC_OR_SYMBOL);
                    NumericOrSymbolProperty numericSymbolProperty = (NumericOrSymbolProperty) prop;
                    builder.setMax(numericSymbolProperty.getMax());
                    builder.setMin(numericSymbolProperty.getMin());
                    builder.setRegex(numericSymbolProperty.getValue());
                    break;
                case RANGE: // numeric
                    builder.setPropType(PropertyType.NUMERIC);
                    RangeProperty rangeProperty = (RangeProperty) prop;
                    builder.setMax(rangeProperty.getMax());
                    builder.setMin(rangeProperty.getMin());
                    break;
                case RANGE_FLOAT:
                    builder.setPropType(PropertyType.RANGE_FLOAT);
                    RangeFloatProperty rangeFloatProperty = (RangeFloatProperty) prop;
                    builder.setMaxFloat(rangeFloatProperty.getMax());
                    builder.setMinFloat(rangeFloatProperty.getMin());
                    break;
                case REGEX:
                    builder.setPropType(PropertyType.REGEX);
                    RegexProperty regexProperty = (RegexProperty) prop;
                    builder.setRegex(regexProperty.getValue());
                    break;
                case SYMBOL:
                    builder.setPropType(PropertyType.SYMBOL);
                    RegexProperty symbolProperty = (RegexProperty) prop;
                    builder.setRegex(symbolProperty.getValue());
                    break;
                default:
                    throw new ImplementationError("Unknown property type: " + prop.getType());
            }
        }
        return protoProps;
    }
    @Override
    public CommonSerializer.CommonSerializerBuilder apiCallRcSeries(ApiCallRc apiCallRc)
    {
        try
        {
            for (ApiCallResponseOuterClass.ApiCallResponse protoMsg : serializeApiCallRc(apiCallRc))
            {
                protoMsg.writeDelimitedTo(baos);
            }
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public CommonSerializer.CommonSerializerBuilder apiCallAnswerMsg(ApiCallRc apiCallRc)
    {
        try
        {
            MsgApiRcResponse.Builder bld = MsgApiRcResponse.newBuilder();
            bld.addAllResponses(serializeApiCallRc(apiCallRc));
            bld.build().writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public CommonSerializer.CommonSerializerBuilder event(
        Integer watchId,
        EventIdentifier eventIdentifier,
        String eventStreamAction
    )
    {
        try
        {
            MsgEventOuterClass.MsgEvent.Builder eventBuilder = MsgEventOuterClass.MsgEvent.newBuilder();

            eventBuilder
                .setWatchId(watchId)
                .setEventAction(eventStreamAction)
                .setEventName(eventIdentifier.getEventName());

            if (eventIdentifier.getResourceName() != null)
            {
                eventBuilder.setResourceName(eventIdentifier.getResourceName().displayValue);
            }

            if (eventIdentifier.getNodeName() != null)
            {
                eventBuilder.setNodeName(eventIdentifier.getNodeName().displayValue);
            }

            if (eventIdentifier.getVolumeNumber() != null)
            {
                eventBuilder.setVolumeNumber(eventIdentifier.getVolumeNumber().value);
            }

            if (eventIdentifier.getSnapshotName() != null)
            {
                eventBuilder.setSnapshotName(eventIdentifier.getSnapshotName().displayValue);
            }

            if (eventIdentifier.getPeerNodeName() != null)
            {
                eventBuilder.setPeerName(eventIdentifier.getPeerNodeName().displayValue);
            }

            eventBuilder.build().writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public CommonSerializer.CommonSerializerBuilder volumeDiskState(String diskState)
    {
        try
        {
            EventVlmDiskStateOuterClass.EventVlmDiskState.newBuilder()
                .setDiskState(diskState)
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public CommonSerializer.CommonSerializerBuilder replicationState(String peerName, String replicationState)
    {
        try
        {
            EventReplicationStateOuterClass.EventReplicationState.newBuilder()
                .setPeerName(peerName)
                .setReplicationState(replicationState)
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public CommonSerializer.CommonSerializerBuilder donePercentageEvent(String peerName, @Nullable Float donePercentage)
    {
        try
        {
            var edp = EventDonePercentageOuterClass.EventDonePercentage.newBuilder()
                .setPeerName(peerName);
            if (donePercentage != null)
            {
                edp.setDonePercentage(donePercentage);
            }
            edp.build().writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public CommonSerializer.CommonSerializerBuilder resourceStateEvent(ResourceState resourceState)
    {
        try
        {
            EventRscStateOuterClass.EventRscState.Builder builder = EventRscStateOuterClass.EventRscState.newBuilder();
            if (resourceState.hasAccessToUpToDateData() != null)
            {
                builder.setReady(resourceState.hasAccessToUpToDateData());
            }
            if (resourceState.getPeersConnected() != null)
            {
                Map<Integer, PeerState> peersConnected = new HashMap<>();
                for (Entry<VolumeNumber, Map<Integer, Boolean>> entry : resourceState.getPeersConnected().entrySet())
                {
                    EventRscStateOuterClass.PeerState.Builder peerStateBuilder = EventRscStateOuterClass.PeerState
                        .newBuilder();
                    for (Entry<Integer, Boolean> peerVlmConnectedState : entry.getValue().entrySet())
                    {
                        peerStateBuilder.putPeerNodeId(
                            peerVlmConnectedState.getKey(),
                            peerVlmConnectedState.getValue()
                        );
                    }
                    peersConnected.put(entry.getKey().value, peerStateBuilder.build());
                }
                builder.putAllPeersConnected(peersConnected);
            }
            builder.setInUse(asProtoInUse(resourceState.getInUse()));
            builder.setUpToDate(resourceState.getUpToDate());
            Integer promotionScore = resourceState.getPromotionScore();
            if (promotionScore != null)
            {
                builder.setPromotionScore(promotionScore);
            }
            Boolean mayPromote = resourceState.mayPromote();
            if (mayPromote != null)
            {
                builder.setMayPromote(mayPromote);
            }

            builder.build().writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public CommonSerializer.CommonSerializerBuilder connectionState(String connectionState)
    {
        try
        {
            EventConnStateOuterClass.EventConnState.newBuilder()
                .setConnectionState(connectionState)
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    private InUse asProtoInUse(Boolean inUseRef)
    {
        // we could also simply NOT set the inUse proto field which would default to
        // UNKNOWN, but this way it is more explicit and maybe easier to follow / understand
        InUse ret;
        if (inUseRef != null)
        {
            ret = inUseRef ? InUse.TRUE : InUse.FALSE;
        }
        else
        {
            ret = InUse.UNKNOWN;
        }
        return ret;
    }

    @Override
    public CommonSerializer.CommonSerializerBuilder requestErrorReports(
        Set<String> nodes,
        boolean withContent,
        @Nullable Date since,
        @Nullable Date to,
        Set<String> ids,
        @Nullable final Long limit,
        @Nullable final Long offset
    )
    {
        try
        {
            MsgReqErrorReport.Builder bld = MsgReqErrorReport.newBuilder();
            if (since != null)
            {
                bld.setSince(since.getTime());
            }
            if (to != null)
            {
                bld.setTo(to.getTime());
            }
            if (limit != null)
            {
                bld.setLimit(limit);
            }
            if (offset != null)
            {
                bld.setOffset(offset);
            }
            bld.addAllNodeNames(nodes).setWithContent(withContent).addAllIds(ids).build().writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public CommonSerializer.CommonSerializerBuilder errorReports(ErrorReportResult errorReportResult)
    {
        try
        {
            MsgErrorReport.Builder msgErrorReport = MsgErrorReport.newBuilder();
            msgErrorReport.setTotalCount(errorReportResult.getTotalCount());
            for (ErrorReport errReport : errorReportResult.getErrorReports())
            {
                com.linbit.linstor.proto.responses.MsgErrorReportOuterClass.ErrorReport.Builder protoErrorReport =
                    com.linbit.linstor.proto.responses.MsgErrorReportOuterClass.ErrorReport.newBuilder();

                protoErrorReport.setErrorTime(errReport.getDateTime().getTime());
                protoErrorReport.setNodeNames(errReport.getNodeName());
                protoErrorReport.setFilename(errReport.getFileName());
                protoErrorReport.setModule((int) errReport.getModule().getFlagValue());
                errReport.getVersion().ifPresent(protoErrorReport::setVersion);
                errReport.getPeer().ifPresent(protoErrorReport::setPeer);
                errReport.getException().ifPresent(protoErrorReport::setException);
                errReport.getExceptionMessage().ifPresent(protoErrorReport::setExceptionMessage);
                errReport.getOriginFile().ifPresent(protoErrorReport::setOriginFile);
                errReport.getOriginMethod().ifPresent(protoErrorReport::setOriginMethod);
                errReport.getOriginLine().ifPresent(protoErrorReport::setOriginLine);
                errReport.getText().ifPresent(protoErrorReport::setText);
                msgErrorReport.addErrorReports(protoErrorReport);
            }
            msgErrorReport.build().writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public CommonSerializer.CommonSerializerBuilder deleteErrorReports(
        @Nullable final Date since,
        @Nullable final Date to,
        @Nullable final String exception,
        @Nullable final String version,
        @Nullable final List<String> ids)
    {
        try
        {
            final MsgDelErrorReports.Builder bld = MsgDelErrorReports.newBuilder();
            if (since != null)
            {
                bld.setSince(since.getTime());
            }
            if (to != null)
            {
                bld.setTo(to.getTime());
            }
            if (exception != null)
            {
                bld.setException(exception);
            }
            if (version != null)
            {
                bld.setVersion(version);
            }
            if (ids != null)
            {
                bld.addAllIds(ids);
            }
            bld.build().writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    public StltConfig.Builder stltConfig(
        String configDir,
        boolean debugConsoleEnabled,
        boolean logPrintStackTrace,
        String logDirectory,
        String logLevel,
        String logLevelLinstor,
        String stltOverrideNodeName,
        boolean remoteSpdk,
        boolean ebs,
        Pattern drbdKeepResPattern,
        String netBindAddress,
        Integer netPort,
        String netType,
        Set<String> extFileWhitelist
    )
    {
        StltConfigOuterClass.StltConfig.Builder bld = StltConfigOuterClass.StltConfig.newBuilder();
        bld.setConfigDir(configDir)
            .setDebugConsoleEnabled(debugConsoleEnabled)
            .setLogPrintStackTrace(logPrintStackTrace)
            .setLogDirectory(logDirectory)
            .setLogLevel(logLevel)
            .setLogLevelLinstor(logLevelLinstor)
            .setStltOverrideNodeName(stltOverrideNodeName)
            .setRemoteSpdk(remoteSpdk)
            .setEbs(ebs)
            .setDrbdKeepResPattern(drbdKeepResPattern.toString())
            .setNetBindAddress(netBindAddress)
            .setNetPort(netPort)
            .setNetType(netType)
            .addAllWhitelistedExtFilePaths(extFileWhitelist);
        return bld;
    }

    @Override
    public CommonSerializer.CommonSerializerBuilder requestSosReport(String sosReportNameRef, LocalDateTime since)
    {
        try
        {
            MsgReqSosReportList.newBuilder()
                .setSosReportName(sosReportNameRef)
                .setSince(TimeUtils.getEpochMillis(since))
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public CommonSerializerBuilder requestSosReportFiles(
        String sosReportNameRef,
        ArrayList<RequestFilePojo> requestedFilesRef
    )
    {
        try
        {
            MsgReqSosReportFiles.newBuilder()
                .setSosReportName(sosReportNameRef)
                .addAllFiles(serializeRequestedFiles(requestedFilesRef))
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    private List<ReqFile> serializeRequestedFiles(ArrayList<RequestFilePojo> requestedFilesRef)
    {
        List<ReqFile> ret = new ArrayList<>();
        for (RequestFilePojo pojo : requestedFilesRef)
        {
            ret.add(
                ReqFile.newBuilder()
                    .setName(pojo.name)
                    .setLength(pojo.length)
                    .setOffset(pojo.offset)
                    .build()
            );
        }
        return ret;
    }

    @Override
    public CommonSerializerBuilder sosReportFileInfoList(
        String nodeNameRef,
        String sosReportNameRef,
        @Nullable List<FileInfoPojo> fileListRef,
        @Nullable String errorMsg
    )
    {
        try
        {
            MsgSosReportListReply.Builder builder = MsgSosReportListReply.newBuilder()
                .setNodeName(nodeNameRef)
                .setSosReportName(sosReportNameRef);
            if (fileListRef != null)
            {
                builder.addAllFiles(serializeSosFileList(fileListRef));
            }
            if (errorMsg != null)
            {
                builder.setErrorMessage(errorMsg);
            }

            builder
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    private List<FileInfo> serializeSosFileList(List<FileInfoPojo> fileListRef)
    {
        List<FileInfo> ret = new ArrayList<>();
        for (FileInfoPojo pojo : fileListRef)
        {
            ret.add(
                FileInfo.newBuilder()
                    .setName(pojo.fileName)
                    .setSize(pojo.size)
                    .setTime(pojo.timestamp)
                    .build()
            );
        }
        return ret;
    }

    @Override
    public CommonSerializer.CommonSerializerBuilder sosReportFiles(
        String nodeNameRef,
        String sosReportNameRef,
        List<FilePojo> filesRef
    )
    {
        try
        {
            MsgSosReportFilesReply.newBuilder()
                .setNodeName(nodeNameRef)
                .setSosReportName(sosReportNameRef)
                .addAllFiles(serializeFiles(filesRef))
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    private Iterable<FileOuterClass.File> serializeFiles(List<FilePojo> filesRef)
    {
        List<FileOuterClass.File> ret = new ArrayList<>();
        for (FilePojo pojo : filesRef)
        {
            ret.add(
                FileOuterClass.File.newBuilder()
                    .setFileName(pojo.fileName)
                    .setOffset(pojo.offset)
                    .setTime(pojo.timestamp)
                    .setContent(ByteString.copyFrom(pojo.content))
                    .build()
            );
        }
        return ret;
    }

    @Override
    public CommonSerializerBuilder cleanupSosReport(String sosReportNameRef)
    {
        try
        {
            MsgReqSosCleanup.newBuilder()
                .setSosReportName(sosReportNameRef)
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public CommonSerializer.CommonSerializerBuilder filter(
        Set<NodeName> nodesFilter,
        Set<StorPoolName> storPoolFilter,
        Set<ResourceName> resourceFilter
    )
    {
        try
        {
            FilterOuterClass.Filter.newBuilder()
                .addAllNodeNames(
                    nodesFilter.stream().map(NodeName::getDisplayName).collect(Collectors.toList()))
                .addAllStorPoolNames(
                    storPoolFilter.stream().map(StorPoolName::getDisplayName).collect(Collectors.toList()))
                .addAllResourceNames(
                    resourceFilter.stream().map(ResourceName::getDisplayName).collect(Collectors.toList()))
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    public static NodeOuterClass.Node serializeNode(
        AccessContext accCtx,
        Node node
    )
        throws AccessDeniedException
    {
        Map<String, String> nodeProps = node.getProps(accCtx).map();
        return NodeOuterClass.Node.newBuilder()
            .setUuid(node.getUuid().toString())
            .setName(node.getName().displayValue)
            .setType(node.getNodeType(accCtx).name())
            .putAllProps(nodeProps)
            .addAllNetInterfaces(
                serializeNetInterfaces(
                    accCtx,
                    node.streamNetInterfaces(accCtx).collect(Collectors.toList())
                )
            )
            .build();
    }

    public static List<NetInterfaceOuterClass.NetInterface> serializeNetInterfaces(
        AccessContext accCtx,
        List<NetInterface> netIfs
    )
        throws AccessDeniedException
    {
        List<NetInterfaceOuterClass.NetInterface> protoNetIfList = new ArrayList<>(netIfs.size());
        for (NetInterface netIf : netIfs)
        {
            protoNetIfList.add(serializeNetInterface(accCtx, netIf));
        }
        return protoNetIfList;
    }

    public static NetInterfaceOuterClass.NetInterface serializeNetInterface(
        AccessContext accCtx,
        NetInterface netIf
    )
        throws AccessDeniedException
    {
        NetInterfaceOuterClass.NetInterface.Builder builder = NetInterfaceOuterClass.NetInterface.newBuilder()
            .setUuid(netIf.getUuid().toString())
            .setName(netIf.getName().displayValue)
            .setAddress(netIf.getAddress(accCtx).getAddress());
        @Nullable TcpPortNumber stltConnPort = netIf.getStltConnPort(accCtx);
        @Nullable EncryptionType stltConnEncryptionType = netIf.getStltConnEncryptionType(accCtx);
        if (stltConnPort != null && stltConnEncryptionType != null)
        {
            builder.setStltConn(
                NetInterfaceOuterClass.StltConn.newBuilder()
                    .setStltPort(stltConnPort.value)
                    .setStltEncryptionType(stltConnEncryptionType.name())
                    .build()
            );
        }
        return builder.build();
    }

    public static RscGrp serializeResourceGroup(AccessContext accCtx, ResourceGroup rscGrp)
        throws AccessDeniedException
    {
        return serializeResourceGroup(rscGrp.getApiData(accCtx));
    }

    public static RscGrp serializeResourceGroup(ResourceGroupApi rscGrpApi)
    {
        UUID uuid = rscGrpApi.getUuid();
        if (uuid == null)
        {
            throw new ImplementationError("uuid is only allowed to be null in json-data (i.e. client<->ctrl)");
        }
        RscGrp.Builder builder = RscGrp.newBuilder()
            .setUuid(uuid.toString())
            .setName(rscGrpApi.getName())
            .putAllRscDfnProps(rscGrpApi.getProps())
            .addAllVlmGrp(serializeVolumeGroups(rscGrpApi.getVlmGrpList()));
        if (rscGrpApi.getDescription() != null)
        {
            builder.setDescription(rscGrpApi.getDescription());
        }
        return builder.build();
    }

    public static Iterable<? extends VlmGrp> serializeVolumeGroups(List<VolumeGroupApi> vlmGrpListRef)
    {
        List<VlmGrp> list = new ArrayList<>(vlmGrpListRef.size());
        for (VolumeGroupApi vlmGrpApi : vlmGrpListRef)
        {
            list.add(serializeVolumeGroup(vlmGrpApi));
        }
        return list;
    }

    public static VlmGrp serializeVolumeGroup(VolumeGroupApi vlmGrpApiRef)
    {
        return VlmGrp.newBuilder()
            .setUuid(vlmGrpApiRef.getUUID().toString())
            .setVlmNr(vlmGrpApiRef.getVolumeNr())
            .putAllVlmDfnProps(vlmGrpApiRef.getProps())
            .build();
    }

    public static RscDfnOuterClass.RscDfn serializeResourceDefinition(
        AccessContext accCtx,
        ResourceDefinition rscDfn
    )
        throws AccessDeniedException
    {
        return serializeResourceDefinition(rscDfn.getApiData(accCtx));
    }

    public static List<RscDfnOuterClass.RscDfn> serializeResourceDefinitions(
        List<ResourceDefinitionApi> rscDfnApis
    )
    {
        ArrayList<RscDfnOuterClass.RscDfn> protoRscDfs = new ArrayList<>();
        for (ResourceDefinitionApi rscDfnApi : rscDfnApis)
        {
            protoRscDfs.add(serializeResourceDefinition(rscDfnApi));
        }
        return protoRscDfs;
    }

    public static RscDfnOuterClass.RscDfn serializeResourceDefinition(
        ResourceDefinitionApi rscDfnApi
    )
    {
        RscDfnOuterClass.RscDfn.Builder builder = RscDfnOuterClass.RscDfn.newBuilder()
            .setRscName(rscDfnApi.getResourceName())
            .setRscDfnUuid(rscDfnApi.getUuid().toString())
            .setRscGrp(serializeResourceGroup(rscDfnApi.getResourceGroup()))
            .addAllRscDfnFlags(FlagsHelper.toStringList(ResourceDefinition.Flags.class, rscDfnApi.getFlags()))
            .addAllVlmDfns(serializeVolumeDefinition(rscDfnApi.getVlmDfnList()))
            .putAllRscDfnProps(rscDfnApi.getProps())
            .addAllLayerData(LayerObjectSerializer.serializeRscDfnLayerData(rscDfnApi));
        if (rscDfnApi.getExternalName() != null)
        {
            builder.setExternalName(ByteString.copyFrom(rscDfnApi.getExternalName()));
        }
        return builder.build();
    }

    public static List<VlmDfnOuterClass.VlmDfn> serializeVolumeDefinition(
        List<VolumeDefinition> vlmDfnList,
        AccessContext accCtx
    )
        throws AccessDeniedException
    {
        List<VlmDfnOuterClass.VlmDfn> protoVlmDfnList = new ArrayList<>(vlmDfnList.size());
        for (VolumeDefinition vlmDfn : vlmDfnList)
        {
            protoVlmDfnList.add(serializeVolumeDefinition(vlmDfn, accCtx));
        }
        return protoVlmDfnList;
    }

    public static VlmDfnOuterClass.VlmDfn serializeVolumeDefinition(
        VolumeDefinition vlmDfn,
        AccessContext accCtx
    )
        throws AccessDeniedException
    {
        return serializeVolumeDefinition(vlmDfn.getApiData(accCtx));
    }

    public static List<VlmDfnOuterClass.VlmDfn> serializeVolumeDefinition(
        List<VolumeDefinitionApi> vlmDfnApiList
    )
    {
        List<VlmDfnOuterClass.VlmDfn> protoVlmDfnList = new ArrayList<>(vlmDfnApiList.size());
        for (VolumeDefinitionApi vlmDfnApi : vlmDfnApiList)
        {
            protoVlmDfnList.add(serializeVolumeDefinition(vlmDfnApi));
        }
        return protoVlmDfnList;
    }

    public static VlmDfnOuterClass.VlmDfn serializeVolumeDefinition(
        VolumeDefinitionApi vlmDfnApi
    )
    {
        VlmDfnOuterClass.VlmDfn.Builder builder = VlmDfn.newBuilder()
            .setVlmDfnUuid(vlmDfnApi.getUuid().toString())
            .setVlmSize(vlmDfnApi.getSize())
            .addAllVlmFlags(VolumeDefinition.Flags.toStringList(vlmDfnApi.getFlags()))
            .putAllVlmProps(vlmDfnApi.getProps())
            .addAllLayerData(LayerObjectSerializer.seriailzeVlmDfnLayerData(vlmDfnApi.getVlmDfnLayerData()));
        if (vlmDfnApi.getVolumeNr() != null)
        {
            builder.setVlmNr(vlmDfnApi.getVolumeNr());
        }
        return builder.build();
    }

    public static RscOuterClass.Rsc serializeResource(
        AccessContext accCtx,
        Resource rsc
    )
        throws AccessDeniedException
    {
        return RscOuterClass.Rsc.newBuilder()
            .setUuid(rsc.getUuid().toString())
            .setName(rsc.getResourceDefinition().getName().displayValue)
            .setNodeUuid(rsc.getNode().getUuid().toString())
            .setNodeName(rsc.getNode().getName().displayValue)
            .setRscDfnUuid(rsc.getResourceDefinition().getUuid().toString())
            .putAllProps(rsc.getProps(accCtx).map())
            .addAllRscFlags(Resource.Flags.toStringList(rsc.getStateFlags().getFlagsBits(accCtx)))
            .addAllVlms(serializeVolumeList(accCtx, rsc.streamVolumes().collect(Collectors.toList())))
            .setLayerObject(LayerObjectSerializer.serializeLayerObject(rsc.getLayerData(accCtx), accCtx))
            .build();
    }

    public static RscOuterClass.Rsc serializeResource(
        ResourceApi rscApi
    )
    {
        return RscOuterClass.Rsc.newBuilder()
            .setUuid(rscApi.getUuid().toString())
            .setName(rscApi.getName())
            .setNodeUuid(rscApi.getNodeUuid().toString())
            .setNodeName(rscApi.getNodeName())
            .setRscDfnUuid(rscApi.getRscDfnUuid().toString())
            .putAllProps(rscApi.getProps())
            .addAllRscFlags(
                FlagsHelper.toStringList(
                    Resource.Flags.class,
                    rscApi.getFlags()
                )
            )
            .addAllVlms(serializeVolumeList(rscApi.getVlmList()))
            .setLayerObject(LayerObjectSerializer.serializeLayerObject(rscApi.getLayerData()))
            .build();
    }

    public static List<RscConnOuterClass.RscConn> serializeResourceConnections(
        AccessContext accCtx,
        List<ResourceConnection> rscConList
    )
        throws AccessDeniedException
    {
        List<RscConnOuterClass.RscConn> list = new ArrayList<>();

        for (ResourceConnection rscConn : rscConList)
        {
            list.add(serializeResourceConnection(accCtx, rscConn));
        }
        return list;
    }

    public static RscConnOuterClass.RscConn serializeResourceConnection(
        AccessContext accCtx,
        ResourceConnection rscConn
    )
        throws AccessDeniedException
    {
        RscConn.Builder builder = RscConn.newBuilder()
            .setRscConnUuid(rscConn.getUuid().toString())
            .setNodeName1(rscConn.getSourceResource(accCtx).getNode().getName().displayValue)
            .setNodeName2(rscConn.getTargetResource(accCtx).getNode().getName().displayValue)
            .setRscName(rscConn.getSourceResource(accCtx).getResourceDefinition().getName().displayValue)
            .setRsc1Uuid(rscConn.getSourceResource(accCtx).getUuid().toString())
            .setRsc2Uuid(rscConn.getTargetResource(accCtx).getUuid().toString())
            .putAllRscConnProps(rscConn.getProps(accCtx).map())
            .addAllRscConnFlags(
                FlagsHelper.toStringList(
                    ResourceConnection.Flags.class,
                    rscConn.getStateFlags().getFlagsBits(accCtx)
                )
            );
        @Nullable TcpPortNumber drbdProxyPortSource = rscConn.getDrbdProxyPortSource(accCtx);
        if (drbdProxyPortSource != null)
        {
            builder.setDrbdProxyPortSource(drbdProxyPortSource.value);
        }
        @Nullable TcpPortNumber drbdProxyPortTarget = rscConn.getDrbdProxyPortTarget(accCtx);
        if (drbdProxyPortTarget != null)
        {
            builder.setDrbdProxyPortTarget(drbdProxyPortTarget.value);
        }
        return builder.build();
    }

    public static StorPoolOuterClass.StorPool serializeStorPool(
        AccessContext accCtx,
        StorPool storPool
    )
        throws AccessDeniedException
    {
        // TODO try to call something like the following
        // return serializeStorPool(storPool.getApiData(null, null, accCtx, null, null))
        StorPoolOuterClass.StorPool.Builder builder = StorPoolOuterClass.StorPool.newBuilder()
            .setStorPoolUuid(storPool.getUuid().toString())
            .setNodeUuid(storPool.getNode().getUuid().toString())
            .setNodeName(storPool.getNode().getName().displayValue)
            .setStorPoolDfnUuid(storPool.getDefinition(accCtx).getUuid().toString())
            .setStorPoolName(storPool.getName().displayValue)
            .setProviderKind(asProviderType(storPool.getDeviceProviderKind()))
            .putAllProps(storPool.getProps(accCtx).map())
            .putAllStorPoolDfnProps(storPool.getDefinition(accCtx).getProps(accCtx).map())
            .putAllStaticTraits(storPool.getDeviceProviderKind().getStorageDriverKind().getStaticTraits())
            .setIsPmem(storPool.isPmem())
            .setIsVdo(storPool.isVDO())
            .setOversubscriptionRatio(storPool.getOversubscriptionRatio(accCtx, null))
            // no need to serialize storPool.
            .setIsExternalLocking(storPool.isExternalLocking());
        FreeSpaceTracker freeSpaceTracker = storPool.getFreeSpaceTracker();
        if (freeSpaceTracker.getTotalCapacity(accCtx).isPresent())
        {
            builder.setFreeSpace(
                StorPoolFreeSpace.newBuilder()
                    .setStorPoolName(storPool.getName().displayValue)
                    .setStorPoolUuid(storPool.getUuid().toString())
                    .setFreeCapacity(freeSpaceTracker.getFreeCapacityLastUpdated(accCtx).get())
                    .setTotalCapacity(freeSpaceTracker.getTotalCapacity(accCtx).get())
                    .build()
            );
        }
        return builder
            .setFreeSpaceMgrName(freeSpaceTracker.getName().displayValue)
            .build();
    }

    protected static ProviderType asProviderType(DeviceProviderKind deviceProviderKindRef) throws ImplementationError
    {
        ProviderType type;
        switch (deviceProviderKindRef)
        {
            case DISKLESS:
                type = ProviderType.DISKLESS;
                break;
            case LVM:
                type = ProviderType.LVM;
                break;
            case LVM_THIN:
                type = ProviderType.LVM_THIN;
                break;
            case ZFS:
                type = ProviderType.ZFS;
                break;
            case ZFS_THIN:
                type = ProviderType.ZFS_THIN;
                break;
            case FILE:
                type = ProviderType.FILE;
                break;
            case FILE_THIN:
                type = ProviderType.FILE_THIN;
                break;
            case SPDK:
                type = ProviderType.SPDK;
                break;
            case REMOTE_SPDK:
                type = ProviderType.REMOTE_SPDK;
                break;
            case EXOS:
                type = ProviderType.EXOS;
                break;
            case EBS_INIT:
                type = ProviderType.EBS_INIT;
                break;
            case EBS_TARGET:
                type = ProviderType.EBS_TARGET;
                break;
            case STORAGE_SPACES:
                type = ProviderType.STORAGE_SPACES;
                break;
            case STORAGE_SPACES_THIN:
                type = ProviderType.STORAGE_SPACES_THIN;
                break;
            case FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER:
            default:
                throw new ImplementationError("Unknown storage driver: " + deviceProviderKindRef);
        }
        return type;
    }

    public static StorPoolOuterClass.StorPool serializeStorPool(
        final StorPoolApi apiStorPool
    )
    {
        StorPoolOuterClass.StorPool.Builder storPoolBld = StorPoolOuterClass.StorPool.newBuilder()
            .setStorPoolName(apiStorPool.getStorPoolName())
            .setStorPoolUuid(apiStorPool.getStorPoolUuid().toString())
            .setNodeName(apiStorPool.getNodeName())
            .setNodeUuid(apiStorPool.getNodeUuid().toString())
            .setStorPoolDfnUuid(apiStorPool.getStorPoolDfnUuid().toString())
            .setProviderKind(asProviderType(apiStorPool.getDeviceProviderKind()))
            .putAllProps(apiStorPool.getStorPoolProps())
            .putAllStorPoolDfnProps(apiStorPool.getStorPoolDfnProps())
            .putAllStaticTraits(apiStorPool.getStorPoolStaticTraits())
            .setFreeSpaceMgrName(apiStorPool.getFreeSpaceManagerName())
            .setIsPmem(apiStorPool.isPmem())
            .setIsVdo(apiStorPool.isVDO())
            .setOversubscriptionRatio(apiStorPool.getOversubscriptionRatio())
            .setIsExternalLocking(apiStorPool.isExternalLocking());
        if (apiStorPool.getFreeCapacity().isPresent())
        {
            storPoolBld.setFreeSpace(
                StorPoolFreeSpace.newBuilder()
                    .setStorPoolName(apiStorPool.getStorPoolName())
                    .setStorPoolUuid(apiStorPool.getStorPoolUuid().toString())
                    .setFreeCapacity(apiStorPool.getFreeCapacity().get())
                    .setTotalCapacity(apiStorPool.getTotalCapacity().get()
                )
                .build()
            );
        }

        return storPoolBld.build();
    }

    public static StorPoolDfnOuterClass.StorPoolDfn serializeStorPoolDfn(
        AccessContext accCtx,
        StorPoolDefinition storPoolDfn
    )
        throws AccessDeniedException
    {
        return StorPoolDfnOuterClass.StorPoolDfn.newBuilder()
            .setStorPoolName(storPoolDfn.getName().displayValue)
            .setUuid(storPoolDfn.getUuid().toString())
            .putAllProps(storPoolDfn.getProps(accCtx).map())
            .build();
    }

    public static StorPoolDfnOuterClass.StorPoolDfn serializeStorPoolDfn(
        StorPoolDefinitionApi storPoolDfnApi
    )
    {
        return StorPoolDfnOuterClass.StorPoolDfn.newBuilder()
            .setStorPoolName(storPoolDfnApi.getName())
            .setUuid(storPoolDfnApi.getUuid().toString())
            .putAllProps(storPoolDfnApi.getProps())
            .build();
    }

    public static Iterable<? extends Vlm> serializeVolumeList(
        AccessContext accCtxRef,
        Collection<Volume> volumesRef
    )
        throws AccessDeniedException
    {
        List<VolumeApi> vlmApiList = new ArrayList<>(volumesRef.size());
        for (Volume vlm : volumesRef)
        {
            vlmApiList.add(
                vlm.getApiData(
                    vlm.isAllocatedSizeSet(accCtxRef) ?
                        vlm.getAllocatedSize(accCtxRef) :
                        null,
                    accCtxRef
                )
            );
        }
        return serializeVolumeList(vlmApiList);
    }

    private static List<VlmOuterClass.Vlm> serializeVolumeList(List<? extends VolumeApi> vlmApiList)
    {
        List<VlmOuterClass.Vlm> protoVlmList = new ArrayList<>(vlmApiList.size());
        for (VolumeApi vlmApi : vlmApiList)
        {
            Vlm.Builder builder = Vlm.newBuilder()
                .setVlmUuid(vlmApi.getVlmUuid().toString())
                .setVlmDfnUuid(vlmApi.getVlmDfnUuid().toString())
                .setVlmNr(vlmApi.getVlmNr())
                .addAllVlmFlags(Volume.Flags.toStringList(vlmApi.getFlags()))
                .putAllVlmProps(vlmApi.getVlmProps());
            if (vlmApi.getDevicePath() != null)
            {
                builder.setDevicePath(vlmApi.getDevicePath());
            }
            vlmApi.getAllocatedSize().ifPresent(builder::setAllocatedSize);
            vlmApi.getUsableSize().ifPresent(builder::setUsableSize);

            protoVlmList.add(builder.build());
        }
        return protoVlmList;
    }

    public static List<ApiCallResponseOuterClass.ApiCallResponse> serializeApiCallRc(ApiCallRc apiCallRc)
    {
        List<ApiCallResponseOuterClass.ApiCallResponse> list = new ArrayList<>();

        for (ApiCallRc.RcEntry apiCallEntry : apiCallRc)
        {
            ApiCallResponseOuterClass.ApiCallResponse.Builder msgApiCallResponseBuilder =
                ApiCallResponseOuterClass.ApiCallResponse.newBuilder();

            msgApiCallResponseBuilder.setRetCode(apiCallEntry.getReturnCode());
            if (apiCallEntry.getCause() != null)
            {
                msgApiCallResponseBuilder.setCause(apiCallEntry.getCause());
            }
            if (apiCallEntry.getCorrection() != null)
            {
                msgApiCallResponseBuilder.setCorrection(apiCallEntry.getCorrection());
            }
            if (apiCallEntry.getDetails() != null)
            {
                msgApiCallResponseBuilder.setDetails(apiCallEntry.getDetails());
            }
            if (apiCallEntry.getMessage() != null)
            {
                msgApiCallResponseBuilder.setMessage(apiCallEntry.getMessage());
            }
            else
            {
                msgApiCallResponseBuilder.setMessage("- no message -");
            }
            msgApiCallResponseBuilder.addAllErrorReportIds(apiCallEntry.getErrorIds());
            msgApiCallResponseBuilder.putAllObjRefs(apiCallEntry.getObjRefs());

            ApiCallResponseOuterClass.ApiCallResponse protoMsg = msgApiCallResponseBuilder.build();

            list.add(protoMsg);
        }

        return list;
    }

    private static LayerType asLayerType(final DeviceLayerKind kind)
    {
        LayerType layerType; // WOHOOO checkstyle
        switch (kind)
        {
            case LUKS:
                layerType = LayerType.LUKS;
                break;
            case DRBD:
                layerType = LayerType.DRBD;
                break;
            case STORAGE:
                layerType = LayerType.STORAGE;
                break;
            case NVME:
                layerType = LayerType.NVME;
                break;
            case WRITECACHE:
                layerType = LayerType.WRITECACHE;
                break;
            case CACHE:
                layerType = LayerType.CACHE;
                break;
            case BCACHE:
                layerType = LayerType.BCACHE;
                break;
            default: throw new RuntimeException("Not implemented.");
        }
        return layerType;
    }

    private ExternalTools asExtTools(ExtTools toolRef)
    {
        ExternalTools ret;
        switch (toolRef)
        {
            case CRYPT_SETUP:
                ret = ExternalTools.CRYPT_SETUP;
                break;
            case DRBD9_KERNEL:
                ret = ExternalTools.DRBD9;
                break;
            case DRBD9_UTILS:
                ret = ExternalTools.DRBD_UTILS;
                break;
            case DRBD_PROXY:
                ret = ExternalTools.DRBD_PROXY;
                break;
            case LVM:
                ret = ExternalTools.LVM;
                break;
            case LVM_THIN:
                ret = ExternalTools.LVM_THIN;
                break;
            case THIN_SEND_RECV:
                ret = ExternalTools.THIN_SEND_RECV;
                break;
            case NVME:
                ret = ExternalTools.NVME;
                break;
            case ZFS_KMOD:
                ret = ExternalTools.ZFS_KMOD;
                break;
            case ZFS_UTILS:
                ret = ExternalTools.ZFS_UTILS;
                break;
            case SPDK:
                ret = ExternalTools.SPDK;
                break;
            case DM_WRITECACHE:
                ret = ExternalTools.DM_WRITECACHE;
                break;
            case DM_CACHE:
                ret = ExternalTools.DM_CACHE;
                break;
            case BCACHE_TOOLS:
                ret = ExternalTools.BCACHE_TOOLS;
                break;
            case LOSETUP:
                ret = ExternalTools.LOSETUP;
                break;
            case ZSTD:
                ret = ExternalTools.ZSTD;
                break;
            case SOCAT:
                ret = ExternalTools.SOCAT;
                break;
            case COREUTILS_LINUX:
                ret = ExternalTools.COREUTILS_LINUX;
                break;
            case UDEVADM:
                ret = ExternalTools.UDEVADM;
                break;
            case LSSCSI:
                ret = ExternalTools.LSSCSI;
                break;
            case SAS_DEVICE:
                ret = ExternalTools.SAS_DEVICE;
                break;
            case SAS_PHY:
                ret = ExternalTools.SAS_PHY;
                break;
            case EBS_INIT:
                ret = ExternalTools.EBS_INIT;
                break;
            case EBS_TARGET:
                ret = ExternalTools.EBS_TARGET;
                break;
            case STORAGE_SPACES:
                ret = ExternalTools.STORAGE_SPACES;
                break;
            default:
                throw new RuntimeException("Not implemented.");
        }
        return ret;
    }

    public static class LayerObjectSerializer
    {
        public static List<RscDfnLayerData> serializeRscDfnLayerData(ResourceDefinitionApi rscDfnApi)
        {
            List<RscDfnLayerData> ret = new ArrayList<>();

            for (Pair<String, RscDfnLayerDataApi> pair : rscDfnApi.getLayerData())
            {
                String kind = pair.objA;
                RscDfnLayerDataApi rscDfnLayerDataApi = pair.objB;

                RscDfnLayerData.Builder builder = RscDfnLayerData.newBuilder()
                    .setLayerType(asLayerType(LinstorParsingUtils.asDeviceLayerKind(kind)));
                if (rscDfnLayerDataApi != null)
                {
                    switch (rscDfnLayerDataApi.getLayerKind())
                    {
                        case DRBD:
                            builder.setDrbd(
                                buildDrbdRscDfnData((DrbdRscDfnPojo) rscDfnLayerDataApi)
                            );
                            break;
                        case LUKS: // fall-through
                        case STORAGE: // fall-through
                        case NVME: // fall-through
                        case WRITECACHE: // fall-through
                        case CACHE: // fall-through
                        case BCACHE:
                            // no rsc-dfn related data
                            break;
                        default:
                            break;

                    }
                }
                ret.add(builder.build());
            }
            return ret;
        }

        public static List<VlmDfnLayerData> seriailzeVlmDfnLayerData(
            List<Pair<String, VlmDfnLayerDataApi>> vlmDfnLayerDataList
        )
        {
            List<VlmDfnLayerData> ret = new ArrayList<>();
            for (Pair<String, VlmDfnLayerDataApi> pair : vlmDfnLayerDataList)
            {
                String kind = pair.objA;
                VlmDfnLayerDataApi vlmDfnLayerDataApi = pair.objB;

                VlmDfnLayerData.Builder builder = VlmDfnLayerData.newBuilder()
                    .setLayerType(asLayerType(LinstorParsingUtils.asDeviceLayerKind(kind)));
                if (vlmDfnLayerDataApi != null)
                {
                    switch (vlmDfnLayerDataApi.getLayerKind())
                    {
                        case DRBD:
                            builder.setDrbd(
                                buildDrbdVlmDfnData((DrbdVlmDfnPojo) vlmDfnLayerDataApi)
                            );
                            break;
                        case LUKS: // fall-through
                        case STORAGE: // fall-through
                        case NVME: // fall-through
                        case WRITECACHE: // fall-through
                        case CACHE: // fall-through
                        case BCACHE:
                            // no vlm-dfn related data
                            break;
                        default:
                            break;
                    }
                }
                ret.add(builder.build());
            }
            return ret;
        }

        public static RscLayerDataOuterClass.RscLayerData serializeLayerObject(
            AbsRscLayerObject<?> layerData,
            AccessContext accCtx
        )
            throws AccessDeniedException
        {
            return serializeLayerObject(layerData.asPojo(accCtx));
        }

        public static RscLayerDataOuterClass.RscLayerData serializeLayerObject(
            RscLayerDataApi rscLayerPojo
        )
        {
            RscLayerData.Builder builder = RscLayerData.newBuilder();

            List<RscLayerData> serializedChildren = new ArrayList<>();
            for (RscLayerDataApi childRscObj : rscLayerPojo.getChildren())
            {
                serializedChildren.add(serializeLayerObject(childRscObj));
            }
            builder
                .setId(rscLayerPojo.getId())
                .setRscNameSuffix(rscLayerPojo.getRscNameSuffix())
                .addAllChildren(serializedChildren);
            switch (rscLayerPojo.getLayerKind())
            {
                case DRBD:
                    builder.setDrbd(buildDrbdRscData((DrbdRscPojo) rscLayerPojo));
                    break;
                case LUKS:
                    builder.setLuks(buildLuksRscData((LuksRscPojo) rscLayerPojo));
                    break;
                case STORAGE:
                    builder.setStorage(buildStorageRscData((StorageRscPojo) rscLayerPojo));
                    break;
                case NVME:
                    builder.setNvme(buildNvmeRscData((NvmeRscPojo) rscLayerPojo));
                    break;
                case WRITECACHE:
                    builder.setWritecache(buildWritecacheRscData((WritecacheRscPojo) rscLayerPojo));
                    break;
                case CACHE:
                    builder.setCache(buildCacheRscData((CacheRscPojo) rscLayerPojo));
                    break;
                case BCACHE:
                    builder.setBcache(buildBCacheRscData((BCacheRscPojo) rscLayerPojo));
                    break;
                default:
                    break;
            }
            builder.setLayerType(asLayerType(rscLayerPojo.getLayerKind()));
            builder.setSuspend(rscLayerPojo.getSuspend());
            for (LayerIgnoreReason reason : rscLayerPojo.getIgnoreReasons())
            {
                builder.addIgnoreReasons(reason.toString());
            }
            return builder.build();
        }

        private static DrbdRsc buildDrbdRscData(DrbdRscPojo drbdRscPojo)
        {
            DrbdRscDfnPojo drbdRscDfnPojo = drbdRscPojo.getDrbdRscDfn();

            List<DrbdVlm> serializedDrbdVlms = new ArrayList<>();
            for (DrbdVlmPojo drbdVlmPojo : drbdRscPojo.getVolumeList())
            {
                serializedDrbdVlms.add(buildDrbdVlm(drbdVlmPojo));
            }

            DrbdRsc.Builder builder = DrbdRsc.newBuilder();
            builder.setDrbdRscDfn(buildDrbdRscDfnData(drbdRscDfnPojo))
                .setNodeId(drbdRscPojo.getNodeId())
                .setPeersSlots(drbdRscPojo.getPeerSlots())
                .setAlStripes(drbdRscPojo.getAlStripes())
                .setAlSize(drbdRscPojo.getAlStripeSize())
                .setFlags(drbdRscPojo.getFlags())
                .addAllDrbdVlms(serializedDrbdVlms);

            @Nullable Set<Integer> ports = drbdRscPojo.getPorts();
            if (ports != null)
            {
                builder.addAllPorts(ports);
            }
            // portCount is not sent via protobuf

            return builder.build();
        }

        private static DrbdVlm buildDrbdVlm(DrbdVlmPojo drbdVlmPojo)
        {
            DrbdVlmDfnPojo drbdVlmDfnPojo = drbdVlmPojo.getDrbdVlmDfn();
            DrbdVlm.Builder builder = DrbdVlm.newBuilder()
                .setDrbdVlmDfn(buildDrbdVlmDfnData(drbdVlmDfnPojo))
                .setAllocatedSize(drbdVlmPojo.getAllocatedSize())
                .setUsableSize(drbdVlmPojo.getUsableSize())
                .setDiscGran(drbdVlmPojo.getDiscGran());
            if (drbdVlmPojo.getDevicePath() != null)
            {
                builder.setDevicePath(drbdVlmPojo.getDevicePath());
            }
            if (drbdVlmPojo.getDataDevice() != null)
            {
                builder.setBackingDevice(drbdVlmPojo.getDataDevice());
            }
            if (drbdVlmPojo.getMetaDevice() != null)
            {
                builder.setMetaDisk(drbdVlmPojo.getMetaDevice());
            }
            if (drbdVlmPojo.getDiskState() != null)
            {
                builder.setDiskState(drbdVlmPojo.getDiskState());
            }
            if (drbdVlmPojo.getExternalMetaDataStorPool() != null)
            {
                builder.setExternalMetaDataStorPool(drbdVlmPojo.getExternalMetaDataStorPool());
            }

            DrbdVlm drbbVlm = builder.build();
            return drbbVlm;
        }

        private static DrbdRscDfn buildDrbdRscDfnData(DrbdRscDfnPojo drbdRscDfnPojo)
        {
            DrbdRscDfn.Builder builder = DrbdRscDfn.newBuilder()
                .setRscNameSuffix(drbdRscDfnPojo.getRscNameSuffix())
                .setPeersSlots(drbdRscDfnPojo.getPeerSlots())
                .setAlStripes(drbdRscDfnPojo.getAlStripes())
                .setAlSize(drbdRscDfnPojo.getAlStripeSize())
                .setTransportType(drbdRscDfnPojo.getTransportType())
                .setDown(drbdRscDfnPojo.isDown());
            String secret = drbdRscDfnPojo.getSecret();
            if (secret != null)
            {
                builder.setSecret(secret);
            }
            return builder.build();
        }

        private static DrbdVlmDfn buildDrbdVlmDfnData(DrbdVlmDfnPojo drbdVlmDfnPojo)
        {
            Builder builder = DrbdVlmDfn.newBuilder()
                .setRscNameSuffix(drbdVlmDfnPojo.getRscNameSuffix())
                .setVlmNr(drbdVlmDfnPojo.getVlmNr());
            Integer minorNr = drbdVlmDfnPojo.getMinorNr();
            if (minorNr != null)
            {
                builder.setMinor(minorNr);
            }
            return builder.build();
        }

        private static LuksRsc buildLuksRscData(LuksRscPojo rscLayerPojoRef)
        {
            List<LuksVlm> luksVlms = new ArrayList<>();
            for (LuksVlmPojo luksVlmPojo : rscLayerPojoRef.getVolumeList())
            {
                luksVlms.add(buildLuksVlm(luksVlmPojo));
            }

            return LuksRsc.newBuilder()
                .addAllLuksVlms(luksVlms)
                .build();
        }

        private static LuksVlm buildLuksVlm(LuksVlmPojo luksVlmPojo)
        {
            LuksVlm.Builder builder = LuksVlm.newBuilder()
                .setVlmNr(luksVlmPojo.getVlmNr())
                .setEncryptedPassword(ByteString.copyFrom(luksVlmPojo.getEncryptedPassword()))
                .setAllocatedSize(luksVlmPojo.getAllocatedSize())
                .setUsableSize(luksVlmPojo.getUsableSize())
                .setDiscGran(luksVlmPojo.getDiscGran())
                .setOpened(luksVlmPojo.isOpen());
            if (luksVlmPojo.getDevicePath() != null)
            {
                builder.setDevicePath(luksVlmPojo.getDevicePath());
            }
            if (luksVlmPojo.getDataDevice() != null)
            {
                builder.setDataDevice(luksVlmPojo.getDataDevice());
            }
            if (luksVlmPojo.getDiskState() != null)
            {
                builder.setDiskState(luksVlmPojo.getDiskState());
            }
            if (luksVlmPojo.getModifyPassword() != null)
            {
                builder.setModifyPassword(ByteString.copyFrom(luksVlmPojo.getModifyPassword()));
            }
            return builder.build();
        }

        private static NvmeRsc buildNvmeRscData(NvmeRscPojo rscLayerPojoRef)
        {
            List<NvmeVlm> nvmeVlms = new ArrayList<>();
            for (NvmeVlmPojo nvmeVlmPojo : rscLayerPojoRef.getVolumeList())
            {
                nvmeVlms.add(buildNvmeVlm(nvmeVlmPojo));
            }

            return NvmeRsc.newBuilder()
                .setFlags(0) // TODO serialize flags as soon NvmeRscData get flags
                .addAllNvmeVlms(nvmeVlms)
                .build();
        }

        private static WritecacheRsc buildWritecacheRscData(WritecacheRscPojo rscLayerPojoRef)
        {
            List<WritecacheVlm> protoVlms = new ArrayList<>();
            for (WritecacheVlmPojo vlmPojo : rscLayerPojoRef.getVolumeList())
            {
                protoVlms.add(buildWritecacheVlm(vlmPojo));
            }

            return WritecacheRsc.newBuilder()
                .setFlags(0)
                .addAllVlms(protoVlms)
                .build();
        }

        private static CacheRsc buildCacheRscData(CacheRscPojo rscLayerPojoRef)
        {
            List<CacheVlm> protoVlms = new ArrayList<>();
            for (CacheVlmPojo vlmPojo : rscLayerPojoRef.getVolumeList())
            {
                protoVlms.add(buildCacheVlm(vlmPojo));
            }

            return CacheRsc.newBuilder()
                .setFlags(0)
                .addAllVlms(protoVlms)
                .build();
        }

        private static BCacheRsc buildBCacheRscData(BCacheRscPojo rscLayerPojoRef)
        {
            List<BCacheVlm> protoVlms = new ArrayList<>();
            for (BCacheVlmPojo vlmPojo : rscLayerPojoRef.getVolumeList())
            {
                protoVlms.add(buildBCacheVlm(vlmPojo));
            }

            return BCacheRsc.newBuilder()
                .setFlags(0)
                .addAllVlms(protoVlms)
                .build();
        }

        private static StorageRsc buildStorageRscData(StorageRscPojo rscLayerPojoRef)
        {
            List<StorageVlm> storageVlms = new ArrayList<>();
            for (VlmLayerDataApi vlmPojo : rscLayerPojoRef.getVolumeList())
            {
                StorageVlm storageVlm = buildStorageVlm(vlmPojo);
                storageVlms.add(storageVlm);
            }
            return StorageRsc.newBuilder()
                .addAllStorageVlms(storageVlms)
                .build();
        }

        private static StorageVlm buildStorageVlm(VlmLayerDataApi vlmPojo) throws ImplementationError
        {
            StorageVlm.Builder builder = StorageVlm.newBuilder()
                .setVlmNr(vlmPojo.getVlmNr())
                .setAllocatedSize(vlmPojo.getAllocatedSize())
                .setUsableSize(vlmPojo.getUsableSize())
                .setDiscGran(vlmPojo.getDiscGran())
                .setExists(vlmPojo.exists())
                .setStoragePool(serializeStorPool(vlmPojo.getStorPoolApi()));
            if (vlmPojo.getDevicePath() != null)
            {
                builder.setDevicePath(vlmPojo.getDevicePath());
            }
            if (vlmPojo.getDiskState() != null)
            {
                builder.setDiskState(vlmPojo.getDiskState());
            }

            switch (vlmPojo.getProviderKind())
            {
                case DISKLESS:
                    builder.setDiskless(DisklessVlm.newBuilder().build());
                    break;
                case LVM:
                    builder.setLvm(LvmVlm.newBuilder().build());
                    break;
                case LVM_THIN:
                    builder.setLvmThin(LvmThinVlm.newBuilder().build());
                    break;
                case STORAGE_SPACES:
                    builder.setStorageSpaces(StorageSpacesVlm.newBuilder().build());
                    break;
                case STORAGE_SPACES_THIN:
                    builder.setStorageSpacesThin(StorageSpacesThinVlm.newBuilder().build());
                    break;
                case ZFS:
                {
                    StorageRscOuterClass.ZfsVlm.Builder protoZfsVlmBuilder = ZfsVlm.newBuilder();
                    if (vlmPojo.getExtentSize() != null)
                    {
                        protoZfsVlmBuilder.setExtentSize(vlmPojo.getExtentSize());
                    }
                    builder.setZfs(protoZfsVlmBuilder.build());
                }
                    break;
                case ZFS_THIN:
                {
                    StorageRscOuterClass.ZfsThinVlm.Builder protoZfsVlmBuilder = ZfsThinVlm.newBuilder();
                    if (vlmPojo.getExtentSize() != null)
                    {
                        protoZfsVlmBuilder.setExtentSize(vlmPojo.getExtentSize());
                    }
                    builder.setZfsThin(protoZfsVlmBuilder.build());
                }
                    break;
                case FILE:
                    builder.setFile(FileVlm.newBuilder().build());
                    break;
                case FILE_THIN:
                    builder.setFileThin(FileThinVlm.newBuilder().build());
                    break;
                case SPDK:
                    builder.setSpdk(SpdkVlm.newBuilder().build());
                    break;
                case REMOTE_SPDK:
                    builder.setRemoteSpdk(RemoteSpdkVlm.newBuilder().build());
                    break;
                case EXOS:
                    builder.setExos(ExosVlm.newBuilder().build());
                    break;
                case EBS_INIT: // fall-through
                case EBS_TARGET:
                    EbsVlm.Builder ebsVlmBuilder = EbsVlm.newBuilder();
                    builder.setEbs(ebsVlmBuilder.build());
                    break;
                case FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER:
                default:
                    throw new ImplementationError("Unexpected provider kind: " + vlmPojo.getProviderKind());
            }
            return builder.build();
        }

        private static NvmeVlm buildNvmeVlm(NvmeVlmPojo nvmeVlmPojo)
        {
            NvmeVlm.Builder nvmeVlmBuilder = NvmeVlm.newBuilder()
                .setVlmNr(nvmeVlmPojo.getVlmNr())
                .setAllocatedSize(nvmeVlmPojo.getAllocatedSize())
                .setUsableSize(nvmeVlmPojo.getUsableSize())
                .setDiscGran(nvmeVlmPojo.getDiscGran());
            if (nvmeVlmPojo.getDevicePath() != null)
            {
                nvmeVlmBuilder.setDevicePath(nvmeVlmPojo.getDevicePath());
            }
            if (nvmeVlmPojo.getBackingDisk() != null)
            {
                nvmeVlmBuilder.setBackingDevice(nvmeVlmPojo.getBackingDisk());
            }
            if (nvmeVlmPojo.getDiskState() != null)
            {
                nvmeVlmBuilder.setDiskState(nvmeVlmPojo.getDiskState());
            }

            return nvmeVlmBuilder.build();
        }

        private static WritecacheVlm buildWritecacheVlm(WritecacheVlmPojo vlmPojo)
        {
            WritecacheVlm.Builder protoVlmBuilder = WritecacheVlm.newBuilder()
                .setVlmNr(vlmPojo.getVlmNr())
                .setAllocatedSize(vlmPojo.getAllocatedSize())
                .setUsableSize(vlmPojo.getUsableSize())
                .setDiscGran(vlmPojo.getDiscGran());
            if (vlmPojo.getDevicePath() != null)
            {
                protoVlmBuilder.setDevicePath(vlmPojo.getDevicePath());
            }
            if (vlmPojo.getDataDevice() != null)
            {
                protoVlmBuilder.setDataDevice(vlmPojo.getDataDevice());
            }
            if (vlmPojo.getCacheDevice() != null)
            {
                protoVlmBuilder.setCacheDevice(vlmPojo.getCacheDevice());
            }
            if (vlmPojo.getDiskState() != null)
            {
                protoVlmBuilder.setDiskState(vlmPojo.getDiskState());
            }
            if (vlmPojo.getCacheStorPoolName() != null)
            {
                protoVlmBuilder.setCacheStorPoolName(vlmPojo.getCacheStorPoolName());
            }

            return protoVlmBuilder.build();
        }

        private static CacheVlm buildCacheVlm(CacheVlmPojo vlmPojo)
        {
            CacheVlm.Builder protoVlmBuilder = CacheVlm.newBuilder()
                .setVlmNr(vlmPojo.getVlmNr())
                .setAllocatedSize(vlmPojo.getAllocatedSize())
                .setUsableSize(vlmPojo.getUsableSize())
                .setDiscGran(vlmPojo.getDiscGran());
            if (vlmPojo.getDevicePath() != null)
            {
                protoVlmBuilder.setDevicePath(vlmPojo.getDevicePath());
            }
            if (vlmPojo.getDataDevice() != null)
            {
                protoVlmBuilder.setDataDevice(vlmPojo.getDataDevice());
            }
            if (vlmPojo.getCacheDevice() != null)
            {
                protoVlmBuilder.setCacheDevice(vlmPojo.getCacheDevice());
            }
            if (vlmPojo.getMetaDevice() != null)
            {
                protoVlmBuilder.setMetaDevice(vlmPojo.getMetaDevice());
            }
            if (vlmPojo.getDiskState() != null)
            {
                protoVlmBuilder.setDiskState(vlmPojo.getDiskState());
            }
            if (vlmPojo.getCacheStorPoolName() != null)
            {
                protoVlmBuilder.setCacheStorPoolName(vlmPojo.getCacheStorPoolName());
            }
            if (vlmPojo.getMetaStorPoolName() != null)
            {
                protoVlmBuilder.setMetaStorPoolName(vlmPojo.getMetaStorPoolName());
            }

            return protoVlmBuilder.build();
        }

        private static BCacheVlm buildBCacheVlm(BCacheVlmPojo vlmPojo)
        {
            BCacheVlm.Builder protoVlmBuilder = BCacheVlm.newBuilder()
                .setVlmNr(vlmPojo.getVlmNr())
                .setAllocatedSize(vlmPojo.getAllocatedSize())
                .setUsableSize(vlmPojo.getUsableSize())
                .setDiscGran(vlmPojo.getDiscGran());
            if (vlmPojo.getDevicePath() != null)
            {
                protoVlmBuilder.setDevicePath(vlmPojo.getDevicePath());
            }
            if (vlmPojo.getDataDevice() != null)
            {
                protoVlmBuilder.setDataDevice(vlmPojo.getDataDevice());
            }
            if (vlmPojo.getCacheDevice() != null)
            {
                protoVlmBuilder.setCacheDevice(vlmPojo.getCacheDevice());
            }
            if (vlmPojo.getDiskState() != null)
            {
                protoVlmBuilder.setDiskState(vlmPojo.getDiskState());
            }
            if (vlmPojo.getCacheStorPoolName() != null)
            {
                protoVlmBuilder.setCacheStorPoolName(vlmPojo.getCacheStorPoolName());
            }

            return protoVlmBuilder.build();
        }

        private LayerObjectSerializer()
        {
        }
    }
}
