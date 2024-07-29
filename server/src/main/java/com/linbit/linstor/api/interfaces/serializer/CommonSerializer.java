package com.linbit.linstor.api.interfaces.serializer;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.pojo.FileInfoPojo;
import com.linbit.linstor.api.pojo.FilePojo;
import com.linbit.linstor.api.pojo.RequestFilePojo;
import com.linbit.linstor.api.prop.WhitelistProps;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.event.EventIdentifier;
import com.linbit.linstor.event.common.ResourceState;
import com.linbit.linstor.logging.ErrorReportResult;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;

import javax.annotation.Nonnull;
import com.linbit.linstor.annotation.Nullable;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

public interface CommonSerializer
{
    CommonSerializerBuilder headerlessBuilder();

    CommonSerializerBuilder onewayBuilder(String apiCall);

    CommonSerializerBuilder apiCallBuilder(String apiCall, Long apiCallId);

    CommonSerializerBuilder answerBuilder(String msgContent, Long apiCallId);

    CommonSerializerBuilder completionBuilder(Long apiCallId);

    interface CommonSerializerBuilder
    {
        byte[] build();

        CommonSerializerBuilder authError(ApiCallRcImpl apiCallRcRef);

        @Nonnull
        CommonSerializerBuilder authSuccess(
            long expectedFullSyncId,
            @Nonnull int[] stltVersion,
            @Nonnull String nodeUname,
            @Nonnull Collection<ExtToolsInfo> layerInfoListRef,
            @Nonnull ApiCallRc responses,
            @Nonnull String configDir,
            boolean debugConsoleEnabled,
            boolean logPrintStackTrace,
            @Nonnull String logDirectory,
            @Nonnull String logLevel,
            @Nullable String logLevelLinstor,
            @Nullable String stltOverrideNodeName,
            boolean remoteSpdk,
            boolean ebs,
            @Nullable Pattern drbdKeepResPattern,
            @Nonnull String netBindAddress,
            @Nonnull Integer netPort,
            @Nonnull String netType,
            @Nonnull Set<String> extFileWhitelist,
            @Nonnull WhitelistProps whitelistProps
        );

        CommonSerializerBuilder bytes(byte[] bytes);

        CommonSerializerBuilder apiCallRcSeries(ApiCallRc apiCallRc);
        CommonSerializerBuilder apiCallAnswerMsg(ApiCallRc apiCallRc);

        CommonSerializerBuilder event(
            Integer watchId,
            EventIdentifier eventIdentifier,
            String eventStreamAction
        );

        CommonSerializerBuilder volumeDiskState(String diskState);
        CommonSerializerBuilder replicationState(String peerName, String replicationState);
        CommonSerializerBuilder donePercentageEvent(String peerName, @Nullable Float donePercentage);

        CommonSerializerBuilder resourceStateEvent(ResourceState resourceState);

        CommonSerializerBuilder connectionState(String connectionState);

        CommonSerializerBuilder requestErrorReports(
            Set<String> nodes,
            boolean withContent,
            Date since,
            Date to,
            @Nonnull Set<String> ids,
            Long limit,
            Long offset
        );

        CommonSerializerBuilder deleteErrorReports(
            @Nullable Date since,
            @Nullable Date to,
            @Nullable String exception,
            @Nullable String version,
            @Nullable List<String> ids
        );

        CommonSerializerBuilder requestSosReport(String sosReportNameRef, LocalDateTime since);

        CommonSerializerBuilder requestSosReportFiles(
            String sosReportNameRef,
            ArrayList<RequestFilePojo> nextBatchToRequestRef
        );

        CommonSerializerBuilder errorReports(@Nonnull ErrorReportResult errorReportResult);

        CommonSerializerBuilder sosReportFileInfoList(
            String nodeNameRef,
            String sosReportNameRef,
            @Nullable List<FileInfoPojo> fileListRef,
            @Nullable String errorMsgRef
        );

        CommonSerializerBuilder sosReportFiles(
            String nodeNameRef,
            String sosReportNameRef,
            List<FilePojo> filesRef
        );

        CommonSerializerBuilder cleanupSosReport(String sosReportNameRef);

        CommonSerializerBuilder filter(
            Set<NodeName> nodesFilter,
            Set<StorPoolName> storPoolFilter,
            Set<ResourceName> resourceFilter
        );
    }
}
