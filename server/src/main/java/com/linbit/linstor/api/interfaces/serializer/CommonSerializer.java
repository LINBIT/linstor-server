package com.linbit.linstor.api.interfaces.serializer;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.event.EventIdentifier;
import com.linbit.linstor.event.common.ResourceState;
import com.linbit.linstor.logging.ErrorReport;
import com.linbit.linstor.logging.LinstorFile;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;

import javax.annotation.Nullable;

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
        CommonSerializerBuilder authSuccess(
            long expectedFullSyncId,
            int[] stltVersion,
            String nodeUname,
            Collection<ExtToolsInfo> layerInfoListRef,
            ApiCallRc responses,
            String configDir,
            boolean debugConsoleEnabled,
            boolean logPrintStackTrace,
            String logDirectory,
            String logLevel,
            String logLevelLinstor,
            String stltOverrideNodeName,
            boolean openflex,
            Pattern drbdKeepResPattern,
            String netBindAddress,
            Integer netPort,
            String netType
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

        CommonSerializerBuilder resourceStateEvent(ResourceState resourceState);

        CommonSerializerBuilder connectionState(String connectionState);

        CommonSerializerBuilder requestErrorReports(
            Set<String> nodes,
            boolean withContent,
            Date since,
            Date to,
            Set<String> ids
        );

        CommonSerializerBuilder deleteErrorReports(
            @Nullable final Date since,
            @Nullable final Date to,
            @Nullable final String exception,
            @Nullable final String version,
            @Nullable final List<String> ids
        );

        CommonSerializerBuilder requestSosReport(Date since);

        CommonSerializerBuilder errorReports(List<ErrorReport> errorReports);

        CommonSerializerBuilder sosReport(String text, Set<LinstorFile> errorReports);

        CommonSerializerBuilder filter(
            Set<NodeName> nodesFilter,
            Set<StorPoolName> storPoolFilter,
            Set<ResourceName> resourceFilter
        );


    }
}
