package com.linbit.linstor.api.interfaces.serializer;

import java.util.Date;
import java.util.Optional;
import java.util.Set;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.event.EventIdentifier;
import com.linbit.linstor.event.common.UsageState;
import com.linbit.linstor.logging.ErrorReport;

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

        CommonSerializerBuilder bytes(byte[] bytes);

        CommonSerializerBuilder apiCallRcSeries(ApiCallRc apiCallRc);

        CommonSerializerBuilder event(
            Integer watchId,
            EventIdentifier eventIdentifier,
            String eventStreamAction
        );

        CommonSerializerBuilder volumeDiskState(String diskState);

        CommonSerializerBuilder resourceStateEvent(UsageState usageState);

        CommonSerializerBuilder resourceDeploymentStateEvent(ApiCallRc apiCallRc);

        CommonSerializerBuilder requestErrorReports(
            Set<String> nodes,
            boolean withContent,
            Optional<Date> since,
            Optional<Date> to,
            Set<String> ids
        );

        CommonSerializerBuilder errorReports(Set<ErrorReport> errorReports);

        CommonSerializerBuilder hostName(String hostName);
    }
}
