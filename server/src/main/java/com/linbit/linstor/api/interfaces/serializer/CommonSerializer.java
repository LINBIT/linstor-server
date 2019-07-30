package com.linbit.linstor.api.interfaces.serializer;

import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.event.EventIdentifier;
import com.linbit.linstor.event.common.UsageState;
import com.linbit.linstor.logging.ErrorReport;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

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
            List<DeviceLayerKind> supportedDeviceLayerRef,
            List<DeviceProviderKind> supportedDeviceProviderRef,
            ApiCallRc responses
        );

        CommonSerializerBuilder bytes(byte[] bytes);

        CommonSerializerBuilder apiCallRcSeries(ApiCallRc apiCallRc);

        CommonSerializerBuilder event(
            Integer watchId,
            EventIdentifier eventIdentifier,
            String eventStreamAction
        );

        CommonSerializerBuilder volumeDiskState(String diskState);

        CommonSerializerBuilder resourceStateEvent(UsageState usageState);

        CommonSerializerBuilder requestErrorReports(
            Set<String> nodes,
            boolean withContent,
            Optional<Date> since,
            Optional<Date> to,
            Set<String> ids
        );

        CommonSerializerBuilder errorReports(Set<ErrorReport> errorReports);

        CommonSerializerBuilder hostName(String hostName);

        CommonSerializerBuilder filter(
            Set<NodeName> nodesFilter,
            Set<StorPoolName> storPoolFilter,
            Set<ResourceName> resourceFilter
        );


    }
}
