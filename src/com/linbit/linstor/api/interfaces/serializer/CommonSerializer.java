package com.linbit.linstor.api.interfaces.serializer;

import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import com.linbit.linstor.event.EventIdentifier;
import com.linbit.linstor.logging.ErrorReport;

public interface CommonSerializer
{
    CommonSerializerBuilder builder();

    CommonSerializerBuilder builder(String apiCall);

    CommonSerializerBuilder builder(String apiCall, Integer msgId);

    interface CommonSerializerBuilder
    {
        byte[] build();

        CommonSerializerBuilder event(
            Integer watchId,
            EventIdentifier eventIdentifier,
            String eventStreamAction
        );

        CommonSerializerBuilder volumeDiskState(String diskState);

        CommonSerializerBuilder resourceStateEvent(Boolean resourceReady);

        CommonSerializerBuilder requestErrorReports(
            Set<String> nodes,
            boolean withContent,
            Optional<Date> since,
            Optional<Date> to,
            Set<String> ids
        );
        CommonSerializerBuilder errorReports(Set<ErrorReport> errorReports);
    }
}
