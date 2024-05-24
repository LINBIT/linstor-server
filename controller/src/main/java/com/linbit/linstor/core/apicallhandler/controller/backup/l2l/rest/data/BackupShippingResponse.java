package com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.data;

import com.linbit.linstor.api.ApiCallRcImpl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Expected response to {@link BackupShippingRequest}.
 */
public class BackupShippingResponse
{
    public final boolean canReceive;

    public final ApiCallRcImpl responses;

    @JsonCreator
    public BackupShippingResponse(
        @JsonProperty("canReceive") boolean canReceiveRef,
        @JsonProperty("responses") ApiCallRcImpl responsesRef
    )
    {
        canReceive = canReceiveRef;
        responses = responsesRef;
    }

}
