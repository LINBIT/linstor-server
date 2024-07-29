package com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.data;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRcImpl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Expected response to {@link BackupShippingRequestPrevSnap}.
 */
public class BackupShippingResponsePrevSnap
{
    public final boolean canReceive;
    public final @Nullable String prevSnapUuid;
    public final boolean resetData;
    public final @Nullable String dstBaseSnapName;
    public final @Nullable String dstActualNodeName;
    public final ApiCallRcImpl responses;

    @JsonCreator
    public BackupShippingResponsePrevSnap(
        @JsonProperty("canReceive") boolean canReceiveRef,
        @JsonProperty("prevSnapUuid") @Nullable String prevSnapUuidRef,
        @JsonProperty("resetData") boolean resetDataRef,
        @JsonProperty("dstBaseSnapName") @Nullable String dstBaseSnapNameRef,
        @JsonProperty("dstActualNodeName") @Nullable String dstActualNodeNameRef,
        @JsonProperty("responses") ApiCallRcImpl responsesRef
    )
    {
        canReceive = canReceiveRef;
        prevSnapUuid = prevSnapUuidRef;
        resetData = resetDataRef;
        dstBaseSnapName = dstBaseSnapNameRef;
        dstActualNodeName = dstActualNodeNameRef;
        responses = responsesRef;
    }
}
