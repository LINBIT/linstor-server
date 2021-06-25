package com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest;

import com.linbit.linstor.api.ApiCallRcImpl;

import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Expected response to {@link BackupShippingRequest}.
 */
public class BackupShippingResponse
{
    public final boolean canReceive;

    public final ApiCallRcImpl responses;
    /**
     * The actual IP of the target satellite.
     * This means that the src-Stlt and dst-Stlt will ship p2p, not through one or both controllers!
     */
    public final @Nullable String dstStltIp;
    public final @Nullable Integer dstStltPort;
    public final @Nullable String srcSnapDfnUuid;

    @JsonCreator
    public BackupShippingResponse(
        @JsonProperty("canReceive") boolean canReceiveRef,
        @JsonProperty("responses") ApiCallRcImpl responsesRef,
        @JsonProperty("dstStltIp") @Nullable String dstStltIpRef,
        @JsonProperty("dstStltPort") @Nullable Integer dstStltPortRef,
        @JsonProperty("srcBaseSnapDfnUuid") @Nullable String srcSnapDfnUuidRef
    )
    {
        canReceive = canReceiveRef;
        responses = responsesRef;
        dstStltIp = dstStltIpRef;
        dstStltPort = dstStltPortRef;
        srcSnapDfnUuid = srcSnapDfnUuidRef;
    }

}
