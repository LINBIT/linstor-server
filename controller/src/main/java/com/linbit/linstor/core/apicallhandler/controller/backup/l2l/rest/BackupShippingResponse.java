package com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest;

import javax.annotation.Nullable;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Expected response to {@link BackupShippingRequest}.
 */
public class BackupShippingResponse
{
    public final boolean canReceive;
    /**
     * Should be empty if <code>canReceive</code> is true.
     * If this list is NOT empty, all other fields (except <code>canReceive</code>) should be null
     */
    public final List<String> errorList;

    /**
     * The actual IP of the target satellite.
     * This means that the src-Stlt and dst-Stlt will ship p2p, not through one or both controllers!
     */
    public final @Nullable String dstStltIp;
    public final @Nullable Integer dstStltPort;

    @JsonCreator
    public BackupShippingResponse(
        @JsonProperty("canReceive") boolean canReceiveRef,
        @JsonProperty("errorList") List<String> errorListRef,
        @JsonProperty("dstStltIp") @Nullable String dstStltIpRef,
        @JsonProperty("dstStltPort") @Nullable Integer dstStltPortRef
    )
    {
        canReceive = canReceiveRef;
        errorList = errorListRef;
        dstStltIp = dstStltIpRef;
        dstStltPort = dstStltPortRef;
    }

}
