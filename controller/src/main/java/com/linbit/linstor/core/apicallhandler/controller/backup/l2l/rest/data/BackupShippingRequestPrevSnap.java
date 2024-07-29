package com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.data;

import com.linbit.linstor.annotation.Nullable;

import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Initial Request from src to dst to find out
 * * whether a shipping to this cluster is allowed
 * * whether we need to make an inc or full backup
 * * and if it is inc, which snap to use as the base snap
 * This request is expected to be answered with {@BackupShippingResponsePrevSnap}
 */
public class BackupShippingRequestPrevSnap
{
    public final int[] srcVersion;
    public final String srcClusterId;
    public final String dstRscName;
    public final Set<String> srcSnapDfnUuids;
    public final @Nullable String dstNodeName;

    @JsonCreator
    public BackupShippingRequestPrevSnap(
        @JsonProperty("srcVersion") int[] srcVersionRef,
        @JsonProperty("srcClusterId") String srcClusterIdRef,
        @JsonProperty("dstRscName") String dstRscNameRef,
        @JsonProperty("srcSnapDfnUuids") Set<String> srcSnapDfnUuidsRef,
        @JsonProperty("dstNodeName") @Nullable String dstNodeNameRef
    )
    {
        srcVersion = Objects.requireNonNull(srcVersionRef, "Version must not be null!");
        srcClusterId = Objects.requireNonNull(srcClusterIdRef, "Source cluster id must not be null!");
        dstRscName = Objects.requireNonNull(dstRscNameRef, "Target resource name must not be null!");
        srcSnapDfnUuids = Objects.requireNonNull(
            srcSnapDfnUuidsRef,
            "Source snapshot definition uuids must not be null!"
        );

        dstNodeName = dstNodeNameRef;
    }
}
