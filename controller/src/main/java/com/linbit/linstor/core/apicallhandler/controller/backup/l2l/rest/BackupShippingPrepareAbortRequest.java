package com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest;

import com.linbit.linstor.api.ApiCallRcImpl;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class BackupShippingPrepareAbortRequest
{
    public final ApiCallRcImpl responses;
    public final String srcClusterId;
    public final String rscName;
    public final Set<String> snapNamesToAbort;
    public final int[] srcVersion;

    @JsonCreator
    public BackupShippingPrepareAbortRequest(
        @JsonProperty("responses") ApiCallRcImpl responsesRef,
        @JsonProperty("srcClusterId") String srcClusterIdRef,
        @JsonProperty("rscName") String rscNameRef,
        @JsonProperty("snapNamesToAbort") Set<String> snapNamesToAbortRef,
        @JsonProperty("srcVersion") int[] srcVersionRef
    )
    {
        responses = responsesRef;
        srcClusterId = srcClusterIdRef;
        rscName = rscNameRef;
        snapNamesToAbort = snapNamesToAbortRef;
        srcVersion = srcVersionRef;
    }
}
