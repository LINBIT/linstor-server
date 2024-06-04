package com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest;

import com.linbit.linstor.api.ApiCallRcImpl;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class BackupShippingPrepareAbortRequest
{
    public final ApiCallRcImpl responses;
    public final String srcClusterId;
    public final Map<String, List<String>> rscAndSnapNamesToAbort;
    public final int[] srcVersion;

    @JsonCreator
    public BackupShippingPrepareAbortRequest(
        @JsonProperty("responses") ApiCallRcImpl responsesRef,
        @JsonProperty("srcClusterId") String srcClusterIdRef,
        @JsonProperty("rscAndSnapNamesToAbort") Map<String, List<String>> rscAndSnapNamesToAbortRef,
        @JsonProperty("srcVersion") int[] srcVersionRef
    )
    {
        responses = responsesRef;
        srcClusterId = srcClusterIdRef;
        rscAndSnapNamesToAbort = rscAndSnapNamesToAbortRef;
        srcVersion = srcVersionRef;
    }
}
