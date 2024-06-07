package com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest;

import com.linbit.linstor.api.ApiCallRcImpl;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class BackupShippingPrepareAbortRequest
{
    public final ApiCallRcImpl responses;
    public final String clusterId;
    public final Map<String, List<String>> rscAndSnapNamesToAbort;
    public final int[] clusterVersion;

    @JsonCreator
    public BackupShippingPrepareAbortRequest(
        @JsonProperty("responses") ApiCallRcImpl responsesRef,
        @JsonProperty("clusterId") String clusterIdRef,
        @JsonProperty("rscAndSnapNamesToAbort") Map<String, List<String>> rscAndSnapNamesToAbortRef,
        @JsonProperty("clusterVersion") int[] clusterVersionRef
    )
    {
        responses = responsesRef;
        clusterId = clusterIdRef;
        rscAndSnapNamesToAbort = rscAndSnapNamesToAbortRef;
        clusterVersion = clusterVersionRef;
    }
}
