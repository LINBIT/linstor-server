package com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.data;

import com.linbit.linstor.api.ApiCallRcImpl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class BackupShippingReceiveDoneRequest
{
    public final ApiCallRcImpl responses;
    public final String linstorRemoteName;
    public final String stltRemoteName;
    public final String remoteUrl;

    @JsonCreator
    public BackupShippingReceiveDoneRequest(
        @JsonProperty("responses") ApiCallRcImpl responsesRef,
        @JsonProperty("linstorRemoteName") String linstorRemoteNameRef,
        @JsonProperty("stltRemoteName") String stltRemoteNameRef,
        @JsonProperty("remoteUrl") String remoteUrlRef
    )
    {
        responses = responsesRef;
        linstorRemoteName = linstorRemoteNameRef;
        stltRemoteName = stltRemoteNameRef;
        remoteUrl = remoteUrlRef;
    }
}
