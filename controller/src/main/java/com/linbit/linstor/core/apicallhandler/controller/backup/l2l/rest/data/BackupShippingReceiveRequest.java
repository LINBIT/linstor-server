package com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.data;

import com.linbit.linstor.api.ApiCallRcImpl;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Request from the target Linstor cluster to the target Linstor cluster to confirm that the setup was successful and
 * shipping can now be started.
 */
public class BackupShippingReceiveRequest
{
    public final boolean canReceive;

    public final ApiCallRcImpl responses;
    public final String linstorRemoteName;
    public final String stltRemoteName;
    public final String remoteUrl;
    /**
     * The actual IP of the target satellite.
     * This means that the src-Stlt and dst-Stlt will ship p2p, not through one or both controllers!
     */
    public final String dstStltIp;
    public final Map<String, Integer> dstStltPorts;
    public final String srcSnapDfnUuid;

    public final boolean useZstd;
    public final String srcStltRemoteName;

    @JsonCreator
    public BackupShippingReceiveRequest(
        @JsonProperty("canReceive") boolean canReceiveRef,
        @JsonProperty("responses") ApiCallRcImpl responsesRef,
        @JsonProperty("linstorRemoteName") String linstorRemoteNameRef,
        @JsonProperty("stltRemoteName") String stltRemoteNameRef,
        @JsonProperty("remoteUrl") String remoteUrlRef,
        @JsonProperty("dstStltIp") String dstStltIpRef,
        @JsonProperty("dstStltPort") Map<String, Integer> snapShipPortsRef,
        @JsonProperty("srcBaseSnapDfnUuid") String srcSnapDfnUuidRef,
        @JsonProperty("useZstd") boolean useZstdRef,
        @JsonProperty("srcStltRemoteName") String srcStltRemoteNameRef
    )
    {
        canReceive = canReceiveRef;
        responses = responsesRef;
        linstorRemoteName = linstorRemoteNameRef;
        stltRemoteName = stltRemoteNameRef;
        remoteUrl = remoteUrlRef;
        dstStltIp = dstStltIpRef;
        dstStltPorts = snapShipPortsRef;
        srcSnapDfnUuid = srcSnapDfnUuidRef;
        useZstd = useZstdRef;
        srcStltRemoteName = srcStltRemoteNameRef;
    }
}
