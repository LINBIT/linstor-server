package com.linbit.linstor.core;

import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.event.EventIdentifier;
import com.linbit.linstor.event.EventBroker;
import com.linbit.linstor.event.Watch;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ControllerSecurityModule;
import com.linbit.linstor.security.ObjectProtection;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.UUID;

public class CtrlWatchApiCallHandler
{
    private final ErrorReporter errorReporter;
    private final AccessContext accCtx;
    private final Peer peer;
    private final EventBroker eventBroker;
    private final ObjectProtection nodesMapProt;
    private final ObjectProtection rscDfnMapProt;

    @Inject
    public CtrlWatchApiCallHandler(
        ErrorReporter errorReporterRef,
        @PeerContext AccessContext accCtxRef,
        Peer peerRef,
        EventBroker eventBrokerRef,
        @Named(ControllerSecurityModule.NODES_MAP_PROT) ObjectProtection nodesMapProtRef,
        @Named(ControllerSecurityModule.RSC_DFN_MAP_PROT) ObjectProtection rscDfnMapProtRef
    )
    {
        errorReporter = errorReporterRef;
        accCtx = accCtxRef;
        peer = peerRef;
        eventBroker = eventBrokerRef;
        nodesMapProt = nodesMapProtRef;
        rscDfnMapProt = rscDfnMapProtRef;
    }

    public ApiCallRc createWatch(
        int peerWatchId,
        String eventName,
        String nodeNameStr,
        String resourceNameStr,
        Integer volumeNumber
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        errorReporter.logDebug("Create watch ID:%d event:%s node:%s resource:%s volume:%s",
            peerWatchId,
            eventName != null ? eventName : "<all>",
            nodeNameStr != null ? nodeNameStr : "<all>",
            resourceNameStr != null ? resourceNameStr : "<all>",
            volumeNumber != null ? volumeNumber : "<all>"
        );

        String errorMsg = null;
        long rc = 0;
        Exception errorExc = null;

        NodeName nodeName = null;
        if (nodeNameStr != null)
        {
            try
            {
                nodeName = new NodeName(nodeNameStr);
            }
            catch (InvalidNameException exc)
            {
                errorMsg = "Invalid name: " + exc.invalidName;
                rc = ApiConsts.FAIL_INVLD_NODE_NAME;
                errorExc = exc;
            }
        }

        ResourceName resourceName = null;
        if (resourceNameStr != null)
        {
            try
            {
                resourceName = new ResourceName(resourceNameStr);
            }
            catch (InvalidNameException exc)
            {
                errorMsg = "Invalid name: " + exc.invalidName;
                rc = ApiConsts.FAIL_INVLD_RSC_NAME;
                errorExc = exc;
            }
        }

        try
        {
            // Watches can result in data being retrieved for objects that do not yet exist.
            // For these objects we do not know the access requirements.
            // Hence we require read access to the entire node and resource definition collections.
            nodesMapProt.requireAccess(accCtx, AccessType.VIEW);
            rscDfnMapProt.requireAccess(accCtx, AccessType.VIEW);

            eventBroker.createWatch(peer.getAccessContext(), new Watch(
                UUID.randomUUID(), peer.getId(), peerWatchId,
                new EventIdentifier(
                    eventName,
                    nodeName,
                    resourceName,
                    volumeNumber != null ? new VolumeNumber(volumeNumber) : null
                )
            ));

            apiCallRc.addEntry("Watch created", ApiConsts.MASK_CRT | ApiConsts.CREATED);
        }
        catch (AccessDeniedException |
            ValueOutOfRangeException |
            LinStorDataAlreadyExistsException exc)
        {
            if (exc instanceof AccessDeniedException)
            {
                errorMsg = AbsApiCallHandler.getAccDeniedMsg(
                    accCtx,
                    "create a watch"
                );
                rc = ApiConsts.FAIL_ACC_DENIED_WATCH;
            }
            else
            if (exc instanceof ValueOutOfRangeException)
            {
                errorMsg = "Value out of range";
                rc = ApiConsts.FAIL_INVLD_VLM_NR;
            }
            else
            {
                errorMsg = "Watch ID already in use";
                rc = ApiConsts.FAIL_EXISTS_WATCH;
            }
        }

        if (errorMsg != null)
        {
            apiCallRc.addEntry(errorMsg, rc | ApiConsts.MASK_CRT);
            errorReporter.reportError(
                errorExc,
                accCtx,
                null,
                errorMsg
            );
        }
        return apiCallRc;
    }
}
