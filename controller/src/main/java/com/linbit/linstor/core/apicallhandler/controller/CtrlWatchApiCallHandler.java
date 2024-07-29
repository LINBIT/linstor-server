package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.response.ResponseUtils;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.repository.NodeRepository;
import com.linbit.linstor.core.repository.ResourceDefinitionRepository;
import com.linbit.linstor.event.EventBroker;
import com.linbit.linstor.event.EventIdentifier;
import com.linbit.linstor.event.ObjectIdentifier;
import com.linbit.linstor.event.UnknownEventException;
import com.linbit.linstor.event.Watch;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.UUID;

@Singleton
public class CtrlWatchApiCallHandler
{
    private final ErrorReporter errorReporter;
    private final Provider<AccessContext> peerAccCtx;
    private final Provider<Peer> peer;
    private final EventBroker eventBroker;
    private final NodeRepository nodeRepository;
    private final ResourceDefinitionRepository resourceDefinitionRepository;

    @Inject
    public CtrlWatchApiCallHandler(
        ErrorReporter errorReporterRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        Provider<Peer> peerRef,
        EventBroker eventBrokerRef,
        NodeRepository nodeRepositoryRef,
        ResourceDefinitionRepository resourceDefinitionRepositoryRef
    )
    {
        errorReporter = errorReporterRef;
        peerAccCtx = peerAccCtxRef;
        peer = peerRef;
        eventBroker = eventBrokerRef;
        nodeRepository = nodeRepositoryRef;
        resourceDefinitionRepository = resourceDefinitionRepositoryRef;
    }

    public ApiCallRc createWatch(
        int peerWatchId,
        @Nullable String eventName,
        @Nullable String nodeNameStr,
        @Nullable String resourceNameStr,
        @Nullable Integer volumeNumberInt,
        @Nullable String snapshotNameStr
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        errorReporter.logDebug("Create watch ID:%d event:%s node:%s resource:%s volume:%s",
            peerWatchId,
            eventName != null ? eventName : "<all>",
            nodeNameStr != null ? nodeNameStr : "<all>",
            resourceNameStr != null ? resourceNameStr : "<all>",
            volumeNumberInt != null ? volumeNumberInt : "<all>"
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

        VolumeNumber volumeNumber = null;
        if (volumeNumberInt != null)
        {
            try
            {
                volumeNumber = new VolumeNumber(volumeNumberInt);
            }
            catch (ValueOutOfRangeException exc)
            {
                errorMsg = "Volume number out of range: " + volumeNumberInt;
                rc = ApiConsts.FAIL_INVLD_VLM_NR;
                errorExc = exc;
            }
        }

        SnapshotName snapshotName = null;
        if (snapshotNameStr != null)
        {
            try
            {
                snapshotName = new SnapshotName(snapshotNameStr);
            }
            catch (InvalidNameException exc)
            {
                errorMsg = "Invalid name: " + exc.invalidName;
                rc = ApiConsts.FAIL_INVLD_SNAPSHOT_NAME;
                errorExc = exc;
            }
        }

        if (errorMsg == null)
        {
            try
            {
                // Watches can result in data being retrieved for objects that do not yet exist.
                // For these objects we do not know the access requirements.
                // Hence we require read access to the entire node and resource definition collections.
                nodeRepository.requireAccess(peerAccCtx.get(), AccessType.VIEW);
                resourceDefinitionRepository.requireAccess(peerAccCtx.get(), AccessType.VIEW);

                eventBroker.createWatch(peer.get(), new Watch(
                    UUID.randomUUID(), peer.get().getId(), peerWatchId,
                    new EventIdentifier(
                        eventName,
                        new ObjectIdentifier(nodeName, resourceName, volumeNumber, snapshotName, null)
                    )
                ));

                apiCallRc.addEntry("Watch created", ApiConsts.MASK_CRT | ApiConsts.CREATED);
            }
            catch (UnknownEventException exc)
            {
                errorMsg = "Unknown event name '" + exc.getEventName() + "'";
                rc = ApiConsts.FAIL_UNKNOWN_ERROR;
                errorExc = exc;
            }
            catch (AccessDeniedException exc)
            {
                errorMsg = ResponseUtils.getAccDeniedMsg(
                    peerAccCtx.get(),
                    "create a watch"
                );
                rc = ApiConsts.FAIL_ACC_DENIED_WATCH;
                errorExc = exc;
            }
        }

        if (errorMsg != null)
        {
            apiCallRc.addEntry(errorMsg, rc | ApiConsts.MASK_CRT);
            errorReporter.reportError(
                errorExc,
                peerAccCtx.get(),
                null,
                errorMsg
            );
        }
        return apiCallRc;
    }

    public ApiCallRc deleteWatch(int peerWatchId)
    {
        eventBroker.deleteWatch(peer.get().getId(), peerWatchId);

        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        apiCallRc.addEntry("Watch deleted", ApiConsts.MASK_DEL | ApiConsts.DELETED);

        return apiCallRc;
    }
}
