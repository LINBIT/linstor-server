package com.linbit.drbdmanage.core;

import java.util.List;
import java.util.Map;

import com.linbit.drbdmanage.ApiCallRc;
import com.linbit.drbdmanage.Volume;
import com.linbit.drbdmanage.VolumeDefinition;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.security.AccessContext;

public class CtrlApiCallHandler
{
    public static final String PROPS_NODE_TYPE_KEY = "nodeType";
    public static final String PROPS_NODE_FLAGS_KEY = "nodeFlags";

    public static final String PROPS_RESOURCE_DEFINITION_PEER_COUNT_KEY = "rscDfnPeerCountKey";
    public static final String PROPS_RESOURCE_DEFINITION_AL_SIZE_KEY = "rscDfnAlSizeKey";
    public static final String PROPS_RESOURCE_DEFINITION_AL_STRIPES_KEY = "rscDfnAlStripesKey";

    public static final String API_RC_VAR_NODE_NAME_KEY = "nodeName";
    public static final String API_RC_VAR_RESOURCE_NAME_KEY = "resName";
    public static final String API_RC_VAR_VOlUME_NUMBER_KEY = "volNr";
    public static final String API_RC_VAR_VOlUME_MINOR_KEY = "volMinor";
    public static final String API_RC_VAR_VOlUME_SIZE_KEY = "volSize";
    public static final String API_RC_VAR_RESOURCE_PEER_COUNT_KEY = "peerCount";
    public static final String API_RC_VAR_RESOURCE_AL_STRIPES_KEY = "alStripes";
    public static final String API_RC_VAR_RESOURCE_AL_SIZE_KEY = "alSize";
    public static final String API_RC_VAR_ACC_CTX_ID_KEY = "accCtxId";
    public static final String API_RC_VAR_ACC_CTX_ROLE_KEY = "accCtxRole";

    private final CtrlNodeApiCallHandler nodeApiCallHandler;
    private final CtrlResourceApiCallHandler resourceApiCallHandler;
    private final Controller controller;

    CtrlApiCallHandler(Controller controllerRef)
    {
        controller = controllerRef;
        nodeApiCallHandler = new CtrlNodeApiCallHandler(controllerRef);
        resourceApiCallHandler = new CtrlResourceApiCallHandler(controllerRef);
    }

    public ApiCallRc createNode(
        AccessContext accCtx,
        Peer client,
        String nodeNameStr,
        String nodeTypeStr,
        Map<String, String> props
    )
    {
        controller.nodesMapLock.writeLock().lock();
        ApiCallRc apiCallRc = nodeApiCallHandler.createNode(
            accCtx,
            client,
            nodeNameStr,
            nodeTypeStr,
            props
        );
        controller.nodesMapLock.writeLock().unlock();

        return apiCallRc;
    }

    public ApiCallRc deleteNode(AccessContext accCtx, Peer client, String nodeName)
    {
        controller.nodesMapLock.writeLock().lock();
        ApiCallRc apiCallRc = nodeApiCallHandler.deleteNode(accCtx, client, nodeName);
        controller.nodesMapLock.writeLock().unlock();

        return apiCallRc;
    }

    public ApiCallRc createResourceDefinition(
        AccessContext accCtx,
        Peer client,
        String resourceName,
        Map<String, String> props,
        List<VolumeDefinition.VlmDfnApi> volDescrMap
    )
    {
        controller.rscDfnMapLock.writeLock().lock();
        ApiCallRc apiCallRc = resourceApiCallHandler.createResourceDefinition(
            accCtx,
            client,
            resourceName,
            props,
            volDescrMap
        );
        controller.rscDfnMapLock.writeLock().unlock();
        return apiCallRc;
    }

    public ApiCallRc deleteResourceDefinition(
        AccessContext accCtx,
        Peer client,
        String resourceName
    )
    {
        controller.rscDfnMapLock.writeLock().lock();
        ApiCallRc apiCallRc = resourceApiCallHandler.deleteResourceDefinition(
            accCtx,
            client,
            resourceName
        );
        controller.rscDfnMapLock.writeLock().unlock();
        return apiCallRc;
    }

    public ApiCallRc createResource(
        AccessContext accCtx,
        Peer client,
        String nodeName,
        String rscName,
        int nodeId,
        Map<String, String> rscPropsMap,
        List<Volume.VlmApi> vlmApiDataList
    )
    {
        controller.rscDfnMapLock.writeLock().lock();
        controller.nodesMapLock.writeLock().lock();

        ApiCallRc apiCallRc = resourceApiCallHandler.createResource(
            accCtx,
            client,
            nodeName,
            rscName,
            nodeId,
            rscPropsMap,
            vlmApiDataList
        );

        controller.rscDfnMapLock.writeLock().unlock();
        controller.nodesMapLock.writeLock().unlock();

        return apiCallRc;
    }

    public ApiCallRc deleteResource(
        AccessContext accCtx,
        Peer client,
        String nodeName,
        String rscName
    )
    {
        controller.rscDfnMapLock.writeLock().lock();
        controller.nodesMapLock.writeLock().lock();

        ApiCallRc apiCallRc = resourceApiCallHandler.deleteResource(
            accCtx,
            client,
            nodeName,
            rscName
        );

        controller.rscDfnMapLock.writeLock().unlock();
        controller.nodesMapLock.writeLock().unlock();

        return apiCallRc;
    }
}
