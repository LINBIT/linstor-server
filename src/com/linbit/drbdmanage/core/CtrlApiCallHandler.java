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
    private final CtrlRscDfnApiCallHandler rscDfnApiCallHandler;
    private final CtrlRscApiCallHandler rscApiCallHandler;
    private final CtrlStorPoolDfnApiCallHandler storPoolDfnApiCallHandler;
    private final CtrlStorPoolApiCallHandler storPoolApiCallHandler;
    private final CtrlNodeConnectionApiCallHandler nodeConnApiCallHandler;
    private final CtrlRscConnectionApiCallHandler rscConnApiCallHandler;
    private final CtrlVlmConnectionApiCallHandler vlmConnApiCallHandler;


    private final Controller controller;

    CtrlApiCallHandler(Controller controllerRef)
    {
        controller = controllerRef;
        nodeApiCallHandler = new CtrlNodeApiCallHandler(controllerRef);
        rscApiCallHandler = new CtrlRscApiCallHandler(controllerRef);
        rscDfnApiCallHandler = new CtrlRscDfnApiCallHandler(controllerRef);
        storPoolDfnApiCallHandler = new CtrlStorPoolDfnApiCallHandler(controllerRef);
        storPoolApiCallHandler = new CtrlStorPoolApiCallHandler(controllerRef);
        nodeConnApiCallHandler = new CtrlNodeConnectionApiCallHandler(controllerRef);
        rscConnApiCallHandler = new CtrlRscConnectionApiCallHandler(controllerRef);
        vlmConnApiCallHandler = new CtrlVlmConnectionApiCallHandler(controllerRef);
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
        ApiCallRc apiCallRc = rscDfnApiCallHandler.createResourceDefinition(
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
        ApiCallRc apiCallRc = rscDfnApiCallHandler.deleteResourceDefinition(
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

        ApiCallRc apiCallRc = rscApiCallHandler.createResource(
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

        ApiCallRc apiCallRc = rscApiCallHandler.deleteResource(
            accCtx,
            client,
            nodeName,
            rscName
        );

        controller.rscDfnMapLock.writeLock().unlock();
        controller.nodesMapLock.writeLock().unlock();

        return apiCallRc;
    }

    public ApiCallRc createStoragePoolDefinition(
        AccessContext accCtx,
        Peer client,
        String storPoolName,
        Map<String, String> storPoolDfnPropsMap
    )
    {
        controller.storPoolDfnMapLock.writeLock().lock();
        ApiCallRc apiCallRc = storPoolDfnApiCallHandler.createStorPoolDfn(
            accCtx,
            client,
            storPoolName,
            storPoolDfnPropsMap
        );
        controller.storPoolDfnMapLock.writeLock().unlock();
        return apiCallRc;
    }

    public ApiCallRc deleteStoragePoolDefinition(
        AccessContext accCtx,
        Peer client,
        String storPoolName
    )
    {
        controller.storPoolDfnMapLock.writeLock().lock();
        ApiCallRc apiCallRc = storPoolDfnApiCallHandler.deleteStorPoolDfn(
            accCtx,
            client,
            storPoolName
        );
        controller.storPoolDfnMapLock.writeLock().unlock();
        return apiCallRc;
    }

    public ApiCallRc createStoragePool(
        AccessContext accCtx,
        Peer client,
        String nodeName,
        String storPoolName,
        String driver,
        Map<String, String> storPoolPropsMap
    )
    {
        controller.nodesMapLock.writeLock().lock();
        controller.storPoolDfnMapLock.writeLock().lock();

        ApiCallRc apiCallRc = storPoolApiCallHandler.createStorPool(
            accCtx,
            client,
            nodeName,
            storPoolName,
            driver,
            storPoolPropsMap
        );

        controller.storPoolDfnMapLock.writeLock().unlock();
        controller.nodesMapLock.writeLock().unlock();

        return apiCallRc;
    }

    public ApiCallRc deleteStoragePool(
        AccessContext accCtx,
        Peer client,
        String nodeName,
        String storPoolName
    )
    {
        controller.nodesMapLock.writeLock().lock();
        controller.storPoolDfnMapLock.writeLock().lock();

        ApiCallRc apiCallRc = storPoolApiCallHandler.deleteStorPool(
            accCtx,
            client,
            nodeName,
            storPoolName
        );

        controller.storPoolDfnMapLock.writeLock().unlock();
        controller.nodesMapLock.writeLock().unlock();

        return apiCallRc;
    }

    public ApiCallRc createNodeConnection(
        AccessContext accCtx,
        Peer client,
        String nodeName1,
        String nodeName2,
        Map<String, String> nodeConnPropsMap
    )
    {
        controller.nodesMapLock.writeLock().lock();
        ApiCallRc apiCallRc = nodeConnApiCallHandler.createNodeConnection(
            accCtx,
            client,
            nodeName1,
            nodeName2,
            nodeConnPropsMap
        );
        controller.nodesMapLock.writeLock().unlock();
        return apiCallRc;
    }

    public ApiCallRc deleteNodeConnection(
        AccessContext accCtx,
        Peer client,
        String nodeName1,
        String nodeName2
    )
    {
        controller.nodesMapLock.writeLock().lock();
        ApiCallRc apiCallRc = nodeConnApiCallHandler.deleteNodeConnection(
            accCtx,
            client,
            nodeName1,
            nodeName2
        );
        controller.nodesMapLock.writeLock().unlock();
        return apiCallRc;
    }

    public ApiCallRc createResourceConnection(
        AccessContext accCtx,
        Peer client,
        String nodeName1,
        String nodeName2,
        String rscName,
        Map<String, String> rscConnPropsMap
    )
    {
        controller.nodesMapLock.writeLock().lock();
        controller.rscDfnMapLock.writeLock().lock();
        ApiCallRc apiCallRc = rscConnApiCallHandler.createResourceConnection(
            accCtx,
            client,
            nodeName1,
            nodeName2,
            rscName,
            rscConnPropsMap
        );
        controller.rscDfnMapLock.writeLock().unlock();
        controller.nodesMapLock.writeLock().unlock();
        return apiCallRc;
    }

    public ApiCallRc deleteResourceConnection(
        AccessContext accCtx,
        Peer client,
        String nodeName1,
        String nodeName2,
        String rscName
    )
    {
        controller.nodesMapLock.writeLock().lock();
        controller.rscDfnMapLock.writeLock().lock();
        ApiCallRc apiCallRc = rscConnApiCallHandler.deleteResourceConnection(
            accCtx,
            client,
            nodeName1,
            nodeName2,
            rscName
        );
        controller.rscDfnMapLock.writeLock().unlock();
        controller.nodesMapLock.writeLock().unlock();
        return apiCallRc;
    }

    public ApiCallRc createVolumeConnection(
        AccessContext accCtx,
        Peer client,
        String nodeName1,
        String nodeName2,
        String rscName,
        int vlmNr,
        Map<String, String> vlmConnPropsMap
    )
    {
        controller.nodesMapLock.writeLock().lock();
        controller.rscDfnMapLock.writeLock().lock();
        ApiCallRc apiCallRc = vlmConnApiCallHandler.createVolumeConnection(
            accCtx,
            client,
            nodeName1,
            nodeName2,
            rscName,
            vlmNr,
            vlmConnPropsMap
        );
        controller.rscDfnMapLock.writeLock().unlock();
        controller.nodesMapLock.writeLock().unlock();
        return apiCallRc;
    }

    public ApiCallRc deleteVolumeConnection(
        AccessContext accCtx,
        Peer client,
        String nodeName1,
        String nodeName2,
        String rscName,
        int vlmNr
    )
    {
        controller.nodesMapLock.writeLock().lock();
        controller.rscDfnMapLock.writeLock().lock();
        ApiCallRc apiCallRc = vlmConnApiCallHandler.deleteVolumeConnection(
            accCtx,
            client,
            nodeName1,
            nodeName2,
            rscName,
            vlmNr
        );
        controller.rscDfnMapLock.writeLock().unlock();
        controller.nodesMapLock.writeLock().unlock();
        return apiCallRc;
    }

}
