package com.linbit.linstor.core;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.SatelliteTransactionMgr;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolData;
import com.linbit.linstor.StorPoolDefinition;
import com.linbit.linstor.StorPoolDefinitionData;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.Volume;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.pojo.StorPoolPojo;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
class StltStorPoolApiCallHandler
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final DeviceManager deviceManager;
    private final CoreModule.StorPoolDefinitionMap storPoolDfnMap;
    private final ControllerPeerConnector controllerPeerConnector;

    @Inject
    StltStorPoolApiCallHandler(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        DeviceManager deviceManagerRef,
        CoreModule.StorPoolDefinitionMap storPoolDfnMapRef,
        ControllerPeerConnector controllerPeerConnectorRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        deviceManager = deviceManagerRef;
        storPoolDfnMap = storPoolDfnMapRef;
        controllerPeerConnector = controllerPeerConnectorRef;
    }
    /**
     * We requested an update to a storPool and the controller is telling us that the requested storPool
     * does no longer exist.
     * Basically we now just mark the update as received and applied to prevent the
     * {@link DeviceManager} from waiting for the update.
     *
     * @param storPoolNameStr
     */
    public void applyDeletedStorPool(String storPoolNameStr)
    {
        try
        {
            StorPoolName storPoolName = new StorPoolName(storPoolNameStr);

            StorPoolDefinition removedStorPoolDfn = storPoolDfnMap.remove(storPoolName); // just to be sure
            if (removedStorPoolDfn != null)
            {
                SatelliteTransactionMgr transMgr = new SatelliteTransactionMgr();
                removedStorPoolDfn.setConnection(transMgr);
                removedStorPoolDfn.delete(apiCtx);
                transMgr.commit();
            }

            errorReporter.logInfo("Storage pool definition '" + storPoolNameStr +
                "' and the corresponding storage pool was removed by Controller.");

            Set<StorPoolName> storPoolSet = new TreeSet<>();
            storPoolSet.add(storPoolName);
            deviceManager.storPoolUpdateApplied(storPoolSet);
        }
        catch (Exception | ImplementationError exc)
        {
            // TODO: kill connection?
            errorReporter.reportError(exc);
        }
    }

    public void applyChanges(StorPoolPojo storPoolRaw)
    {
        try
        {
            SatelliteTransactionMgr transMgr = new SatelliteTransactionMgr();
            ChangedData changedData = applyChanges(storPoolRaw, transMgr);
//            StorPoolDefinition storPoolDfnToRegister = applyChanges(storPoolRaw, transMgr);

            StorPoolName storPoolName = new StorPoolName(storPoolRaw.getStorPoolName());

            transMgr.commit();


            errorReporter.logInfo(
                "Storage pool '%s' created.",
                storPoolName.displayValue
            );

            if (changedData.storPoolDfnToRegister != null)
            {
                storPoolDfnMap.put(
                    storPoolName,
                    changedData.storPoolDfnToRegister
                );
            }

            Set<StorPoolName> storPoolSet = new HashSet<>();
            storPoolSet.add(storPoolName);
            deviceManager.storPoolUpdateApplied(storPoolSet);
            deviceManager.getUpdateTracker().checkMultipleResources(changedData.changedResourcesMap);
        }
        catch (Exception | ImplementationError exc)
        {
            errorReporter.reportError(exc);
        }
    }

    public ChangedData applyChanges(StorPoolPojo storPoolRaw, SatelliteTransactionMgr transMgr)
        throws DivergentDataException, InvalidNameException, AccessDeniedException
    {
        StorPoolName storPoolName;

        StorPoolDefinition storPoolDfnToRegister = null;

        // TODO: uncomment the next line once the localNode gets requested from the controller
        // checkUuid(satellite.localNode, storPoolRaw);

        storPoolName = new StorPoolName(storPoolRaw.getStorPoolName());
        NodeData localNode = controllerPeerConnector.getLocalNode();
        StorPool storPool;
        if (localNode == null)
        {
            throw new ImplementationError("ApplyChanges called with invalid localnode", new NullPointerException());
        }
        storPool = localNode.getStorPool(apiCtx, storPoolName);
        Map<ResourceName, UUID> changedResourcesMap = new TreeMap<>();
        if (storPool != null)
        {
            checkUuid(storPool, storPoolRaw);
            checkUuid(storPool.getDefinition(apiCtx), storPoolRaw);

            storPool.getProps(apiCtx).map().putAll(storPoolRaw.getStorPoolProps());

            Collection<Volume> volumes = storPool.getVolumes(apiCtx);
            for (Volume vlm : volumes)
            {
                ResourceDefinition rscDfn = vlm.getResourceDefinition();
                changedResourcesMap.put(rscDfn.getName(), rscDfn.getUuid());
            }
        }
        else
        {
            StorPoolDefinition storPoolDfn = storPoolDfnMap.get(storPoolName);
            if (storPoolDfn == null)
            {
                storPoolDfn = StorPoolDefinitionData.getInstanceSatellite(
                    apiCtx,
                    storPoolRaw.getStorPoolDfnUuid(),
                    storPoolName,
                    transMgr
                );
                checkUuid(storPoolDfn, storPoolRaw);

                storPoolDfnToRegister = storPoolDfn;
            }

            storPool = StorPoolData.getInstanceSatellite(
                apiCtx,
                storPoolRaw.getStorPoolUuid(),
                controllerPeerConnector.getLocalNode(),
                storPoolDfn,
                storPoolRaw.getDriver(),
                transMgr
            );
            storPool.getProps(apiCtx).map().putAll(storPoolRaw.getStorPoolProps());
        }

        ChangedData changedData = new ChangedData(storPoolDfnToRegister, changedResourcesMap);
        return changedData;
    }

    private void checkUuid(Node node, StorPoolPojo storPoolRaw)
        throws DivergentUuidsException
    {
        checkUuid(
            node.getUuid(),
            storPoolRaw.getNodeUuid(),
            "Node",
            node.getName().displayValue,
            "(unknown)"
        );
    }

    private void checkUuid(StorPool storPool, StorPoolPojo storPoolRaw)
        throws DivergentUuidsException, AccessDeniedException
    {
        checkUuid(
            storPool.getUuid(),
            storPoolRaw.getStorPoolUuid(),
            "StorPool",
            storPool.getDefinition(apiCtx).getName().displayValue,
            storPoolRaw.getStorPoolName()
        );
    }

    private void checkUuid(StorPoolDefinition storPoolDfn, StorPoolPojo storPoolRaw)
        throws DivergentUuidsException
    {
        checkUuid(
            storPoolDfn.getUuid(),
            storPoolRaw.getStorPoolDfnUuid(),
            "StorPoolDefinition",
            storPoolDfn.getName().displayValue,
            storPoolRaw.getStorPoolName()
        );
    }

    private void checkUuid(UUID localUuid, UUID remoteUuid, String type, String localName, String remoteName)
        throws DivergentUuidsException
    {
        if (!localUuid.equals(remoteUuid))
        {
            throw new DivergentUuidsException(
                type,
                localName,
                remoteName,
                localUuid,
                remoteUuid
            );
        }
    }

    private class ChangedData
    {
        Map<ResourceName, UUID> changedResourcesMap;
        StorPoolDefinition storPoolDfnToRegister;

        ChangedData(StorPoolDefinition storPoolDfnToRegisterRef, Map<ResourceName, UUID> changedResourcesMapRef)
        {
            storPoolDfnToRegister = storPoolDfnToRegisterRef;
            changedResourcesMap = changedResourcesMapRef;
        }
    }
}
