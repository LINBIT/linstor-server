package com.linbit.linstor.core;

import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.SatelliteTransactionMgr;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolData;
import com.linbit.linstor.StorPoolDefinition;
import com.linbit.linstor.StorPoolDefinitionData;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.api.pojo.StorPoolPojo;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

class StltStorPoolApiCallHandler
{
    private final Satellite satellite;
    private final AccessContext apiCtx;

    StltStorPoolApiCallHandler(Satellite satelliteRef, AccessContext apiCtxRef)
    {
        satellite = satelliteRef;
        apiCtx = apiCtxRef;
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

            StorPoolDefinition removedStorPoolDfn = satellite.storPoolDfnMap.remove(storPoolName); // just to be sure
            if (removedStorPoolDfn != null)
            {
                SatelliteTransactionMgr transMgr = new SatelliteTransactionMgr();
                removedStorPoolDfn.setConnection(transMgr);
                removedStorPoolDfn.delete(apiCtx);
                transMgr.commit();
            }

            satellite.getErrorReporter().logInfo("Storage pool definition '" + storPoolNameStr +
                "' and the corresponding storage pool was removed by Controller.");

            Set<StorPoolName> storPoolSet = new TreeSet<>();
            storPoolSet.add(storPoolName);
            satellite.getDeviceManager().storPoolUpdateApplied(storPoolSet);
        }
        catch (Exception | ImplementationError exc)
        {
            // TODO: kill connection?
            satellite.getErrorReporter().reportError(exc);
        }
    }

    public void applyChanges(StorPoolPojo storPoolRaw)
    {
        try
        {
            SatelliteTransactionMgr transMgr = new SatelliteTransactionMgr();
            StorPoolDefinition storPoolDfnToRegister = applyChanges(storPoolRaw, transMgr);

            StorPoolName storPoolName = new StorPoolName(storPoolRaw.getStorPoolName());

            transMgr.commit();


            satellite.getErrorReporter().logInfo(
                "Storage pool '%s' created.",
                storPoolName.displayValue
            );

            if (storPoolDfnToRegister != null)
            {
                satellite.storPoolDfnMap.put(
                    storPoolName,
                    storPoolDfnToRegister
                );
            }

            Set<StorPoolName> storPoolSet = new HashSet<>();
            storPoolSet.add(storPoolName);
            satellite.getDeviceManager().storPoolUpdateApplied(storPoolSet);
        }
        catch (Exception | ImplementationError exc)
        {
            satellite.getErrorReporter().reportError(exc);
        }
    }

    public StorPoolDefinition applyChanges(StorPoolPojo storPoolRaw, SatelliteTransactionMgr transMgr)
        throws DivergentDataException, InvalidNameException, AccessDeniedException
    {
        StorPoolName storPoolName;

        StorPoolDefinition storPoolDfnToRegister = null;

        // TODO: uncomment the next line once the localNode gets requested from the controller
        // checkUuid(satellite.localNode, storPoolRaw);

        storPoolName = new StorPoolName(storPoolRaw.getStorPoolName());
        NodeData localNode = satellite.getLocalNode();
        StorPool storPool;
        if (localNode == null)
        {
            throw new ImplementationError("ApplyChanges called with invalid localnode", new NullPointerException());
        }
        storPool = localNode.getStorPool(apiCtx, storPoolName);
        if (storPool != null)
        {
            checkUuid(storPool, storPoolRaw);
            checkUuid(storPool.getDefinition(apiCtx), storPoolRaw);
        }
        else
        {
            StorPoolDefinition storPoolDfn = satellite.storPoolDfnMap.get(storPoolName);
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
                satellite.getLocalNode(),
                storPoolDfn,
                storPoolRaw.getDriver(),
                transMgr,
                satellite
            );
            storPool.getProps(apiCtx).map().putAll(storPoolRaw.getStorPoolProps());
        }
        return storPoolDfnToRegister;
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
}
