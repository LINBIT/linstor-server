package com.linbit.linstor.core;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.SatelliteTransactionMgr;
import com.linbit.linstor.Node;
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

    public StltStorPoolApiCallHandler(Satellite satellite, AccessContext apiCtx)
    {
        this.satellite = satellite;
        this.apiCtx = apiCtx;
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
//          checkUuid(satellite.localNode, storPoolRaw);

        storPoolName = new StorPoolName(storPoolRaw.getStorPoolName());
        StorPool storPool = satellite.localNode.getStorPool(apiCtx, storPoolName);

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
                satellite.localNode,
                storPoolDfn,
                storPoolRaw.getDriver(),
                transMgr,
                satellite
            );
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
