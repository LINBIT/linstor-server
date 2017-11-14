package com.linbit.drbdmanage.core;

import java.util.UUID;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.SatelliteTransactionMgr;
import com.linbit.drbdmanage.Node;
import com.linbit.drbdmanage.StorPool;
import com.linbit.drbdmanage.StorPoolData;
import com.linbit.drbdmanage.StorPoolDefinition;
import com.linbit.drbdmanage.StorPoolDefinitionData;
import com.linbit.drbdmanage.StorPoolName;
import com.linbit.drbdmanage.api.raw.StorPoolRawData;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;

public class StltStorPoolApiCallHandler
{
    private final Satellite satellite;
    private final AccessContext apiCtx;

    public StltStorPoolApiCallHandler(Satellite satellite, AccessContext apiCtx)
    {
        this.satellite = satellite;
        this.apiCtx = apiCtx;
    }

    public void deployStorPool(StorPoolRawData storPoolRaw) throws DivergentDataException
    {
        SatelliteTransactionMgr transMgr = new SatelliteTransactionMgr();
        StorPoolName storPoolName;

        StorPoolDefinition storPoolDfnToRegister = null;

        try
        {
            // TODO: uncomment the following line once the localNode gets requested from the controller
//            checkUuid(satellite.localNode, storPoolRaw);

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
                    transMgr
                );

                if (storPoolDfnToRegister != null)
                {
                    satellite.storPoolDfnMap.put(storPoolName, storPoolDfnToRegister);
                }
            }
        }
        catch (InvalidNameException invalidDataExc)
        {
            satellite.getErrorReporter().reportError(
                new ImplementationError(
                    "Controller sent invalid Data for resource deployment",
                    invalidDataExc
                )
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            satellite.getErrorReporter().reportError(
                new ImplementationError(
                    "Satellite's apiContext has not enough privileges to deploy resource",
                    accDeniedExc
                )
            );
        }

    }

    private void checkUuid(Node node, StorPoolRawData storPoolRaw)
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

    private void checkUuid(StorPool storPool, StorPoolRawData storPoolRaw)
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

    private void checkUuid(StorPoolDefinition storPoolDfn, StorPoolRawData storPoolRaw)
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
