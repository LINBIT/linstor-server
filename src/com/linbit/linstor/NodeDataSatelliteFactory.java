package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.SatelliteTransactionMgr;
import com.linbit.linstor.dbdrivers.interfaces.NodeDataDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.stateflags.StateFlagsBits;
import com.linbit.linstor.storage.DisklessDriver;

import javax.inject.Inject;
import java.util.UUID;

public class NodeDataSatelliteFactory
{
    private final NodeDataDatabaseDriver dbDriver;
    private final ObjectProtectionFactory objectProtectionFactory;
    private final StorPoolDataFactory storPoolDataFactory;
    private final PropsContainerFactory propsContainerFactory;

    @Inject
    public NodeDataSatelliteFactory(
        NodeDataDatabaseDriver dbDriverRef,
        ObjectProtectionFactory objectProtectionFactoryRef,
        StorPoolDataFactory storPoolDataFactoryRef,
        PropsContainerFactory propsContainerFactoryRef
    )
    {
        dbDriver = dbDriverRef;
        objectProtectionFactory = objectProtectionFactoryRef;
        storPoolDataFactory = storPoolDataFactoryRef;
        propsContainerFactory = propsContainerFactoryRef;
    }

    public NodeData getInstanceSatellite(
        AccessContext accCtx,
        UUID uuid,
        NodeName nameRef,
        Node.NodeType typeRef,
        Node.NodeFlag[] flags,
        UUID disklessStorPoolUuid,
        SatelliteTransactionMgr transMgr,
        StorPoolDefinition disklessStorPoolDfn
    )
        throws ImplementationError
    {
        NodeData nodeData = null;
        try
        {
            nodeData = dbDriver.load(nameRef, false, transMgr);
            if (nodeData == null)
            {
                nodeData = new NodeData(
                    accCtx,
                    uuid,
                    objectProtectionFactory.getInstance(
                        accCtx,
                        "",
                        true,
                        transMgr
                    ),
                    nameRef,
                    typeRef,
                    StateFlagsBits.getMask(flags),
                    transMgr,
                    dbDriver,
                    propsContainerFactory
                );

                nodeData.setDisklessStorPool(storPoolDataFactory.getInstanceSatellite(
                    accCtx,
                    disklessStorPoolUuid,
                    nodeData,
                    disklessStorPoolDfn,
                    DisklessDriver.class.getSimpleName(),
                    transMgr
                ));
            }
            nodeData.initialized();
            nodeData.setConnection(transMgr);
        }
        catch (Exception exc)
        {
            throw new ImplementationError(
                "This method should only be called with a satellite db in background!",
                exc
            );
        }
        return nodeData;
    }
}
