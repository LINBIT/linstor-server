package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.dbdrivers.interfaces.NodeDataDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.stateflags.StateFlagsBits;
import com.linbit.linstor.storage.DisklessDriver;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;

import java.util.UUID;

public class NodeDataSatelliteFactory
{
    private final NodeDataDatabaseDriver dbDriver;
    private final ObjectProtectionFactory objectProtectionFactory;
    private final StorPoolDataFactory storPoolDataFactory;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final CoreModule.NodesMap nodesMap;
    private final FreeSpaceMgr disklessFreeSpaceMgr;

    @Inject
    public NodeDataSatelliteFactory(
        NodeDataDatabaseDriver dbDriverRef,
        ObjectProtectionFactory objectProtectionFactoryRef,
        StorPoolDataFactory storPoolDataFactoryRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        CoreModule.NodesMap nodesMapRef,
        @Named(LinStor.DISKLESS_FREE_SPACE_MGR_NAME) FreeSpaceMgr disklessFreeSpaceMgrRef
    )
    {
        dbDriver = dbDriverRef;
        objectProtectionFactory = objectProtectionFactoryRef;
        storPoolDataFactory = storPoolDataFactoryRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
        nodesMap = nodesMapRef;
        disklessFreeSpaceMgr = disklessFreeSpaceMgrRef;
    }

    public NodeData getInstanceSatellite(
        AccessContext accCtx,
        UUID uuid,
        NodeName nameRef,
        Node.NodeType typeRef,
        Node.NodeFlag[] flags,
        UUID disklessStorPoolUuid,
        StorPoolDefinition disklessStorPoolDfn
    )
        throws ImplementationError
    {
        NodeData nodeData = null;
        try
        {
            // we should have system context anyways, so we skip the objProt check
            nodeData = (NodeData) nodesMap.get(nameRef);
            if (nodeData == null)
            {
                nodeData = new NodeData(
                    uuid,
                    objectProtectionFactory.getInstance(
                        accCtx,
                        "",
                        true
                    ),
                    nameRef,
                    typeRef,
                    StateFlagsBits.getMask(flags),
                    dbDriver,
                    propsContainerFactory,
                    transObjFactory,
                    transMgrProvider
                );

                nodeData.setDisklessStorPool(storPoolDataFactory.getInstanceSatellite(
                    accCtx,
                    disklessStorPoolUuid,
                    nodeData,
                    disklessStorPoolDfn,
                    DisklessDriver.class.getSimpleName(),
                    disklessFreeSpaceMgr
                ));
            }
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
