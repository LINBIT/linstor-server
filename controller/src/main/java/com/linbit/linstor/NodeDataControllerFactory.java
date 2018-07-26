package com.linbit.linstor;

import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.dbdrivers.ControllerDbModule;
import com.linbit.linstor.dbdrivers.interfaces.NodeDataDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ControllerSecurityModule;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.stateflags.StateFlagsBits;
import com.linbit.linstor.storage.DisklessDriver;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;

import java.sql.SQLException;
import java.util.UUID;

public class NodeDataControllerFactory
{
    private final NodeDataDatabaseDriver dbDriver;
    private final ObjectProtectionFactory objectProtectionFactory;
    private final StorPoolDataFactory storPoolDataFactory;
    private final StorPoolDefinition disklessStorPoolDfn;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final CoreModule.NodesMap nodesMap;
    private final ObjectProtection nodesMapObjProt;

    @Inject
    public NodeDataControllerFactory(
        NodeDataDatabaseDriver dbDriverRef,
        ObjectProtectionFactory objectProtectionFactoryRef,
        StorPoolDataFactory storPoolDataFactoryRef,
        @Named(ControllerDbModule.DISKLESS_STOR_POOL_DFN) StorPoolDefinition disklessStorPoolDfnRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        CoreModule.NodesMap nodesMapRef,
        @Named(ControllerSecurityModule.NODES_MAP_PROT) ObjectProtection nodesMapProtRef
    )
    {
        dbDriver = dbDriverRef;
        objectProtectionFactory = objectProtectionFactoryRef;
        storPoolDataFactory = storPoolDataFactoryRef;
        disklessStorPoolDfn = disklessStorPoolDfnRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
        nodesMap = nodesMapRef;
        nodesMapObjProt = nodesMapProtRef;
    }

    public NodeData getInstance(
        AccessContext accCtx,
        NodeName nameRef
    )
        throws AccessDeniedException
    {
        nodesMapObjProt.requireAccess(accCtx, AccessType.VIEW);
        return (NodeData) nodesMap.get(nameRef);
    }

    public NodeData create(
        AccessContext accCtx,
        NodeName nameRef,
        Node.NodeType type,
        Node.NodeFlag[] flags
    )
        throws SQLException, AccessDeniedException, LinStorDataAlreadyExistsException
    {
        NodeData nodeData = getInstance(accCtx, nameRef);

        if (nodeData != null)
        {
            throw new LinStorDataAlreadyExistsException("The Node already exists");
        }

        nodeData = new NodeData(
            UUID.randomUUID(),
            objectProtectionFactory.getInstance(
                accCtx,
                ObjectProtection.buildPath(nameRef),
                true
            ),
            nameRef,
            type,
            StateFlagsBits.getMask(flags),
            dbDriver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider
        );
        dbDriver.create(nodeData);

        nodeData.setDisklessStorPool(
            storPoolDataFactory.create(
                accCtx,
                nodeData,
                disklessStorPoolDfn,
                DisklessDriver.class.getSimpleName()
            )
        );

        return nodeData;
    }
}
