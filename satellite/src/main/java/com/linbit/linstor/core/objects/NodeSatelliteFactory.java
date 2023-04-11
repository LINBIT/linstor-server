package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.dbdrivers.interfaces.NodeDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.stateflags.StateFlagsBits;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;

import java.util.UUID;

public class NodeSatelliteFactory
{
    private final NodeDatabaseDriver dbDriver;
    private final ObjectProtectionFactory objectProtectionFactory;
    private final StorPoolSatelliteFactory storPoolFactory;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final FreeSpaceMgrSatelliteFactory freeSpaceMgrFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final CoreModule.NodesMap nodesMap;

    @Inject
    public NodeSatelliteFactory(
        NodeDatabaseDriver dbDriverRef,
        ObjectProtectionFactory objectProtectionFactoryRef,
        StorPoolSatelliteFactory storPoolFactoryRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        FreeSpaceMgrSatelliteFactory freeSpaceMgrFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        CoreModule.NodesMap nodesMapRef
    )
    {
        dbDriver = dbDriverRef;
        objectProtectionFactory = objectProtectionFactoryRef;
        storPoolFactory = storPoolFactoryRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        freeSpaceMgrFactory = freeSpaceMgrFactoryRef;
        transMgrProvider = transMgrProviderRef;
        nodesMap = nodesMapRef;
    }

    public Node getInstanceSatellite(
        AccessContext accCtx,
        UUID uuid,
        NodeName nameRef,
        Node.Type typeRef,
        Node.Flags[] flags
    )
        throws ImplementationError
    {
        Node node = null;
        try
        {
            // we should have system context anyways, so we skip the objProt check
            node = nodesMap.get(nameRef);
            if (node == null)
            {
                node = new Node(
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
                nodesMap.put(nameRef, node);
            }
        }
        catch (Exception exc)
        {
            throw new ImplementationError(
                "This method should only be called with a satellite db in background!",
                exc
            );
        }
        return node;
    }
}
