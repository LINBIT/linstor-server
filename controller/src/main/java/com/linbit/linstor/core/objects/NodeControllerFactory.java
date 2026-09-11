package com.linbit.linstor.core.objects;

import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.repository.NodeRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.NodeDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.stateflags.StateFlagsBits;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;

import java.util.UUID;

@Singleton
public class NodeControllerFactory
{
    private final ErrorReporter errorReporter;
    private final NodeDatabaseDriver dbDriver;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final NodeRepository nodeRepository;
    private final ReadOnlyProps ctrlConf;

    @Inject
    public NodeControllerFactory(
        ErrorReporter errorReporterRef,
        NodeDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        NodeRepository nodeRepositoryRef,
        @Named(LinStor.CONTROLLER_PROPS) ReadOnlyProps ctrlConfRef
    )
    {
        errorReporter = errorReporterRef;
        dbDriver = dbDriverRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
        nodeRepository = nodeRepositoryRef;
        ctrlConf = ctrlConfRef;
    }

    public Node create(
        NodeName nameRef,
        @Nullable Node.Type type,
        @Nullable Node.Flags[] flags
    )
        throws DatabaseException, LinStorDataAlreadyExistsException
    {
        Node node = nodeRepository.get(nameRef);

        if (node != null)
        {
            throw new LinStorDataAlreadyExistsException("The Node already exists");
        }

        node = new Node(
            UUID.randomUUID(),
            nameRef,
            type,
            StateFlagsBits.getMask(flags),
            ctrlConf,
            errorReporter,
            dbDriver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            false
        );
        dbDriver.create(node);

        return node;
    }
}
