package com.linbit.linstor.core;

import com.linbit.linstor.NodeDataControllerFactory;
import com.linbit.linstor.ResourceDataFactory;
import com.linbit.linstor.ResourceDefinitionDataControllerFactory;
import com.linbit.linstor.SnapshotDefinitionDataControllerFactory;
import com.linbit.linstor.StorPoolDataFactory;
import com.linbit.linstor.StorPoolDefinitionDataControllerFactory;
import com.linbit.linstor.core.apicallhandler.AbsApiCallHandler;

import javax.inject.Inject;

/**
 * Holds the object factories required by {@link AbsApiCallHandler}.
 */
// This class is a crutch to enable injection of these factories until
// AbsApiCallHandler is split up.
public class CtrlObjectFactories
{
    private final NodeDataControllerFactory nodeDataFactory;
    private final ResourceDefinitionDataControllerFactory resourceDefinitionDataFactory;
    private final ResourceDataFactory resourceDataFactory;
    private final SnapshotDefinitionDataControllerFactory snapshotDefinitionDataFactory;
    private final StorPoolDefinitionDataControllerFactory storPoolDefinitionDataFactory;
    private final StorPoolDataFactory storPoolDataFactory;

    @Inject
    public CtrlObjectFactories(
        NodeDataControllerFactory nodeDataFactoryRef,
        ResourceDefinitionDataControllerFactory resourceDefinitionDataFactoryRef,
        ResourceDataFactory resourceDataFactoryRef,
        SnapshotDefinitionDataControllerFactory snapshotDefinitionDataFactoryRef,
        StorPoolDefinitionDataControllerFactory storPoolDefinitionDataFactoryRef,
        StorPoolDataFactory storPoolDataFactoryRef
    )
    {
        nodeDataFactory = nodeDataFactoryRef;
        resourceDefinitionDataFactory = resourceDefinitionDataFactoryRef;
        resourceDataFactory = resourceDataFactoryRef;
        snapshotDefinitionDataFactory = snapshotDefinitionDataFactoryRef;
        storPoolDefinitionDataFactory = storPoolDefinitionDataFactoryRef;
        storPoolDataFactory = storPoolDataFactoryRef;
    }

    public NodeDataControllerFactory getNodeDataFactory()
    {
        return nodeDataFactory;
    }

    public ResourceDefinitionDataControllerFactory getResourceDefinitionDataFactory()
    {
        return resourceDefinitionDataFactory;
    }

    public ResourceDataFactory getResourceDataFactory()
    {
        return resourceDataFactory;
    }

    public SnapshotDefinitionDataControllerFactory getSnapshotDefinitionDataFactory()
    {
        return snapshotDefinitionDataFactory;
    }

    public StorPoolDefinitionDataControllerFactory getStorPoolDefinitionDataFactory()
    {
        return storPoolDefinitionDataFactory;
    }

    public StorPoolDataFactory getStorPoolDataFactory()
    {
        return storPoolDataFactory;
    }
}
