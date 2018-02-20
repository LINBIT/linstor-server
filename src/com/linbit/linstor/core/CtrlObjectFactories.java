package com.linbit.linstor.core;

import com.linbit.linstor.NodeDataControllerFactory;
import com.linbit.linstor.ResourceDataFactory;
import com.linbit.linstor.ResourceDefinitionDataFactory;
import com.linbit.linstor.StorPoolDataFactory;
import com.linbit.linstor.StorPoolDefinitionDataFactory;

import javax.inject.Inject;

/**
 * Holds the object factories required by {@link AbsApiCallHandler}.
 */
// This class is a crutch to enable injection of these factories until
// AbsApiCallHandler is split up.
public class CtrlObjectFactories
{
    private final NodeDataControllerFactory nodeDataFactory;
    private final ResourceDefinitionDataFactory resourceDefinitionDataFactory;
    private final ResourceDataFactory resourceDataFactory;
    private final StorPoolDefinitionDataFactory storPoolDefinitionDataFactory;
    private final StorPoolDataFactory storPoolDataFactory;

    @Inject
    public CtrlObjectFactories(
        NodeDataControllerFactory nodeDataFactoryRef,
        ResourceDefinitionDataFactory resourceDefinitionDataFactoryRef,
        ResourceDataFactory resourceDataFactoryRef,
        StorPoolDefinitionDataFactory storPoolDefinitionDataFactoryRef,
        StorPoolDataFactory storPoolDataFactoryRef
    )
    {
        nodeDataFactory = nodeDataFactoryRef;
        resourceDefinitionDataFactory = resourceDefinitionDataFactoryRef;
        resourceDataFactory = resourceDataFactoryRef;
        storPoolDefinitionDataFactory = storPoolDefinitionDataFactoryRef;
        storPoolDataFactory = storPoolDataFactoryRef;
    }

    public NodeDataControllerFactory getNodeDataFactory()
    {
        return nodeDataFactory;
    }

    public ResourceDefinitionDataFactory getResourceDefinitionDataFactory()
    {
        return resourceDefinitionDataFactory;
    }

    public ResourceDataFactory getResourceDataFactory()
    {
        return resourceDataFactory;
    }

    public StorPoolDefinitionDataFactory getStorPoolDefinitionDataFactory()
    {
        return storPoolDefinitionDataFactory;
    }

    public StorPoolDataFactory getStorPoolDataFactory()
    {
        return storPoolDataFactory;
    }
}
