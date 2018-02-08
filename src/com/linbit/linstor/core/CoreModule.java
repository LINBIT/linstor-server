package com.linbit.linstor.core;

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.StorPoolDefinition;
import com.linbit.linstor.StorPoolName;

import java.util.HashMap;
import java.util.Map;

public class CoreModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        bind(NodesMap.class).toInstance(new NodesMapImpl());
        bind(new TypeLiteral<Map<NodeName, Node>>() {}).to(NodesMap.class);

        bind(ResourceDefinitionMap.class).toInstance(new ResourceDefinitionMapImpl());
        bind(new TypeLiteral<Map<ResourceName, ResourceDefinition>>() {}).to(ResourceDefinitionMap.class);

        bind(StorPoolDefinitionMap.class).toInstance(new StorPoolDefinitionMapImpl());
        bind(new TypeLiteral<Map<StorPoolName, StorPoolDefinition>>() {}).to(StorPoolDefinitionMap.class);
    }

    public interface NodesMap extends Map<NodeName, Node>
    {
    }

    public interface ResourceDefinitionMap extends Map<ResourceName, ResourceDefinition>
    {
    }

    public interface StorPoolDefinitionMap extends Map<StorPoolName, StorPoolDefinition>
    {
    }

    private static class NodesMapImpl
        extends HashMap<NodeName, Node> implements NodesMap
    {
    }

    private static class ResourceDefinitionMapImpl
        extends HashMap<ResourceName, ResourceDefinition> implements ResourceDefinitionMap
    {
    }

    private static class StorPoolDefinitionMapImpl
        extends HashMap<StorPoolName, StorPoolDefinition> implements StorPoolDefinitionMap
    {
    }
}
