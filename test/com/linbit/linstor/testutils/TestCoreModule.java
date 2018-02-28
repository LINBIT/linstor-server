package com.linbit.linstor.testutils;

import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.linbit.linstor.annotation.Uninitialized;
import com.linbit.linstor.core.CoreModule;

public class TestCoreModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        // Core maps do not require initialization for tests
        bind(CoreModule.NodesMap.class)
            .to(Key.get(CoreModule.NodesMap.class, Uninitialized.class));
        bind(CoreModule.ResourceDefinitionMap.class)
            .to(Key.get(CoreModule.ResourceDefinitionMap.class, Uninitialized.class));
        bind(CoreModule.StorPoolDefinitionMap.class)
            .to(Key.get(CoreModule.StorPoolDefinitionMap.class, Uninitialized.class));
    }
}
