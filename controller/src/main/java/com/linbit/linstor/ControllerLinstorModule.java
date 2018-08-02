package com.linbit.linstor;

import com.google.inject.AbstractModule;

public class ControllerLinstorModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        bind(NodeRepository.class).to(NodeProtectionRepository.class);
        bind(ResourceDefinitionRepository.class).to(ResourceDefinitionProtectionRepository.class);
        bind(StorPoolDefinitionRepository.class).to(StorPoolDefinitionProtectionRepository.class);
        bind(SystemConfRepository.class).to(SystemConfProtectionRepository.class);
    }
}
