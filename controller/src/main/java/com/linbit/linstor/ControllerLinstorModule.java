package com.linbit.linstor;

import com.linbit.linstor.core.objects.FreeSpaceMgrProtectionRepository;
import com.linbit.linstor.core.objects.FreeSpaceMgrRepository;
import com.linbit.linstor.core.objects.KeyValueStoreProtectionRepository;
import com.linbit.linstor.core.objects.KeyValueStoreRepository;
import com.linbit.linstor.core.objects.NodeProtectionRepository;
import com.linbit.linstor.core.objects.NodeRepository;
import com.linbit.linstor.core.objects.ResourceDefinitionProtectionRepository;
import com.linbit.linstor.core.objects.ResourceDefinitionRepository;
import com.linbit.linstor.core.objects.StorPoolDefinitionProtectionRepository;
import com.linbit.linstor.core.objects.StorPoolDefinitionRepository;
import com.linbit.linstor.core.objects.SystemConfProtectionRepository;
import com.linbit.linstor.core.objects.SystemConfRepository;

import com.google.inject.AbstractModule;

public class ControllerLinstorModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        bind(NodeRepository.class).to(NodeProtectionRepository.class);
        bind(ResourceDefinitionRepository.class).to(ResourceDefinitionProtectionRepository.class);
        bind(StorPoolDefinitionRepository.class).to(StorPoolDefinitionProtectionRepository.class);
        bind(FreeSpaceMgrRepository.class).to(FreeSpaceMgrProtectionRepository.class);
        bind(SystemConfRepository.class).to(SystemConfProtectionRepository.class);
        bind(KeyValueStoreRepository.class).to(KeyValueStoreProtectionRepository.class);
    }
}
