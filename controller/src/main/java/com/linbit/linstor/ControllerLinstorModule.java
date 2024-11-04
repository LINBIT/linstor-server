package com.linbit.linstor;

import com.linbit.WorkerPool;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.repository.ExternalFileProtectionRepository;
import com.linbit.linstor.core.repository.ExternalFileRepository;
import com.linbit.linstor.core.repository.FreeSpaceMgrProtectionRepository;
import com.linbit.linstor.core.repository.FreeSpaceMgrRepository;
import com.linbit.linstor.core.repository.KeyValueStoreProtectionRepository;
import com.linbit.linstor.core.repository.KeyValueStoreRepository;
import com.linbit.linstor.core.repository.NodeProtectionRepository;
import com.linbit.linstor.core.repository.NodeRepository;
import com.linbit.linstor.core.repository.RemoteProtectionRepository;
import com.linbit.linstor.core.repository.RemoteRepository;
import com.linbit.linstor.core.repository.ResourceDefinitionProtectionRepository;
import com.linbit.linstor.core.repository.ResourceDefinitionRepository;
import com.linbit.linstor.core.repository.ResourceGroupProtectionRepository;
import com.linbit.linstor.core.repository.ResourceGroupRepository;
import com.linbit.linstor.core.repository.ScheduleProtectionRepository;
import com.linbit.linstor.core.repository.ScheduleRepository;
import com.linbit.linstor.core.repository.StorPoolDefinitionProtectionRepository;
import com.linbit.linstor.core.repository.StorPoolDefinitionRepository;
import com.linbit.linstor.core.repository.SystemConfProtectionRepository;
import com.linbit.linstor.core.repository.SystemConfRepository;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;

public class ControllerLinstorModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        bind(NodeRepository.class).to(NodeProtectionRepository.class);
        bind(ResourceDefinitionRepository.class).to(ResourceDefinitionProtectionRepository.class);
        bind(ResourceGroupRepository.class).to(ResourceGroupProtectionRepository.class);
        bind(StorPoolDefinitionRepository.class).to(StorPoolDefinitionProtectionRepository.class);
        bind(FreeSpaceMgrRepository.class).to(FreeSpaceMgrProtectionRepository.class);
        bind(SystemConfRepository.class).to(SystemConfProtectionRepository.class);
        bind(KeyValueStoreRepository.class).to(KeyValueStoreProtectionRepository.class);
        bind(ExternalFileRepository.class).to(ExternalFileProtectionRepository.class);
        bind(RemoteRepository.class).to(RemoteProtectionRepository.class);
        bind(ScheduleRepository.class).to(ScheduleProtectionRepository.class);
    }

    @Provides
    @Singleton
    public @Nullable WorkerPool initializeStltWorkerThreadPool()
    {
        return null;
    }
}
