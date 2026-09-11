package com.linbit.linstor;

import com.linbit.WorkerPool;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.repository.AuthTokenRepositoryImpl;
import com.linbit.linstor.core.repository.AuthTokenRepository;
import com.linbit.linstor.core.repository.ExternalFileRepositoryImpl;
import com.linbit.linstor.core.repository.ExternalFileRepository;
import com.linbit.linstor.core.repository.FreeSpaceMgrRepositoryImpl;
import com.linbit.linstor.core.repository.FreeSpaceMgrRepository;
import com.linbit.linstor.core.repository.KeyValueStoreRepositoryImpl;
import com.linbit.linstor.core.repository.KeyValueStoreRepository;
import com.linbit.linstor.core.repository.NodeRepositoryImpl;
import com.linbit.linstor.core.repository.NodeRepository;
import com.linbit.linstor.core.repository.RemoteRepositoryImpl;
import com.linbit.linstor.core.repository.RemoteRepository;
import com.linbit.linstor.core.repository.ResourceDefinitionRepositoryImpl;
import com.linbit.linstor.core.repository.ResourceDefinitionRepository;
import com.linbit.linstor.core.repository.ResourceGroupRepositoryImpl;
import com.linbit.linstor.core.repository.ResourceGroupRepository;
import com.linbit.linstor.core.repository.ScheduleRepositoryImpl;
import com.linbit.linstor.core.repository.ScheduleRepository;
import com.linbit.linstor.core.repository.StorPoolDefinitionRepositoryImpl;
import com.linbit.linstor.core.repository.StorPoolDefinitionRepository;
import com.linbit.linstor.core.repository.SystemConfRepositoryImpl;
import com.linbit.linstor.core.repository.SystemConfRepository;

import jakarta.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;

public class ControllerLinstorModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        bind(NodeRepository.class).to(NodeRepositoryImpl.class);
        bind(ResourceDefinitionRepository.class).to(ResourceDefinitionRepositoryImpl.class);
        bind(ResourceGroupRepository.class).to(ResourceGroupRepositoryImpl.class);
        bind(StorPoolDefinitionRepository.class).to(StorPoolDefinitionRepositoryImpl.class);
        bind(FreeSpaceMgrRepository.class).to(FreeSpaceMgrRepositoryImpl.class);
        bind(SystemConfRepository.class).to(SystemConfRepositoryImpl.class);
        bind(KeyValueStoreRepository.class).to(KeyValueStoreRepositoryImpl.class);
        bind(ExternalFileRepository.class).to(ExternalFileRepositoryImpl.class);
        bind(RemoteRepository.class).to(RemoteRepositoryImpl.class);
        bind(ScheduleRepository.class).to(ScheduleRepositoryImpl.class);
        bind(AuthTokenRepository.class).to(AuthTokenRepositoryImpl.class);
    }

    @Provides
    @Singleton
    public @Nullable WorkerPool initializeStltWorkerThreadPool()
    {
        return null;
    }
}
