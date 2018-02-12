package com.linbit.linstor.core;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;

public class ConfigModule extends AbstractModule
{
    public static final String CONFIG_PEER_COUNT = "configPeerCount";
    public static final String CONFIG_AL_SIZE = "configAlSize";
    public static final String CONFIG_AL_STRIPES = "configAlStripes";
    public static final String CONFIG_STOR_POOL_NAME = "configStorPoolName";

    public static final short DEFAULT_PEER_COUNT = 31;
    private static final long DEFAULT_AL_SIZE = 32;
    public static final int DEFAULT_AL_STRIPES = 1;
    public static final String DEFAULT_STOR_POOL_NAME = "DfltStorPool";

    @Override
    protected void configure()
    {
        bind(Short.class).annotatedWith(Names.named(CONFIG_PEER_COUNT)).toInstance(DEFAULT_PEER_COUNT);
        bind(Long.class).annotatedWith(Names.named(CONFIG_AL_SIZE)).toInstance(DEFAULT_AL_SIZE);
        bind(Integer.class).annotatedWith(Names.named(CONFIG_AL_STRIPES)).toInstance(DEFAULT_AL_STRIPES);
        bind(String.class).annotatedWith(Names.named(CONFIG_STOR_POOL_NAME)).toInstance(DEFAULT_STOR_POOL_NAME);
    }
}
