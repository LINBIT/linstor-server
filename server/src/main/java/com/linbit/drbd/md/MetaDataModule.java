package com.linbit.drbd.md;

import com.google.inject.AbstractModule;

public class MetaDataModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        bind(MetaDataApi.class).to(MetaData.class);
    }
}
