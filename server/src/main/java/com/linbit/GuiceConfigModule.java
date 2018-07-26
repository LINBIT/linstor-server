package com.linbit;

import com.google.inject.AbstractModule;

public class GuiceConfigModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        binder().requireAtInjectOnConstructors();
        binder().requireExactBindingAnnotations();
        binder().disableCircularProxies();
    }
}
