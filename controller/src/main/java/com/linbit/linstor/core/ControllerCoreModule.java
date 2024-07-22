package com.linbit.linstor.core;

import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.objects.FreeSpaceMgr;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.transaction.TransactionMap;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Map;
import java.util.TreeMap;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.name.Names;

public class ControllerCoreModule extends AbstractModule
{

    @Override
    protected void configure()
    {
        bind(String.class).annotatedWith(Names.named(CoreModule.MODULE_NAME))
            .toInstance(LinStor.CONTROLLER_MODULE);

        bind(FreeSpaceMgrMap.class).to(FreeSpaceMgrMapImpl.class);
    }

    @Provides
    @Singleton
    @Named(LinStor.CONTROLLER_PROPS)
    public Props createControllerPropsContainer(PropsContainerFactory propsContainerFactory)
    {
        return propsContainerFactory.create(
            LinStorObject.CONTROLLER.path,
            LinStorObject.CONTROLLER.toString(),
            LinStorObject.CONTROLLER
        );
    }

    @Provides
    @Singleton
    @Named(LinStor.CONTROLLER_PROPS)
    public ReadOnlyProps castCtrlPropsToReadOnlyProps(@Named(LinStor.CONTROLLER_PROPS) Props ctrlProps)
    {
        return ctrlProps;
    }

    public interface FreeSpaceMgrMap extends Map<SharedStorPoolName, FreeSpaceMgr>
    {
    }

    @Singleton
    public static class FreeSpaceMgrMapImpl
        extends TransactionMap<Void, SharedStorPoolName, FreeSpaceMgr> implements FreeSpaceMgrMap
    {
        @Inject
        public FreeSpaceMgrMapImpl(Provider<TransactionMgr> transMgrProvider)
        {
            super(null, new TreeMap<>(), null, transMgrProvider);
        }
    }
}
