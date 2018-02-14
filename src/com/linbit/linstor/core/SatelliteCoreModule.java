package com.linbit.linstor.core;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import com.linbit.ImplementationError;
import com.linbit.SatelliteTransactionMgr;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsContainer;

import javax.inject.Named;
import javax.inject.Singleton;
import java.sql.SQLException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SatelliteCoreModule extends AbstractModule
{
    public static final String SATELLITE_PROPS = "SatelliteProps";

    public static final String STLT_CONF_LOCK = "stltConfLock";

    private static final String DB_SATELLITE_PROPSCON_INSTANCE_NAME = "STLTCFG";

    @Override
    protected void configure()
    {
        bind(ReadWriteLock.class).annotatedWith(Names.named(STLT_CONF_LOCK))
            .toInstance(new ReentrantReadWriteLock(true));

        bind(ControllerPeerConnector.class).to(ControllerPeerConnectorImpl.class);
    }

    @Provides
    @Singleton
    @Named(SATELLITE_PROPS)
    public Props loadPropsContainer()
    {
        Props propsContainer;
        try
        {
            SatelliteTransactionMgr transMgr = new SatelliteTransactionMgr();
            propsContainer = PropsContainer.getInstance(DB_SATELLITE_PROPSCON_INSTANCE_NAME, transMgr);
            transMgr.commit();
        }
        catch (SQLException exc)
        {
            // not possible
            throw new ImplementationError(exc);
        }
        return propsContainer;
    }
}
