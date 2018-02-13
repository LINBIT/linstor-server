package com.linbit.linstor.core;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import com.linbit.TransactionMgr;
import com.linbit.linstor.dbcp.DbConnectionPool;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsContainer;

import javax.inject.Named;
import javax.inject.Singleton;
import java.sql.SQLException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ControllerCoreModule extends AbstractModule
{
    public static final String CONTROLLER_PROPS = "ControllerProps";

    public static final String CTRL_CONF_LOCK = "ctrlConfLock";

    private static final String DB_CONTROLLER_PROPSCON_INSTANCE_NAME = "CTRLCFG";

    @Override
    protected void configure()
    {
        bind(ReadWriteLock.class).annotatedWith(Names.named(CTRL_CONF_LOCK))
            .toInstance(new ReentrantReadWriteLock(true));
    }

    @Provides
    @Singleton
    @Named(CONTROLLER_PROPS)
    public Props loadPropsContainer(DbConnectionPool dbConnPool)
        throws SQLException
    {
        Props propsContainer;
        TransactionMgr transMgr = null;
        try
        {
            transMgr = new TransactionMgr(dbConnPool);
            propsContainer = PropsContainer.getInstance(DB_CONTROLLER_PROPSCON_INSTANCE_NAME, transMgr);
            transMgr.commit();
        }
        finally
        {
            if (transMgr != null)
            {
                dbConnPool.returnConnection(transMgr);
            }
        }
        return propsContainer;
    }
}
