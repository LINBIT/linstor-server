package com.linbit.linstor.core;

import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.ControllerTransactionMgr;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Named;
import java.sql.SQLException;

public class DbDataInitializer
{
    private final LinStorScope initScope;
    private final ControllerDatabase dbConnPool;
    private final Props ctrlConf;
    private final Props stltConf;

    @Inject
    public DbDataInitializer(
        LinStorScope initScopeRef,
        ControllerDatabase dbConnPoolRef,
        @Named(LinStor.CONTROLLER_PROPS) Props ctrlConfRef,
        @Named(LinStor.SATELLITE_PROPS) Props stltConfRef
    )
    {
        initScope = initScopeRef;
        dbConnPool = dbConnPoolRef;
        ctrlConf = ctrlConfRef;
        stltConf = stltConfRef;
    }

    public void initialize()
        throws AccessDeniedException, SQLException
    {
        TransactionMgr transMgr = null;
        try
        {
            transMgr = new ControllerTransactionMgr(dbConnPool);
            initScope.enter();
            initScope.seed(TransactionMgr.class, transMgr);

            ctrlConf.loadAll();
            stltConf.loadAll();

            transMgr.commit();
            initScope.exit();
        }
        finally
        {
            if (transMgr != null)
            {
                transMgr.rollback();
                transMgr.returnConnection();
            }
        }
    }
}
