package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionFactory;

public abstract class AbsProtectedDatabaseDriver<DATA extends Comparable<? super DATA>, INIT_MAPS, LOAD_ALL>
    extends AbsDatabaseDriver<DATA, INIT_MAPS, LOAD_ALL>
{
    private final ObjectProtectionFactory objProtFactory;

    protected AbsProtectedDatabaseDriver(
        AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        @Nullable DatabaseTable tableRef,
        DbEngine dbEngineRef,
        ObjectProtectionFactory objProtFactoryRef
    )
    {
        super(dbCtxRef, errorReporterRef, tableRef, dbEngineRef);

        objProtFactory = objProtFactoryRef;
    }

    protected ObjectProtection getObjectProtection(String objProtPath) throws DatabaseException
    {
        return objProtFactory.getInstance(dbCtx, objProtPath, false);
    }

}
