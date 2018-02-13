package com.linbit.linstor;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.TransactionMgr;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.dbcp.DbConnectionPool;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import javax.inject.Named;
import javax.inject.Singleton;
import java.sql.SQLException;
import java.util.concurrent.locks.ReadWriteLock;

public class LinStorModule extends AbstractModule
{
    public static final String DISKLESS_STOR_POOL_DFN = "disklessStorPoolDfn";

    @Override
    protected void configure()
    {
    }

    @Provides
    @Singleton
    @Named(DISKLESS_STOR_POOL_DFN)
    public StorPoolDefinitionData initializeDisklessStorPoolDfn(
        ErrorReporter errorLogRef,
        @SystemContext AccessContext initCtx,
        @Named(CoreModule.STOR_POOL_DFN_MAP_LOCK) ReadWriteLock storPoolDfnMapLock,
        DbConnectionPool dbConnPool,
        CoreModule.StorPoolDefinitionMap storPoolDfnMap
    )
        throws AccessDeniedException
    {
        StorPoolDefinitionData disklessStorPoolDfn = null;

        try
        {
            storPoolDfnMapLock.writeLock().lock();
            TransactionMgr transMgr = new TransactionMgr(dbConnPool);

            disklessStorPoolDfn = StorPoolDefinitionData.getInstance(
                initCtx,
                new StorPoolName(LinStor.DISKLESS_STOR_POOL_NAME),
                transMgr,
                true,
                false
            );

            transMgr.commit();

            storPoolDfnMap.put(disklessStorPoolDfn.getName(), disklessStorPoolDfn);
            dbConnPool.returnConnection(transMgr);
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw new ImplementationError(dataAlreadyExistsExc);
        }
        catch (SQLException sqlExc)
        {
            errorLogRef.reportError(sqlExc);
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw new ImplementationError(
                "Invalid name for default diskless stor pool: " + invalidNameExc.invalidName,
                invalidNameExc
            );
        }
        finally
        {
            storPoolDfnMapLock.writeLock().unlock();
        }

        return disklessStorPoolDfn;
    }
}
