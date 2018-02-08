package com.linbit.linstor.security;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.linbit.ImplementationError;
import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.InitializationException;
import com.linbit.linstor.logging.ErrorReporter;

import java.security.NoSuchAlgorithmException;

public class SecurityModule extends AbstractModule
{
    private final AccessContext initCtx;

    public SecurityModule(AccessContext initCtx)
    {
        this.initCtx = initCtx;
    }

    @Override
    protected void configure()
    {

    }

    @Provides
    @Singleton
    public DbAccessor securityDbDriver(ErrorReporter errorLogRef)
    {
        return new DbDerbyPersistence(initCtx, errorLogRef);
    }

    @Provides
    @Singleton
    public Authentication initializeAuthentication(
        ErrorReporter errorLogRef,
        ControllerDatabase dbConnPool,
        DbAccessor securityDbDriver
    )
        throws InitializationException
    {
        errorLogRef.logInfo("Initializing authentication subsystem");

        try
        {
            return new Authentication(initCtx, dbConnPool, securityDbDriver, errorLogRef);
        }
        catch (AccessDeniedException accExc)
        {
            throw new ImplementationError(
                "The initialization security context does not have the necessary " +
                    "privileges to create the authentication subsystem",
                accExc
            );
        }
        catch (NoSuchAlgorithmException algoExc)
        {
            throw new InitializationException(
                "Initialization of the authentication subsystem failed because the " +
                    "required hashing algorithm is not supported on this platform",
                algoExc
            );
        }
    }

    @Provides
    @Singleton
    public Authorization initializeAuthorization(
        ErrorReporter errorLogRef,
        ControllerDatabase dbConnPool,
        DbAccessor securityDbDriver
    )
    {
        errorLogRef.logInfo("Initializing authorization subsystem");

        try
        {
            return new Authorization(initCtx, dbConnPool, securityDbDriver);
        }
        catch (AccessDeniedException accExc)
        {
            throw new ImplementationError(
                "The initialization security context does not have the necessary " +
                    "privileges to create the authorization subsystem",
                accExc
            );
        }
    }
}
