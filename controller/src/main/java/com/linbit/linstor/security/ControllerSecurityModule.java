package com.linbit.linstor.security;

import com.linbit.ImplementationError;
import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.InitializationException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.PublicContext;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.cfg.CtrlConfig;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.modularcrypto.ModularCryptoProvider;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;

public class ControllerSecurityModule extends AbstractModule
{
    @Override
    protected void configure()
    {
    }

    @Provides
    public <T extends ControllerDatabase> SecurityLevelSetter securityLevelSetter(
        final ControllerDatabase dbConnectionPool,
        final DbAccessor<? extends ControllerDatabase> securityDbDriver
    )
    {
        return (accCtx, newLevel) ->
        SecurityLevel.set(accCtx, newLevel, (T) dbConnectionPool, (DbAccessor<T>) securityDbDriver);
    }

    @Provides
    public <T extends ControllerDatabase> MandatoryAuthSetter mandatoryAuthSetter(
        final ControllerDatabase dbConnectionPool,
        final DbAccessor<? extends ControllerDatabase> securityDbDriver
    )
    {
        return (accCtx, newPolicy) ->
        Authentication.setRequired(accCtx, newPolicy, (T) dbConnectionPool, (DbAccessor<T>) securityDbDriver);
    }

    @SuppressWarnings("unchecked")
    @Provides
    @Singleton
    public CtrlAuthentication<ControllerDatabase> initializeAuthentication(
        @SystemContext AccessContext initCtx,
        @PublicContext AccessContext publicCtx,
        ErrorReporter errorLogRef,
        ControllerDatabase dbConnPool,
        DbAccessor<? extends ControllerDatabase> dbAccessor,
        ModularCryptoProvider cryptoProvider,
        CtrlConfig ctrlConfRef
    )
        throws InitializationException
    {
        errorLogRef.logInfo("Initializing authentication subsystem");

        CtrlAuthentication<ControllerDatabase> authentication;
        try
        {
            authentication = new CtrlAuthentication<>(
                initCtx,
                initCtx,
                publicCtx,
                dbConnPool,
                (DbAccessor<ControllerDatabase>) dbAccessor,
                cryptoProvider,
                errorLogRef,
                ctrlConfRef
            );
        }
        catch (AccessDeniedException accExc)
        {
            throw new ImplementationError(
                "The initialization security context does not have the necessary " +
                    "privileges to create the authentication subsystem",
                accExc
            );
        }
        catch (LinStorException exc)
        {
            throw new InitializationException(exc);
        }
        return authentication;
    }

    @Provides
    @Singleton
    public Authorization initializeAuthorization(
        @SystemContext AccessContext initCtx,
        ErrorReporter errorLogRef,
        ControllerDatabase dbConnPool,
        DbAccessor<? extends ControllerDatabase> securityDbDriver
    )
    {
        errorLogRef.logInfo("Initializing authorization subsystem");

        Authorization authorization;
        try
        {
            authorization = new Authorization(initCtx, dbConnPool, securityDbDriver);
        }
        catch (AccessDeniedException accExc)
        {
            throw new ImplementationError(
                "The initialization security context does not have the necessary " +
                    "privileges to create the authorization subsystem",
                accExc
            );
        }
        return authorization;
    }
}
