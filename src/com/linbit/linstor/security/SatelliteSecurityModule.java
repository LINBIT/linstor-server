package com.linbit.linstor.security;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.transaction.SatelliteTransactionMgr;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Named;
import javax.inject.Singleton;
import java.sql.SQLException;

public class SatelliteSecurityModule extends AbstractModule
{
    @Override
    protected void configure()
    {
    }

    @Provides
    public SecurityLevelSetter securityLevelSetter()
    {
        return new SecurityLevelSetter()
        {
            @Override
            public void setSecurityLevel(AccessContext accCtx, SecurityLevel newLevel)
                throws AccessDeniedException, SQLException
            {
                SecurityLevel.set(
                    accCtx, newLevel, null, null
                );
            }
        };
    }

    @Provides
    @Singleton
    @Named(SecurityModule.SHUTDOWN_PROT)
    public ObjectProtection shutdownProt(
        @SystemContext AccessContext sysCtx,
        ObjectProtectionFactory objectProtectionFactory,
        LinStorScope initScope
    )
        throws AccessDeniedException
    {
        ObjectProtection shutdownProt;

        try
        {
            shutdownProt = objectProtectionFactory.getInstance(
                sysCtx,
                ObjectProtection.buildPathSatellite("shutdown"),
                true
            );
        }
        catch (SQLException sqlExc)
        {
            // cannot happen
            throw new ImplementationError(
                "Creating an ObjectProtection without TransactionManager threw an SQLException",
                sqlExc
            );
        }

        // Set CONTROL access for the SYSTEM role on shutdown
        try
        {
            SatelliteTransactionMgr transMgr = new SatelliteTransactionMgr();
            initScope.enter();
            initScope.seed(TransactionMgr.class, transMgr);

            shutdownProt.addAclEntry(sysCtx, sysCtx.getRole(), AccessType.CONTROL);

            transMgr.commit();
            initScope.exit();
        }
        catch (SQLException sqlExc)
        {
            // cannot happen
            throw new ImplementationError(
                "ObjectProtection without TransactionManager threw an SQLException",
                sqlExc
            );
        }

        return shutdownProt;
    }
}
