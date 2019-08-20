package com.linbit.linstor.security;

import com.linbit.NoOpObjectDatabaseDriver;
import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.data.IdentityRoleEntry;
import com.linbit.linstor.security.data.SignInEntry;
import com.linbit.linstor.security.data.TypeEnforcementRule;
import com.linbit.linstor.transaction.SatelliteTransactionMgr;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionMgrSQL;
import com.linbit.linstor.transaction.TransactionMgrUtil;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.List;

@Singleton
public class EmptySecurityDbDriver implements DbAccessor
{
    public static final SingleColumnDatabaseDriver<?, ?> NOOP_COL_DRIVER =
        new NoOpObjectDatabaseDriver<>();

    @Inject
    public EmptySecurityDbDriver()
    {
    }

    @Override
    public SignInEntry getSignInEntry(ControllerDatabase ctrlDb, IdentityName idName)
    {
        return null;
    }

    @Override
    public IdentityRoleEntry getIdRoleMapEntry(ControllerDatabase ctrlDb, IdentityName idName, RoleName rlName)
    {
        return null;
    }

    @Override
    public IdentityRoleEntry getDefaultRole(ControllerDatabase ctrlDb, IdentityName idName)
    {
        return null;
    }

    @Override
    public List<String> loadIdentities(ControllerDatabase ctrlDb)
    {
        return null;
    }

    @Override
    public List<String> loadSecurityTypes(ControllerDatabase ctrlDb)
    {
        return null;
    }

    @Override
    public List<String> loadRoles(ControllerDatabase ctrlDb)
    {
        return null;
    }

    @Override
    public List<TypeEnforcementRule> loadTeRules(ControllerDatabase ctrlDb)
    {
        return null;
    }

    @Override
    public String loadSecurityLevel(ControllerDatabase ctrlDb)
    {
        return null;
    }

    @Override
    public boolean loadAuthRequired(ControllerDatabase ctrlDb)
    {
        return true;
    }

    @Override
    public void setSecurityLevel(ControllerDatabase ctrlDb, SecurityLevel newLevel)
    {
        // no-op
    }

    @Override
    public void setAuthRequired(ControllerDatabase ctrlDb, boolean newPolicy)
    {
        // no-op
    }

    @Singleton
    public static class EmptyObjectProtectionDatabaseDriver implements ObjectProtectionDatabaseDriver
    {
        private final ObjectProtection objProt;

        @Inject
        EmptyObjectProtectionDatabaseDriver(
            @SystemContext AccessContext accCtx,
            Provider<TransactionMgr> transMgrProviderRef,
            TransactionObjectFactory transObjFactoryRef,
            LinStorScope initScope
        )
            throws AccessDeniedException, DatabaseException
        {
            // Create a dummy singleton object protection instance which is used everywhere in the satellite
            objProt = new ObjectProtection(accCtx, "", this, transObjFactoryRef, transMgrProviderRef);

            SatelliteTransactionMgr transMgr = new SatelliteTransactionMgr();
            initScope.enter();
            TransactionMgrUtil.seedTransactionMgr(initScope, transMgr);

            objProt.addAclEntry(accCtx, accCtx.getRole(), AccessType.CONTROL);

            transMgr.commit();
            initScope.exit();
        }

        @Override
        public void insertOp(ObjectProtection objProtRef) throws DatabaseException
        {
            // no-op
        }

        @Override
        public void deleteOp(String objPath) throws DatabaseException
        {
            // no-op
        }

        @Override
        public void insertAcl(ObjectProtection objProtRef, Role role, AccessType grantedAccess)
            throws DatabaseException
        {
            // no-op
        }

        @Override
        public void updateAcl(ObjectProtection objProtRef, Role role, AccessType grantedAccess)
            throws DatabaseException
        {
            // no-op
        }

        @Override
        public void deleteAcl(ObjectProtection objProtRef, Role role) throws DatabaseException
        {
            // no-op
        }

        @Override
        public ObjectProtection loadObjectProtection(String objPath, boolean logWarnIfNotExists)
            throws DatabaseException
        {
            return objProt;
        }

        @SuppressWarnings("unchecked")
        @Override
        public SingleColumnDatabaseDriver<ObjectProtection, Identity> getIdentityDatabaseDrier()
        {
            // no-op
            return (SingleColumnDatabaseDriver<ObjectProtection, Identity>) NOOP_COL_DRIVER;
        }

        @SuppressWarnings("unchecked")
        @Override
        public SingleColumnDatabaseDriver<ObjectProtection, Role> getRoleDatabaseDriver()
        {
            // no-op
            return (SingleColumnDatabaseDriver<ObjectProtection, Role>) NOOP_COL_DRIVER;
        }

        @SuppressWarnings("unchecked")
        @Override
        public SingleColumnDatabaseDriver<ObjectProtection, SecurityType> getSecurityTypeDriver()
        {
            // no-op
            return (SingleColumnDatabaseDriver<ObjectProtection, SecurityType>) NOOP_COL_DRIVER;
        }
    }
}
