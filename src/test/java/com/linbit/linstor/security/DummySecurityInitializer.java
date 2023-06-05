package com.linbit.linstor.security;

import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.SecObjProtAclDatabaseDriver;
import com.linbit.linstor.transaction.DummyTxMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Provider;

public class DummySecurityInitializer
{
    private static final String DUMMY_OBJ_PATH = "dummy";
    private static final SecObjProtAclDatabaseDriver DUMMY_ACL_DRIVER = new SatelliteSecObjProtAclDbDriver();

    private static final TransactionMgr DUMMY_TX_MGR = new DummyTxMgr();
    private static final Provider<TransactionMgr> TRANS_MGR_PROVIDER = () -> DUMMY_TX_MGR;
    private static final TransactionObjectFactory TRANS_OBJ_FACTORY = new TransactionObjectFactory(TRANS_MGR_PROVIDER);

    public static AccessContext getSystemAccessContext()
    {
        PrivilegeSet sysPrivs = new PrivilegeSet(Privilege.PRIV_SYS_ALL);

        AccessContext sysCtx = new AccessContext(
            Identity.SYSTEM_ID,
            Role.SYSTEM_ROLE,
            SecurityType.SYSTEM_TYPE,
            sysPrivs
        );
        try
        {
            sysCtx.privEffective.enablePrivileges(Privilege.PRIV_SYS_ALL);
        }
        catch (AccessDeniedException iAmNotRootExc)
        {
            throw new RuntimeException(iAmNotRootExc);
        }
        return sysCtx;
    }

    public static AccessContext getPublicAccessContext()
    {
        PrivilegeSet pubPrivs = new PrivilegeSet();

        AccessContext pubCtx = new AccessContext(
            Identity.PUBLIC_ID,
            Role.PUBLIC_ROLE,
            SecurityType.PUBLIC_TYPE,
            pubPrivs
        );
        return pubCtx;
    }

    public static ObjectProtection getDummyObjectProtection(AccessContext accCtx)
    {
        return new ObjectProtection(
            accCtx,
            DUMMY_OBJ_PATH,
            new AccessControlList(
                DUMMY_OBJ_PATH,
                DUMMY_ACL_DRIVER,
                TRANS_OBJ_FACTORY,
                TRANS_MGR_PROVIDER
            ),
            null,
            TRANS_OBJ_FACTORY,
            TRANS_MGR_PROVIDER
        );
    }

    public static void setSecurityLevel(AccessContext accCtx, SecurityLevel newLevel)
        throws AccessDeniedException, DatabaseException
    {
        SecurityLevel.set(accCtx, newLevel, null, null);
    }

    private DummySecurityInitializer()
    {
    }
}
