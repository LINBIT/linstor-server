package com.linbit.linstor.security;

import com.linbit.ImplementationError;
import com.linbit.linstor.dbdrivers.interfaces.SecObjProtAclDatabaseDriver;
import com.linbit.linstor.transaction.DummyTxMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import static com.linbit.linstor.security.AccessType.CHANGE;
import static com.linbit.linstor.security.AccessType.CONTROL;
import static com.linbit.linstor.security.AccessType.USE;
import static com.linbit.linstor.security.AccessType.VIEW;

import javax.inject.Provider;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class AccessControlListTest
{
    private static final String DUMMY_OBJ_PATH = "dummy";
    private static final SecObjProtAclDatabaseDriver DUMMY_ACL_DRIVER = new SatelliteSecObjProtAclDbDriver();

    private static final TransactionMgr DUMMY_TX_MGR = new DummyTxMgr();
    private static final Provider<TransactionMgr> TRANS_MGR_PROVIDER = () -> DUMMY_TX_MGR;
    private static final TransactionObjectFactory TRANS_OBJ_FACTORY = new TransactionObjectFactory(TRANS_MGR_PROVIDER);

    private AccessContext sysCtx;
    private AccessContext rootCtx;

    private Identity userId;
    private Role userRole;
    private SecurityType userSecDomain;

    @Before
    public void setUp() throws Exception
    {
        sysCtx = new AccessContext(
            new Identity(new IdentityName("SYSTEM")),
            new Role(new RoleName("SYSTEM")),
            new SecurityType(new SecTypeName("SYSTEM")),
            new PrivilegeSet(Privilege.PRIV_SYS_ALL)
        );
        rootCtx = sysCtx.clone();
        rootCtx.privEffective.enablePrivileges(Privilege.PRIVILEGE_LIST);

        userId = new Identity(new IdentityName("User"));

        userRole = new Role(new RoleName("UserRole"));

        userSecDomain = new SecurityType(new SecTypeName("UserSecType"));

        SecurityLevel.set(rootCtx, SecurityLevel.MAC, null, null);
    }

    @Test
    public void testRequireAccess() throws AccessDeniedException
    {
        AclIterator aclIt = new AclIterator(true);
        for (AclIteration iteration : aclIt)
        {
            AccessControlList acl = iteration.acl;
            AccessContext accCtx = iteration.accCtx;
            AccessType grantedAt = iteration.grantedAt;
            AccessType requestedAt = iteration.requestedAt;

            boolean expectException = !SecurityLevel.get().equals(SecurityLevel.NO_SECURITY) &&
                (grantedAt == null || !grantedAt.hasAccess(requestedAt));
            switch (requestedAt)
            {
                case VIEW:
                    expectException &= !accCtx.privEffective.hasSomePrivilege(
                        Privilege.PRIV_OBJ_VIEW,
                        Privilege.PRIV_OBJ_USE,
                        Privilege.PRIV_OBJ_CHANGE,
                        Privilege.PRIV_OBJ_CONTROL,
                        Privilege.PRIV_OBJ_OWNER
                    );
                    break;
                case USE:
                    expectException &= !accCtx.privEffective.hasSomePrivilege(
                        Privilege.PRIV_OBJ_USE,
                        Privilege.PRIV_OBJ_CHANGE,
                        Privilege.PRIV_OBJ_CONTROL,
                        Privilege.PRIV_OBJ_OWNER
                    );
                    break;
                case CHANGE:
                    expectException &= !accCtx.privEffective.hasSomePrivilege(
                        Privilege.PRIV_OBJ_CHANGE,
                        Privilege.PRIV_OBJ_CONTROL,
                        Privilege.PRIV_OBJ_OWNER
                    );
                    break;
                case CONTROL:
                    expectException &= !accCtx.privEffective.hasSomePrivilege(
                        Privilege.PRIV_OBJ_CONTROL,
                        Privilege.PRIV_OBJ_OWNER
                    );
                    expectException &= !accCtx.privEffective.hasPrivileges(Privilege.PRIV_OBJ_CONTROL);
                    break;
                default:
                    throw new ImplementationError(
                        "AccessType-enum has been extended without extending this switch",
                        null
                    );
            }

            if (expectException)
            {
                try
                {
                    acl.requireAccess(accCtx, requestedAt);
                    fail("Exception expected");
                }
                catch (AccessDeniedException expected)
                {
                    // expected
                }
            }
            else
            {
                acl.requireAccess(accCtx, requestedAt);
            }
        }
    }

    @Test
    public void testQueryAccess()
    {
        AclIterator aclIt = new AclIterator(true);
        for (AclIteration iteration : aclIt)
        {
            AccessControlList acl = iteration.acl;
            AccessContext accCtx = iteration.accCtx;
            AccessType grantedAt = iteration.grantedAt;

            AccessType privAccess = accCtx.privEffective.toRbacAccess();

            AccessType expectedAt;
            if (SecurityLevel.get().equals(SecurityLevel.NO_SECURITY))
            {
                expectedAt = AccessType.CONTROL;
            }
            else
            {
                expectedAt = AccessType.union(privAccess, grantedAt);
            }

            AccessType actualAt = acl.queryAccess(accCtx);

            assertEquals(expectedAt, actualAt);
        }
    }

    @Test
    public void testGetEntry()
    {
        AclIterator aclIt = new AclIterator(true);
        for (AclIteration iteration : aclIt)
        {
            AccessControlList acl = iteration.acl;
            AccessContext accCtx = iteration.accCtx;
            Role role = iteration.role;
            AccessType grantedAt = iteration.grantedAt;

            assertEquals(grantedAt, acl.getEntry(accCtx));
            assertEquals(grantedAt, acl.getEntry(role));
        }
    }

    private class AclIteration
    {
        public AccessContext accCtx;
        public Role role;
        public AccessControlList acl;
        public AccessType grantedAt;
        public AccessType requestedAt;
    }

    private class AclIterator extends AbsSecurityIterator<AclIteration>
    {
        public static final int IDX_ACC_MAC_OVRD = 0;
        public static final int IDX_ACC_PRIV = 1;

        public static final int IDX_REQUESTED_ACCESS_TYPE = 2;
        public static final int IDX_GRANTED_ACCESS_TYPE = 3;

        AclIterator(boolean iterateSecurityLevel, int...skipColumns)
        {
            super(new Object[][]
                {
                    // AccessContext
                    {true, false},  // has PRIV_MAC_OVRD
                    {
                        0L,
                        Privilege.PRIV_OBJ_VIEW.id,     //
                        Privilege.PRIV_OBJ_USE.id,      //
                        Privilege.PRIV_OBJ_CHANGE.id,   //
                        Privilege.PRIV_OBJ_CONTROL.id,  // privileges.... :)
                        Privilege.PRIV_OBJ_OWNER.id,    //
                        Privilege.PRIV_SYS_ALL.id       //
                    },
                    {VIEW, USE, CHANGE, CONTROL},       // requested AccessType
                    {null, VIEW, USE, CHANGE, CONTROL}, // granted AccessContext
                },
                iterateSecurityLevel,
                rootCtx,
                skipColumns
            );
        }

        @Override
        protected AclIteration getNext() throws Exception
        {
            AclIteration iteration = new AclIteration();

            boolean hasPrivMacOvrd = getValue(IDX_ACC_MAC_OVRD);
            long privs = getValue(IDX_ACC_PRIV);

            if (hasPrivMacOvrd)
            {
                privs |= Privilege.PRIV_MAC_OVRD.id;
            }

            PrivilegeSet privLimit = new PrivilegeSet(privs);
            AccessContext ctx = new AccessContext(userId, userRole, userSecDomain, privLimit);
            ctx.privEffective.enablePrivileges(privLimit.toArray());

            AccessControlList acl = new AccessControlList(
                DUMMY_OBJ_PATH,
                DUMMY_ACL_DRIVER,
                TRANS_OBJ_FACTORY,
                TRANS_MGR_PROVIDER
            );

            iteration.accCtx = ctx;
            iteration.role = userRole;
            iteration.acl = acl;
            iteration.grantedAt = getValue(IDX_GRANTED_ACCESS_TYPE);
            iteration.requestedAt = getValue(IDX_REQUESTED_ACCESS_TYPE);

            if (iteration.grantedAt != null)
            {
                acl.addEntry(userRole, iteration.grantedAt);
            }

            return iteration;
        }
    }
}
