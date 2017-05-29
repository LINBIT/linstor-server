package com.linbit.drbdmanage.security;

import static com.linbit.drbdmanage.security.AccessType.CHANGE;
import static com.linbit.drbdmanage.security.AccessType.CONTROL;
import static com.linbit.drbdmanage.security.AccessType.USE;
import static com.linbit.drbdmanage.security.AccessType.VIEW;
import static com.linbit.drbdmanage.security.Privilege.*;
import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class AccessControlListTest
{
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
        rootCtx.privEffective.enablePrivileges(PRIVILEGE_LIST);

        userId = new Identity(new IdentityName("User"));

        userRole = new Role(new RoleName("UserRole"));

        userSecDomain = new SecurityType(new SecTypeName("UserSecType"));

        SecurityLevel.set(rootCtx, SecurityLevel.MAC);
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
                    expectException &= !accCtx.privEffective.hasPrivileges(PRIV_OBJ_VIEW);
                    break;
                case USE:
                    expectException &= !accCtx.privEffective.hasPrivileges(PRIV_OBJ_USE);
                    break;
                case CHANGE:
                    expectException &= !accCtx.privEffective.hasPrivileges(PRIV_OBJ_CHANGE);
                    break;
                case CONTROL:
                    expectException &= !accCtx.privEffective.hasPrivileges(PRIV_OBJ_CONTROL);
                    break;
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
                    { false, true },                            // has PRIV_MAC_OVRD
                    {
                        0L, PRIV_OBJ_VIEW.id,                   //
                        PRIV_OBJ_USE.id, PRIV_OBJ_CHANGE.id ,   // privileges.... :)
                        PRIV_OBJ_CONTROL.id, PRIV_OBJ_OWNER.id, //
                        PRIV_SYS_ALL.id                         //
                    },
                    { VIEW, USE, CHANGE, CONTROL },             // requested AccessType
                    { null, VIEW, USE, CHANGE, CONTROL },       // granted AccessContext
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
                privs |= PRIV_MAC_OVRD.id;
            }

            PrivilegeSet privLimit = new PrivilegeSet(privs);
            AccessContext ctx = new AccessContext(userId, userRole, userSecDomain, privLimit);
            ctx.privEffective.enablePrivileges(privLimit.toArray());

            AccessControlList acl = new AccessControlList();

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
