package com.linbit.linstor.security;

import com.linbit.InvalidNameException;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SecurityTypeTest
{
    private AccessContext sysCtx;
    private AccessContext rootCtx;

    private Identity userId;

    private Role userRole;

    private SecurityType userSecDomain;
    private SecurityType someOtherUserSecDomain;

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
        someOtherUserSecDomain = new SecurityType(new SecTypeName("SomeOtherUserSecType"));

        SecurityLevel.set(rootCtx, SecurityLevel.MAC, null, null);
    }

    @Test
    public void testAddEntry() throws AccessDeniedException
    {
        SecTypeIterator secTypeIt = new SecTypeIterator(true, SecTypeIterator.IDX_GRANTED_ACCESS_TYPE);
        for (SecTypeIteration iteration : secTypeIt)
        {
            AccessContext accCtx = iteration.accCtx;
            SecurityType sourceDomain = iteration.sourceDomain;
            SecurityType targetDomain = iteration.targetDomain;
            AccessType grantedAccess = iteration.requestingAccessType;

            boolean expectedException = !SecurityLevel.get().equals(SecurityLevel.NO_SECURITY) &&
                !accCtx.privEffective.hasPrivileges(Privilege.PRIV_SYS_ALL);

            AccessType origAccessType = sourceDomain.getRule(targetDomain);

            if (expectedException)
            {
                try
                {
                    sourceDomain.addRule(accCtx, targetDomain, grantedAccess);
                    fail("Exception expected");
                }
                catch (AccessDeniedException expected)
                {
                    // expected
                }
                assertEquals(origAccessType, sourceDomain.getRule(targetDomain));
            }
            else
            {
                sourceDomain.addRule(accCtx, targetDomain, grantedAccess);
                assertEquals(grantedAccess, sourceDomain.getRule(targetDomain));
            }
        }
    }

    @Test
    public void testDelEntry() throws AccessDeniedException
    {
        SecTypeIterator secTypeIt = new SecTypeIterator(true, SecTypeIterator.IDX_GRANTED_ACCESS_TYPE);
        for (SecTypeIteration iteration : secTypeIt)
        {
            AccessContext accCtx = iteration.accCtx;
            SecurityType sourceDomain = iteration.sourceDomain;
            SecurityType targetDomain = iteration.targetDomain;
            AccessType accessType = iteration.requestingAccessType;

            sourceDomain.addRule(rootCtx, targetDomain, accessType);

            boolean expectedException = !SecurityLevel.get().equals(SecurityLevel.NO_SECURITY) &&
                !accCtx.privEffective.hasPrivileges(Privilege.PRIV_SYS_ALL);

            if (expectedException)
            {
                try
                {
                    sourceDomain.delRule(accCtx, targetDomain);
                    fail("Exception expected");
                }
                catch (AccessDeniedException expected)
                {
                    // expected
                }
                assertEquals(accessType, sourceDomain.getRule(targetDomain));
            }
            else
            {
                sourceDomain.delRule(accCtx, targetDomain);
                assertNull(sourceDomain.getRule(targetDomain));
            }
        }
    }

    @Test
    public void testEquals() throws InvalidNameException
    {
        assertTrue(userSecDomain.equals(userSecDomain));
        assertTrue(userSecDomain.equals(new SecurityType(new SecTypeName(userSecDomain.name.displayValue))));
        assertFalse(userSecDomain.equals(someOtherUserSecDomain));
        assertFalse(userSecDomain.equals(null));
        assertFalse(userSecDomain.equals(userSecDomain.name));
        assertFalse(userSecDomain.equals(userSecDomain.name.displayValue));
        assertFalse(userSecDomain.equals(userSecDomain.name.value));
    }

    @Test
    public void testRequireAccess() throws AccessDeniedException
    {
        SecTypeIterator secTypeIt = new SecTypeIterator(true, SecTypeIterator.IDX_DOMAIN_TARGET);
        for (SecTypeIteration iteration : secTypeIt)
        {
            AccessType requestedAT = iteration.requestingAccessType;
            AccessType grantedAT = iteration.grantedAccess;

            SecurityType secType = iteration.sourceDomain;
            AccessContext accCtx  = iteration.accCtx;

            boolean ruleGrantsAccess = requestedAT != null &&
                (grantedAT != null && requestedAT.hasAccess(grantedAT));

            boolean expectException = SecurityLevel.get().equals(SecurityLevel.MAC) &&
                !ruleGrantsAccess && !accCtx.privEffective.hasPrivileges(Privilege.PRIV_MAC_OVRD);

            if (expectException)
            {
                try
                {
                    secType.requireAccess(accCtx, requestedAT);
                    fail("Exception expected");
                }
                catch (AccessDeniedException exc)
                {
                    // expected
                }
            }
            else
            {
                secType.requireAccess(accCtx, requestedAT);
            }
        }
    }

    @Test
    public void testQueryAccess()
    {
        SecTypeIterator secTypeIt = new SecTypeIterator(true, SecTypeIterator.IDX_DOMAIN_TARGET);
        for (SecTypeIteration iteration : secTypeIt)
        {
            AccessType grantedAT = iteration.grantedAccess;

            SecurityType secType = iteration.sourceDomain;
            AccessContext accCtx  = iteration.accCtx;

            AccessType expectedAT;
            if (SecurityLevel.get().equals(SecurityLevel.MAC))
            {
                expectedAT = AccessType.union(accCtx.privEffective.toMacAccess(), grantedAT);
            }
            else
            {
                expectedAT = AccessType.CONTROL;
            }

            AccessType actualAT = secType.queryAccess(accCtx);

            assertEquals(expectedAT, actualAT);
        }
    }

    @Test
    public void testGetEntry()
    {
        SecTypeIterator secTypeIt = new SecTypeIterator(true, SecTypeIterator.IDX_REQUESTED_ACCESS_TYPE);

        for (SecTypeIteration iteration : secTypeIt)
        {
            AccessType grantedAT = iteration.grantedAccess;
            AccessContext accCtx = iteration.accCtx;
            SecurityType secTypeSource = iteration.sourceDomain;
            SecurityType secTypeTarget = iteration.targetDomain;

            if (accCtx.subjectDomain == secTypeTarget)
            {
                assertEquals(grantedAT, secTypeSource.getRule(accCtx));
            }
            assertEquals(grantedAT, secTypeSource.getRule(secTypeTarget));
        }
    }


    @SuppressWarnings("unused")
    private class SecTypeIteration
    {
        public long privs;
        public AccessContext accCtx;
        public Identity accCtxId;
        public Role accCtxRole;
        public SecurityType accCtxSecDomain;
        public SecurityType sourceDomain;
        public SecurityType targetDomain;
        public AccessType requestingAccessType;
        public Object grantedAccessType;
        public AccessType grantedAccess;
    }

    private class SecTypeIterator extends AbsSecurityIterator<SecTypeIteration>
    {
        public static final int IDX_PRIVS = 0;
        public static final int IDX_PRIV_MAC_OVRD = 1;

        public static final int IDX_DOMAIN_SOURCE = 2;
        public static final int IDX_DOMAIN_TARGET = 3;

        public static final int IDX_REQUESTED_ACCESS_TYPE = 4;

        public static final int IDX_GRANTED_ACCESS_TYPE = 5;

        SecTypeIterator(boolean iterateSecLevel, int... skipColumns)
        {
            super(new Object[][]
                {
                    {
                        0L,
                        Privilege.PRIV_OBJ_VIEW.id,     //
                        Privilege.PRIV_OBJ_USE.id,      //
                        Privilege.PRIV_OBJ_CHANGE.id,   //
                        Privilege.PRIV_OBJ_CONTROL.id,  // privileges.... :)
                        Privilege.PRIV_OBJ_OWNER.id,    //
                        Privilege.PRIV_SYS_ALL.id       //
                    },
                    {true, false},                            // has PRIV_MAC_OVRD
                    {userSecDomain, someOtherUserSecDomain},  // source domain
                    {userSecDomain, someOtherUserSecDomain},  // target domain
                    {
                        AccessType.VIEW,    //
                        AccessType.USE,     //
                        AccessType.CHANGE,  // requested access types
                        AccessType.CONTROL  //
                    },
                    {
                        null,
                        AccessType.VIEW,    //
                        AccessType.USE,     //
                        AccessType.CHANGE,  // granted access types
                        AccessType.CONTROL  //
                    }
                },
                iterateSecLevel,
                rootCtx,
                skipColumns
            );
        }

        @Override
        protected SecTypeIteration getNext() throws Exception
        {
            long privLimit = getValue(IDX_PRIVS);
            privLimit |= this.<Boolean>getValue(IDX_PRIV_MAC_OVRD) ? Privilege.PRIV_MAC_OVRD.id : 0;
            AccessContext accCtx = new AccessContext(userId, userRole, userSecDomain, new PrivilegeSet(privLimit));

            SecTypeIteration iteration = new SecTypeIteration();
            iteration.privs = privLimit;
            iteration.accCtx = accCtx;
            iteration.accCtxId = userId;
            iteration.accCtxRole = userRole;
            iteration.accCtxSecDomain = userSecDomain;

            iteration.sourceDomain = getValue(IDX_DOMAIN_SOURCE);
            iteration.targetDomain = getValue(IDX_DOMAIN_TARGET);

            iteration.requestingAccessType = getValue(IDX_REQUESTED_ACCESS_TYPE);
            iteration.grantedAccessType = getValue(IDX_GRANTED_ACCESS_TYPE);

            iteration.sourceDomain.addRule(rootCtx, iteration.targetDomain, iteration.grantedAccess);

            return iteration;
        }
    }
}
