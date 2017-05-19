package com.linbit.drbdmanage.security;

import static com.linbit.drbdmanage.security.Privilege.*;
import static com.linbit.drbdmanage.security.AccessType.*;
import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;
import com.linbit.InvalidNameException;

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
        rootCtx.privEffective.enablePrivileges(PRIVILEGE_LIST);

        userId = new Identity(new IdentityName("User"));

        userRole = new Role(new RoleName("UserRole"));

        userSecDomain = new SecurityType(new SecTypeName("UserSecType"));
        someOtherUserSecDomain = new SecurityType(new SecTypeName("SomeOtherUserSecType"));

        SecurityLevel.set(rootCtx, SecurityLevel.MAC);
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
                !accCtx.privEffective.hasPrivileges(PRIV_SYS_ALL);

            AccessType origAccessType = sourceDomain.getEntry(targetDomain);

            if (expectedException)
            {
                try
                {
                    sourceDomain.addEntry(accCtx, targetDomain, grantedAccess);
                    fail("Exception expected");
                }
                catch (AccessDeniedException expected)
                {
                    // expected
                }
                assertEquals(origAccessType, sourceDomain.getEntry(targetDomain));
            }
            else
            {
                sourceDomain.addEntry(accCtx, targetDomain, grantedAccess);
                assertEquals(grantedAccess, sourceDomain.getEntry(targetDomain));
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

            sourceDomain.addEntry(rootCtx, targetDomain, accessType);

            boolean expectedException = !SecurityLevel.get().equals(SecurityLevel.NO_SECURITY) &&
                !accCtx.privEffective.hasPrivileges(PRIV_SYS_ALL);

            if (expectedException)
            {
                try
                {
                    sourceDomain.delEntry(accCtx, targetDomain);
//                    System.out.println(secTypeIt);
                    fail("Exception expected");
                }
                catch (AccessDeniedException expected)
                {
                    // expected
                }
                assertEquals(accessType, sourceDomain.getEntry(targetDomain));
            }
            else
            {
                sourceDomain.delEntry(accCtx, targetDomain);
                assertNull(sourceDomain.getEntry(targetDomain));
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
                !ruleGrantsAccess && !accCtx.privEffective.hasPrivileges(PRIV_MAC_OVRD);

            if (expectException)
            {
                try
                {
                    secType.requireAccess(accCtx, requestedAT);
                    fail("Exception expected");
                }
                catch (AccessDeniedException e)
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
    public void testQueryAccess() throws AccessDeniedException
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
                assertEquals(grantedAT, secTypeSource.getEntry(accCtx));
            }
            assertEquals(grantedAT, secTypeSource.getEntry(secTypeTarget));
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

    private class SecTypeIterator extends AbsIterator<SecTypeIteration>
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
                        0L, PRIV_OBJ_VIEW.id,                   //
                        PRIV_OBJ_USE.id, PRIV_OBJ_CHANGE.id ,   // privileges.... :)
                        PRIV_OBJ_CONTROL.id, PRIV_OBJ_OWNER.id, //
                        PRIV_SYS_ALL.id                         //
                    },
                    { true, false },                            // has PRIV_MAC_OVRD
                    { userSecDomain, someOtherUserSecDomain },  // source domain
                    { userSecDomain, someOtherUserSecDomain },  // target domain
                    { null, VIEW, USE, CHANGE, CONTROL },       // requested access types
                    { null, VIEW, USE, CHANGE, CONTROL }        // granted access types
                },
                iterateSecLevel,
                rootCtx, skipColumns
            );
        }

        @Override
        protected SecTypeIteration getNext() throws Exception
        {
            long privLimit = getValue(IDX_PRIVS);
            privLimit |= getValue(IDX_PRIV_MAC_OVRD) ? PRIV_MAC_OVRD.id : 0;
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

            iteration.sourceDomain.addEntry(rootCtx, iteration.targetDomain, iteration.grantedAccess);

            return iteration;
        }
    }
}
