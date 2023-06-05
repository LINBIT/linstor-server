package com.linbit.linstor.security;

import com.linbit.linstor.dbdrivers.interfaces.SecObjProtAclDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecObjProtDatabaseDriver;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.SatelliteTransactionMgr;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import static com.linbit.linstor.security.AccessType.CHANGE;
import static com.linbit.linstor.security.AccessType.CONTROL;
import static com.linbit.linstor.security.AccessType.USE;
import static com.linbit.linstor.security.AccessType.VIEW;
import static com.linbit.linstor.security.Privilege.PRIVILEGE_LIST;
import static com.linbit.linstor.security.Privilege.PRIV_MAC_OVRD;
import static com.linbit.linstor.security.Privilege.PRIV_OBJ_CHANGE;
import static com.linbit.linstor.security.Privilege.PRIV_OBJ_CONTROL;
import static com.linbit.linstor.security.Privilege.PRIV_OBJ_OWNER;
import static com.linbit.linstor.security.Privilege.PRIV_OBJ_USE;
import static com.linbit.linstor.security.Privilege.PRIV_OBJ_VIEW;
import static com.linbit.linstor.security.Privilege.PRIV_SYS_ALL;

import javax.inject.Provider;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class ObjectProtectionTest
{
    private AccessContext sysCtx;
    private AccessContext rootCtx;
    private PrivilegeSet privSysAll;

    private Identity userId;
    private Identity someOtherUserId;

    private Role userRole;
    private Role someOtherRole;

    private SecurityType userSecDomain;
    private SecurityType someOtherUserSecDomain;
    private TransactionObjectFactory transObjFactory;
    private Provider<TransactionMgr> transMgrProvider;
    private SecObjProtDatabaseDriver objProtDbDriver;
    private SecObjProtAclDatabaseDriver objProtAclDbDriver;

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
        privSysAll = new PrivilegeSet(Privilege.PRIV_SYS_ALL.id);

        userId = new Identity(new IdentityName("User"));
        someOtherUserId = new Identity(new IdentityName("SomeOtherUser"));

        userRole = new Role(new RoleName("UserRole"));
        someOtherRole = new Role(new RoleName("SomeOtherRole"));

        userSecDomain = new SecurityType(new SecTypeName("UserSecType"));
        someOtherUserSecDomain = new SecurityType(new SecTypeName("SomeOtherUserSecType"));

        TransactionMgr transMgr = new SatelliteTransactionMgr();
        transMgrProvider = () -> transMgr;
        transObjFactory = new TransactionObjectFactory(transMgrProvider);
        objProtDbDriver = new SatelliteSecObjProtDbDriver();
        objProtAclDbDriver = new SatelliteSecObjProtAclDbDriver();

        SecurityLevel.set(rootCtx, SecurityLevel.MAC, null, null);
    }

    @Test
    public void testGetOwner() throws Exception
    {
        AccessContext accCtx = new AccessContext(userId, userRole, userSecDomain, privSysAll);
        ObjectProtection objProt = new ObjectProtection(accCtx, "test", null, null, transObjFactory, null);

        assertEquals(userRole, objProt.getOwner());

        objProt.setOwner(rootCtx, someOtherRole);

        assertEquals(someOtherRole, objProt.getOwner());
    }

    @Test
    public void testSetOwnerAllCombinations() throws Exception
    {
        AccessIterator accIt = new AccessIterator(false);
        for (AccessIteration iteration : accIt)
        {
            // preparations
            AccessContext accCtx = iteration.userAccCtx;
            Role origOwner = iteration.objProtOwner;
            Role newOwner = origOwner == userRole ? someOtherRole : userRole;

            // testing logic
            boolean exceptionExpected = !accCtx.privEffective.hasPrivileges(PRIV_OBJ_OWNER);

            // perform test
            ObjectProtection objProt = iteration.objProt;
            if (exceptionExpected)
            {
                try
                {
                    objProt.setOwner(accCtx, newOwner);
                    fail("ObjectProtection set owner while accCtx should not have the needed permission");
                }
                catch (AccessDeniedException expected)
                {
                    // expected
                }
                assertEquals(origOwner, objProt.getOwner());
            }
            else
            {
                objProt.setOwner(accCtx, newOwner);
                assertEquals(newOwner, objProt.getOwner());
            }
        }
    }

    @Test
    public void testAddAclEntryAccessAllCombinations() throws Exception
    {
        Role[] targetRoles = new Role[]
        {
            userRole, someOtherRole
        };
        AccessIterator accIt = new AccessIterator(true);
        for (AccessIteration iteration : accIt)
        {
            // preparations
            AccessContext accCtx = iteration.userAccCtx;
            AccessControlList acl = createACL("dummy");

            acl.addEntry(accCtx.subjectRole, iteration.objProtAclForUser);
            SecurityType secType = iteration.objProtSecType;
            AccessType secTypeAccessType = secType.queryAccess(accCtx);

            boolean isUserOwner = iteration.objProtIsUserOwner;

            AccessType aclAccessType = acl.queryAccess(accCtx);
            AccessType grantedAccess = iteration.wantedAccessContext;

            for (Role targetRole : targetRoles)
            {
                // testing logic
                boolean selfProtectionException = accCtx.subjectRole == targetRole &&
                    !iteration.userAccCtx.privEffective.hasPrivileges(PRIV_OBJ_CONTROL);

                boolean expectException = (secTypeAccessType == null) ||
                    (!secTypeAccessType.hasAccess(CONTROL));
                if (!isUserOwner && !expectException)
                {
                    expectException = aclAccessType == null ||
                        !aclAccessType.hasAccess(CONTROL) ||
                        selfProtectionException;
                }

                // perform test
                ObjectProtection objProt = iteration.objProt;
                if (expectException)
                {
                    try
                    {
                        objProt.addAclEntry(accCtx, targetRole, grantedAccess);
                        fail("ObjectProtection allowed updating ACL without propper permission");
                    }
                    catch (AccessDeniedException expected)
                    {
                        // expected
                    }
                    assertEquals(acl.getEntry(targetRole), objProt.getAcl().getEntry(targetRole));
                }
                else
                {
                    objProt.addAclEntry(accCtx, targetRole, grantedAccess);
                    assertEquals(grantedAccess, objProt.getAcl().getEntry(targetRole));
                }
            }
        }
    }

    @Test
    public void testDelAclEntryAccessAllCombinations() throws Exception
    {
        Role[] targetRoles = new Role[]
        {
            userRole, someOtherRole
        };
        AccessIterator accIt = new AccessIterator(true);
        for (AccessIteration iteration : accIt)
        {
            // preparations
            AccessContext accCtx = iteration.userAccCtx;
            AccessControlList acl = createACL("dummy");

            acl.addEntry(accCtx.subjectRole, iteration.objProtAclForUser);
            SecurityType secType = iteration.objProtSecType;
            AccessType secTypeAccessType = secType.queryAccess(accCtx);

            boolean isUserOwner = iteration.objProtIsUserOwner;

            AccessType grantedAccess = iteration.wantedAccessContext;

            for (Role targetRole : targetRoles)
            {
                ObjectProtection objProt = iteration.objProt;
                objProt.addAclEntry(rootCtx, targetRole, grantedAccess);
                acl.addEntry(targetRole, grantedAccess);
                AccessType aclAccessType = acl.queryAccess(accCtx);

                // testing logic
                boolean selfProtectionException = accCtx.subjectRole == targetRole &&
                    !iteration.userAccCtx.privEffective.hasPrivileges(PRIV_OBJ_CONTROL);

                boolean expectException = (secTypeAccessType == null) ||
                    (!secTypeAccessType.hasAccess(CONTROL));
                if (!isUserOwner && !expectException)
                {
                    expectException = aclAccessType == null ||
                        !aclAccessType.hasAccess(CONTROL) ||
                        selfProtectionException;
                }

                // perform test
                if (expectException)
                {
                    try
                    {
                        objProt.delAclEntry(accCtx, targetRole);
                        fail("ObjectProtection allowed updating ACL without propper permission");
                    }
                    catch (AccessDeniedException expected)
                    {
                        // expected
                    }
                    assertEquals(grantedAccess, objProt.getAcl().getEntry(targetRole));
                }
                else
                {
                    objProt.delAclEntry(accCtx, targetRole);
                    assertNull(objProt.getAcl().getEntry(targetRole));
                }
            }
        }
    }

    @Test
    public void testGetCreator() throws Exception
    {
        AccessContext accCtx = new AccessContext(userId, userRole, userSecDomain, privSysAll);
        ObjectProtection objProt = new ObjectProtection(
            accCtx,
            "dummy",
            createACL("dummy"),
            objProtDbDriver,
            transObjFactory,
            transMgrProvider
        );

        assertEquals(userId, objProt.getCreator());

        objProt.resetCreator(rootCtx);

        assertNotEquals(userId, objProt.getCreator());
    }

    @Test
    public void testRequireAndQueryAccessAllCombinations() throws Exception
    {
        AccessIterator accIt = new AccessIterator(true);
        for (AccessIteration iteration : accIt)
        {
            // preparations
            // do not use the acl from objProt as we are currently testing that class / instance
            AccessControlList acl = createACL("dummy2");
            acl.addEntry(iteration.userAccCtx.subjectRole, iteration.objProtAclForUser);
            AccessContext accCtx = iteration.userAccCtx;
            AccessType expectedSecTypeAccessType = iteration.objProtSecType.queryAccess(accCtx);
            AccessType expectedAclAccessType = acl.queryAccess(accCtx);
            AccessType wantedAccessType = iteration.wantedAccessContext;

            // testing logic
            AccessType expectedAccessType = AccessType.intersect(expectedSecTypeAccessType, expectedAclAccessType);

            // perform tests
            ObjectProtection objProt = iteration.objProt;
            AccessType actualAccessType = objProt.queryAccess(accCtx);

            assertEquals(expectedAccessType, actualAccessType);

            if (expectedAccessType != null && expectedAccessType.hasAccess(wantedAccessType))
            {
                try
                {
                    objProt.requireAccess(accCtx, wantedAccessType);
                }
                catch (AccessDeniedException exc)
                {
                    fail("Access denied while it should not");
                }
            }
            else
            {
                try
                {
                    objProt.requireAccess(accCtx, wantedAccessType);
                    fail("Access granted while it should not");
                }
                catch (AccessDeniedException exc)
                {
                    // expected
                }
            }
        }
    }

    private AccessControlList createACL(String objPathRef)
    {
        return new AccessControlList(
            objPathRef,
            objProtAclDbDriver,
            transObjFactory,
            transMgrProvider
        );
    }

    @Test
    public void testSetSecurityType() throws Exception
    {
        AccessIterator accIt = new AccessIterator(true);
        for (AccessIteration iteration : accIt)
        {
            // preparations
            AccessContext accCtx = iteration.userAccCtx;
            ObjectProtection objProt = iteration.objProt;

            // testing logic
            boolean expectException = SecurityLevel.get() != SecurityLevel.NO_SECURITY &&
                !accCtx.privEffective.hasPrivileges(PRIV_SYS_ALL);

            // perform tests
            if (expectException)
            {
                try
                {
                    objProt.setSecurityType(accCtx, userSecDomain);
                    fail("Access granted while it should not");
                }
                catch (AccessDeniedException exc)
                {
                    // expected
                }
                assertEquals(someOtherUserSecDomain, objProt.getSecurityType());
            }
            else
            {
                try
                {
                    objProt.setSecurityType(accCtx, userSecDomain);
                    assertEquals(userSecDomain, objProt.getSecurityType());
                }
                catch (AccessDeniedException exc)
                {
                    fail("Access denied while it should not");
                }
            }
        }
    }

    @SuppressWarnings("unused")
    private static class AccessIteration
    {
        public ObjectProtection objProt;

        public AccessType objProtAclForUser;
        public AccessType objProtSecDomainEntryForUser;
        public boolean objProtIsUserOwner;
        public AccessContext objProtAccCtx;
        public SecurityType objProtSecType;
        public Role objProtOwner;

        public boolean userHasPrivMacOvrd;
        public long userPrivsLong;
        public AccessContext userAccCtx;

        public AccessType wantedAccessContext;
    }

    private class AccessIterator extends AbsSecurityIterator<AccessIteration>
    {
        private static final int OBJ_ACL_IDX = 0;
        private static final int OBJ_SEC_IDX = 1;
        private static final int OBJ_OWN_IDX = 2;

        private static final int ACC_MAC_OVRD_IDX = 3;
        private static final int ACC_PRIV_IDX = 4;

        private static final int WANTED_ACC_CTX_IDX = 5;

        AccessIterator(boolean iterateSecurityLevels, int... skipColumns)
        {
            super(
                new Object[][]
                {
                    // object protection
                    {null, VIEW, USE, CHANGE, CONTROL},         // objProt acl entry for user
                    {null, VIEW, USE, CHANGE, CONTROL},         // secDomain entry for user
                    {false, true},                              // is user owner of objProt
                    // accCtx
                    {false, true},                              // has PRIV_MAC_OVRD
                    {
                        0L, PRIV_OBJ_VIEW.id,                   //
                        PRIV_OBJ_USE.id, PRIV_OBJ_CHANGE.id,    // privileges.... :)
                        PRIV_OBJ_CONTROL.id, PRIV_OBJ_OWNER.id, //
                        PRIV_SYS_ALL.id                         //
                    },
                    // wanted access
                    {VIEW, USE, CHANGE, CONTROL}                // wantedAccessContext
                },
                iterateSecurityLevels,
                rootCtx,
                skipColumns
            );
        }

        @Override
        public AccessIteration getNext() throws Exception
        {
            AccessIteration nextIteration = new AccessIteration();

            getObjectProtection(nextIteration);
            getAccessContext(nextIteration);
            getWantedAccessType(nextIteration);

            return nextIteration;
        }

        private void getObjectProtection(AccessIteration iteration) throws Exception
        {
            AccessType aclEntry = getValue(OBJ_ACL_IDX);
            AccessType secEntry =  getValue(OBJ_SEC_IDX);
            boolean isOwner = getValue(OBJ_OWN_IDX);

            Role subjRole = isOwner ? userRole : someOtherRole;

            AccessContext objCtx = new AccessContext(someOtherUserId, subjRole, someOtherUserSecDomain, privSysAll);
            objCtx.privEffective.enablePrivileges(PRIVILEGE_LIST);

            ObjectProtection objProt = new ObjectProtection(
                objCtx,
                "dummy",
                createACL("dummy"),
                objProtDbDriver,
                transObjFactory,
                transMgrProvider
            );


            if (aclEntry != null)
            {
                objProt.addAclEntry(rootCtx, userRole, aclEntry);
            }

            if (secEntry != null)
            {
                someOtherUserSecDomain.addRule(rootCtx, userSecDomain, secEntry);
            }

            iteration.objProtOwner = subjRole;
            iteration.objProtAclForUser = aclEntry;
            iteration.objProtSecDomainEntryForUser = secEntry;
            iteration.objProtIsUserOwner = isOwner;
            iteration.objProtSecType = someOtherUserSecDomain;
            iteration.objProtAccCtx = objCtx;
            iteration.objProt = objProt;
        }

        private void getAccessContext(AccessIteration iteration) throws Exception
        {
            boolean hasPrivMacOvrd = getValue(ACC_MAC_OVRD_IDX);
            long privs = getValue(ACC_PRIV_IDX);

            iteration.userHasPrivMacOvrd = hasPrivMacOvrd;
            iteration.userPrivsLong = privs;

            if (hasPrivMacOvrd)
            {
                privs |= PRIV_MAC_OVRD.id;
            }

            PrivilegeSet privLimit = new PrivilegeSet(privs);
            AccessContext ctx = new AccessContext(userId, userRole, userSecDomain, privLimit);
            ctx.privEffective.enablePrivileges(privLimit.toArray());


            iteration.userAccCtx = ctx;
        }

        private void getWantedAccessType(AccessIteration iteration)
        {
            iteration.wantedAccessContext = getValue(WANTED_ACC_CTX_IDX);
        }
    }
}
