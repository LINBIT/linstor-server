package com.linbit.linstor.security;

import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.SecObjProtAclDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecObjProtDatabaseDriver;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.SatelliteTransactionMgr;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Provider;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Tests the security model
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class SecurityModelTest
{
    private AccessContext sysCtx;
    private AccessContext publicCtx;
    private AccessContext creatorCtx;

    private Identity buddyId;
    private Role buddyRole;
    private Identity banditId;
    private Role banditRole;

    private SecurityType userType;
    private TransactionObjectFactory transObjFactory;
    private Provider<TransactionMgr> transMgrProvider;
    private SecObjProtDatabaseDriver objProtDbDriver;
    private SecObjProtAclDatabaseDriver objProtAclDbDriver;

    @Before
    public void setUp() throws Exception
    {
        // Restore the global security level to MAC before each test
        SatelliteTransactionMgr transMgr = new SatelliteTransactionMgr();
        transMgrProvider = () -> transMgr;
        transObjFactory = new TransactionObjectFactory(transMgrProvider);
        objProtDbDriver = new SatelliteSecObjProtDbDriver();
        objProtAclDbDriver = new SatelliteSecObjProtAclDbDriver();
        setSecurityLevel(SecurityLevel.MAC);
    }

    public SecurityModelTest()
        throws Throwable
    {
        sysCtx = new AccessContext(
            new Identity(new IdentityName("SYSTEM")),
            new Role(new RoleName("SYSTEM")),
            new SecurityType(new SecTypeName("SYSTEM")),
            new PrivilegeSet(Privilege.PRIV_SYS_ALL)
        );
        sysCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_SYS_ALL);

        publicCtx = sysCtx.impersonate(
            new Identity(new IdentityName("PUBLIC")),
            new Role(new RoleName("PUBLIC")),
            new SecurityType(new SecTypeName("PUBLIC"))
        );

        userType = new SecurityType(new SecTypeName("User"));
        userType.addRule(sysCtx, userType, AccessType.CONTROL);

        creatorCtx = sysCtx.impersonate(
            new Identity(new IdentityName("ObjCreator")),
            new Role(new RoleName("ObjOwner")),
            userType
        );

        buddyId = new Identity(new IdentityName("Buddy"));
        buddyRole = new Role(new RoleName("Buddy"));

        banditId = new Identity(new IdentityName("Bandit"));
        banditRole = new Role(new RoleName("Bandit"));
    }

    // Access allowed by ACL and type
    @Test
    public void testObjProtAllow()
        throws Throwable
    {
        AccessContext buddyCtx = sysCtx.impersonate(
            buddyId, buddyRole, userType
        );
        ObjectProtection prot = createDefaultObjProt();
        prot.addAclEntry(creatorCtx, buddyRole, AccessType.CHANGE);
        prot.requireAccess(buddyCtx, AccessType.VIEW);
        prot.requireAccess(buddyCtx, AccessType.USE);
        prot.requireAccess(buddyCtx, AccessType.CHANGE);
    }

    @Test(expected = AccessDeniedException.class)
    public void testSecLevelMacObjProtAclDeny()
        throws Throwable
    {
        ObjectProtection prot = null;
        AccessContext banditCtx = null;
        try
        {
            // Create object as creator
            prot = createDefaultObjProt();
            // Grant access to buddy
            prot.addAclEntry(creatorCtx, buddyRole, AccessType.CHANGE);

            // Impersonate the bandit role
            banditCtx = sysCtx.impersonate(
                banditId, banditRole, userType
            );
            // Attempt to access the object as bandit
        }
        catch (AccessDeniedException deniedExc)
        {
            fail("AccessDeniedException while preparing the test");
        }
        prot.requireAccess(banditCtx, AccessType.VIEW);
        fail("Access by unauthorized role succeeded");
    }

    @Test(expected = AccessDeniedException.class)
    public void testSecLevelRbacObjProtAclDeny()
        throws Throwable
    {
        setSecurityLevel(SecurityLevel.RBAC);

        ObjectProtection prot = null;
        AccessContext banditCtx = null;
        try
        {
            // Create object as creator
            prot = createDefaultObjProt();
            // Grant access to buddy
            prot.addAclEntry(creatorCtx, buddyRole, AccessType.CHANGE);

            // Impersonate the bandit role
            banditCtx = sysCtx.impersonate(
                banditId, banditRole, userType
            );
            // Attempt to access the object as bandit
        }
        catch (AccessDeniedException deniedExc)
        {
            fail("AccessDeniedException while preparing the test");
        }
        prot.requireAccess(banditCtx, AccessType.VIEW);
        fail("Access by unauthorized role succeeded");
    }

    @Test(expected = AccessDeniedException.class)
    public void testSecLevelMacTypeDeny()
        throws Throwable
    {
        ObjectProtection prot = null;
        AccessContext banditCtx = null;
        try
        {
            // Create object as creator
            prot = createDefaultObjProt();
            // Grant access to buddy
            prot.addAclEntry(creatorCtx, buddyRole, AccessType.CHANGE);

            // Impersonate the buddy role, but in the 'public' domain instead of the 'user' domain
            // that the object protection was created in
            banditCtx = sysCtx.impersonate(
                banditId, buddyRole, publicCtx.getDomain()
            );
        }
        catch (AccessDeniedException deniedExc)
        {
            fail("AccessDeniedException while preparing the test");
        }
        // Attempt to access the object from an unauthorized domain
        prot.requireAccess(banditCtx, AccessType.VIEW);
        fail("Access by unauthorized domain succeeded");
    }

    // Test ACL modification by owner and authorized user at the MAC security level
    @Test
    public void testSecLevelMacBuddyAclModify()
        throws Throwable
    {
        AccessContext buddyCtx = sysCtx.impersonate(
            buddyId, buddyRole, creatorCtx.getDomain()
        );
        ObjectProtection prot = createDefaultObjProt();
        // Authorize buddy to change access control entries
        prot.addAclEntry(creatorCtx, buddyRole, AccessType.CONTROL);
        // As buddy, add an entry allowing public VIEW access
        prot.addAclEntry(buddyCtx, publicCtx.getRole(), AccessType.VIEW);
    }

    // Test ACL modification by owner and authorized user at the RBAC security level
    @Test
    public void testSecLevelRbacBuddyAclModify()
        throws Throwable
    {
        setSecurityLevel(SecurityLevel.RBAC);

        AccessContext buddyCtx = sysCtx.impersonate(
            buddyId, buddyRole, creatorCtx.getDomain()
        );
        ObjectProtection prot = createDefaultObjProt();
        // Authorize buddy to change access control entries
        prot.addAclEntry(creatorCtx, buddyRole, AccessType.CONTROL);
        // As buddy, add an entry allowing public VIEW access
        prot.addAclEntry(buddyCtx, publicCtx.getRole(), AccessType.VIEW);
    }

    // Test ACL modification by unauthorized user at the MAC security level
    @Test(expected = AccessDeniedException.class)
    public void testSecLevelMacBanditAclModify()
        throws Throwable
    {
        ObjectProtection prot = null;
        AccessContext banditCtx = null;
        try
        {
            // Create object as creator
            prot = createDefaultObjProt();
            // Grant CONTROL access to buddy
            prot.addAclEntry(creatorCtx, buddyRole, AccessType.CONTROL);

            // Impersonate bandit
            banditCtx = sysCtx.impersonate(
                banditId, banditRole, creatorCtx.getDomain()
            );
        }
        catch (AccessDeniedException deniedExc)
        {
            fail("AccessDeniedException while preparing the test");
        }
        // As an unauthorized user, attempt to add an entry allowing public VIEW access
        prot.addAclEntry(banditCtx, publicCtx.getRole(), AccessType.VIEW);
        fail("Access by unauthorized domain succeeded");
    }

    // Test ACL modification by unauthorized user at the RBAC security level
    @Test(expected = AccessDeniedException.class)
    public void testSecLevelRbacBanditAclModify()
        throws Throwable
    {
        setSecurityLevel(SecurityLevel.RBAC);

        ObjectProtection prot = null;
        AccessContext banditCtx = null;
        try
        {
            // Create object as creator
            prot = createDefaultObjProt();
            // Grant CONTROL access to buddy
            prot.addAclEntry(creatorCtx, buddyRole, AccessType.CONTROL);

            // Impersonate bandit
            banditCtx = sysCtx.impersonate(
                banditId, banditRole, creatorCtx.getDomain()
            );
        }
        catch (AccessDeniedException deniedExc)
        {
            fail("AccessDeniedException while preparing the test");
        }
        // As an unauthorized user, attempt to add an entry allowing public VIEW access
        prot.addAclEntry(banditCtx, publicCtx.getRole(), AccessType.VIEW);
        fail("Access by unauthorized domain succeeded");
    }

    @Test
    public void testSysTypeModify() throws Throwable
    {
        ObjectProtection prot = null;
        try
        {
            // Create object as creator
            prot = createDefaultObjProt();
            prot.setSecurityType(sysCtx, sysCtx.getDomain());
        }
        catch (AccessDeniedException deniedExc)
        {
            fail("AccessDeniedException while preparing the test");
        }
    }

    @Test(expected = AccessDeniedException.class)
    public void testSecLevelMacBanditTypeModify()
        throws Throwable
    {
        ObjectProtection prot = null;
        AccessContext banditCtx = null;
        try
        {
            // Create object as creator
            prot = createDefaultObjProt();

            // Impersonate bandit
            banditCtx = sysCtx.impersonate(
                banditId, banditRole, creatorCtx.getDomain()
            );

        }
        catch (AccessDeniedException deniedExc)
        {
            fail("AccessDeniedException while preparing the test");
        }
        prot.setSecurityType(banditCtx, sysCtx.getDomain());
    }

    @Test(expected = AccessDeniedException.class)
    public void testSecLevelRbacBanditTypeModify()
        throws Throwable
    {
        setSecurityLevel(SecurityLevel.RBAC);

        ObjectProtection prot = null;
        AccessContext banditCtx = null;
        try
        {
            // Create object as creator
            prot = createDefaultObjProt();

            // Impersonate bandit
            banditCtx = sysCtx.impersonate(
                banditId, banditRole, creatorCtx.getDomain()
            );

        }
        catch (AccessDeniedException deniedExc)
        {
            fail("AccessDeniedException while preparing the test");
        }
        prot.setSecurityType(banditCtx, sysCtx.getDomain());
    }

    @Test
    public void testSysOwnerModify() throws Throwable
    {
        ObjectProtection prot = null;
        try
        {
            // Create object as creator
            prot = createDefaultObjProt();

            prot.setOwner(sysCtx, buddyRole);
        }
        catch (AccessDeniedException deniedExc)
        {
            fail("AccessDeniedException while preparing the test");
        }
    }

    @Test(expected = AccessDeniedException.class)
    public void testSecLevelMacBanditOwnerModify()
        throws Throwable
    {
        ObjectProtection prot = null;
        AccessContext banditCtx = null;
        try
        {
            // Create object as creator
            prot = createDefaultObjProt();

            // Impersonate bandit
            banditCtx = sysCtx.impersonate(
                banditId, banditRole, creatorCtx.getDomain()
            );
        }
        catch (AccessDeniedException deniedExc)
        {
            fail("AccessDeniedException while preparing the test");
        }
        prot.setOwner(banditCtx, buddyRole);
    }

    @Test(expected = AccessDeniedException.class)
    public void testSecLevelRbacBanditOwnerModify()
        throws Throwable
    {
        setSecurityLevel(SecurityLevel.RBAC);

        ObjectProtection prot = null;
        AccessContext banditCtx = null;
        try
        {
            // Create object as creator
            prot = createDefaultObjProt();

            // Impersonate bandit
            banditCtx = sysCtx.impersonate(
                banditId, banditRole, creatorCtx.getDomain()
            );
        }
        catch (AccessDeniedException deniedExc)
        {
            fail("AccessDeniedException while preparing the test");
        }
        prot.setOwner(banditCtx, buddyRole);
    }

    @Test
    public void testCloning()
        throws Throwable
    {
        // Create object as creator
        ObjectProtection prot = createDefaultObjProt();

        AccessContext clone1Sys = sysCtx.clone();
        clone1Sys.getEffectivePrivs().disablePrivileges(Privilege.PRIV_SYS_ALL);

        try
        {
            // This should fail
            prot.setOwner(clone1Sys, buddyRole);
            fail("Access by non-privileged system context succeeded (#1)");
        }
        catch (AccessDeniedException ignored)
        {
        }

        AccessContext clone2Sys = clone1Sys.clone();
        clone2Sys.getEffectivePrivs().enablePrivileges(Privilege.PRIV_SYS_ALL);

        // This should succeed
        prot.setOwner(clone2Sys, buddyRole);

        try
        {
            // This should still fail
            prot.setOwner(clone1Sys, buddyRole);
            fail("Access by non-privileged system context succeeded (#2)");
        }
        catch (AccessDeniedException ignored)
        {
        }
    }

    @Test
    public void testEffectivePrivs()
        throws Throwable
    {
        ObjectProtection prot = null;
        AccessContext buddyCtx = null;
        try
        {
            prot = createDefaultObjProt();
            buddyCtx = sysCtx.impersonate(
                buddyId, buddyRole, userType, Privilege.PRIV_OBJ_OWNER
            );
            assertNotNull(buddyCtx);
        }
        catch (AccessDeniedException deniedExc)
        {
            fail("AccessDeniedException while preparing the test");
        }

        try
        {
            // This should fail, because the privilege
            // is not enabled in the effective set
            prot.setOwner(buddyCtx, publicCtx.getRole());
            fail("Privileged access succeeded without enabling the privilege in the effective set");
        }
        catch (AccessDeniedException ignored)
        {
        }

        buddyCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_OBJ_OWNER);
        // This should succeed
        prot.setOwner(buddyCtx, publicCtx.getRole());

        buddyCtx.getEffectivePrivs().disablePrivileges(Privilege.PRIV_OBJ_OWNER);
        try
        {
            // This should fail again
            prot.setOwner(buddyCtx, publicCtx.getRole());
            fail("Privileged access succeeded after disabling the privilege in the effective set");
        }
        catch (AccessDeniedException ignored)
        {
        }
    }

    @Test
    public void testLimitPrivs()
        throws Throwable
    {
        ObjectProtection prot = null;
        AccessContext buddyCtx = null;
        try
        {
            prot = createDefaultObjProt();
            buddyCtx = sysCtx.impersonate(
                buddyId, buddyRole, userType, Privilege.PRIV_OBJ_OWNER
            );
            assertNotNull(buddyCtx);
        }
        catch (AccessDeniedException deniedExc)
        {
            fail("AccessDeniedException while preparing the test");
        }

        buddyCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_OBJ_OWNER);
        // This should succeed
        prot.setOwner(buddyCtx, publicCtx.getRole());

        buddyCtx.getLimitPrivs().disablePrivileges(Privilege.PRIV_SYS_ALL);
        try
        {
            prot.setOwner(buddyCtx, publicCtx.getRole());
            fail("Privileged access succeeded after disabling the privilege in the limit set");
        }
        catch (AccessDeniedException ignored)
        {
        }

        try
        {
            // This should fail because the privilege is no longer in the limit set
            buddyCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_OBJ_OWNER);
            fail("Enabling a privilege that is not in the limit set succeeded");
        }
        catch (AccessDeniedException ignored)
        {
        }
    }

    @Test
    public void testSysCreatorModify()
        throws Throwable
    {
        ObjectProtection prot = null;

        // Create object as creator
        prot = createDefaultObjProt();
        prot.resetCreator(sysCtx);
    }

    @Test(expected = AccessDeniedException.class)
    public void testSecLevelMacBanditCreatorModify()
        throws Throwable
    {
        ObjectProtection prot = null;
        AccessContext banditCtx = null;
        try
        {
            // Create object as creator
            prot = createDefaultObjProt();

            // Impersonate bandit
            banditCtx = sysCtx.impersonate(
                banditId, banditRole, creatorCtx.getDomain()
            );
        }
        catch (AccessDeniedException deniedExc)
        {
            fail("AccessDeniedException while preparing the test");
        }
        prot.resetCreator(banditCtx);
    }

    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_INFERRED")
    @Test(expected = AccessDeniedException.class)
    public void testSecLevelMacImpersonateDeny()
        throws Throwable
    {
        publicCtx.impersonate(Identity.SYSTEM_ID, Role.SYSTEM_ROLE, userType);
    }

    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_INFERRED")
    @Test(expected = AccessDeniedException.class)
    public void testSecLevelRbacImpersonateDeny()
        throws Throwable
    {
        setSecurityLevel(SecurityLevel.RBAC);

        publicCtx.impersonate(Identity.SYSTEM_ID, Role.SYSTEM_ROLE, userType);
    }

    @Test
    public void testSecLevelRbacTypeOvrd()
        throws Throwable
    {
        setSecurityLevel(SecurityLevel.RBAC);

        ObjectProtection prot = null;
        AccessContext otherDomainCtx = null;
        try
        {
            // Create object as creator
            prot = createDefaultObjProt();
            // Grant access to buddy
            prot.addAclEntry(creatorCtx, buddyRole, AccessType.CHANGE);

            // Impersonate the buddy role, but in the 'public' domain instead of the 'user' domain
            // that the object protection was created in
            otherDomainCtx = sysCtx.impersonate(
                banditId, buddyRole, publicCtx.getDomain()
            );
        }
        catch (AccessDeniedException deniedExc)
        {
            fail("AccessDeniedException while preparing the test");
        }
        // Attempt to access the object from an unauthorized domain, but with
        // security level = RBAC, domains/types not enforced
        prot.requireAccess(otherDomainCtx, AccessType.VIEW);
    }

    @Test(expected = AccessDeniedException.class)
    public void testSecLevelRbacAclDeny()
        throws Throwable
    {
        ObjectProtection prot = null;
        AccessContext banditCtx = null;
        try
        {
            // Create object as creator
            prot = createDefaultObjProt();
            // Grant access to buddy
            prot.addAclEntry(creatorCtx, buddyRole, AccessType.CHANGE);

            // Impersonate the bandit role
            banditCtx = sysCtx.impersonate(
                banditId, banditRole, userType
            );
            // Attempt to access the object as bandit
        }
        catch (AccessDeniedException deniedExc)
        {
            fail("AccessDeniedException while preparing the test");
        }
        prot.requireAccess(banditCtx, AccessType.VIEW);
        fail("Access by unauthorized role succeeded");
    }

    @Test
    public void testSecLevelNoneObjProtOvrd()
        throws Throwable
    {
        setSecurityLevel(SecurityLevel.NO_SECURITY);

        ObjectProtection prot = null;
        // Create the object as creator
        prot = createDefaultObjProt();
        prot.requireAccess(publicCtx, AccessType.CONTROL);
    }

    @Test(expected = AccessDeniedException.class)
    public void testBanditSetSecLevel()
        throws Throwable
    {
        AccessContext banditCtx = sysCtx.impersonate(
            banditId, banditRole, publicCtx.getDomain()
        );
        SecurityLevel.set(banditCtx, SecurityLevel.NO_SECURITY, null, null);
        fail("SecurityLevel change by unauthorized role succeeded");
    }

    protected void setSecurityLevel(SecurityLevel level) throws AccessDeniedException, DatabaseException
    {
        SecurityLevel.set(sysCtx, level, null, null);
    }

    private ObjectProtection createDefaultObjProt()
    {
        return new ObjectProtection(
            creatorCtx,
            "dummy",
            new AccessControlList(
                "dummy",
                objProtAclDbDriver,
                transObjFactory,
                transMgrProvider
            ),
            objProtDbDriver,
            transObjFactory,
            transMgrProvider
        );
    }
}
