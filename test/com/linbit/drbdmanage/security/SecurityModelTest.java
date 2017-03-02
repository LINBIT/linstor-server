package com.linbit.drbdmanage.security;

import com.linbit.drbdmanage.InvalidNameException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

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

    @Before
    public void setUp()
    {
    }

    @After
    public void tearDown()
    {
    }

    public SecurityModelTest()
        throws InvalidNameException, AccessDeniedException
    {
        sysCtx = new AccessContext(
            new Identity(new IdentityName("SYSTEM")),
            new Role(new RoleName("SYSTEM")),
            new SecurityType(new SecTypeName("SYSTEM")),
            new PrivilegeSet(
                new Privilege[]
                {
                    Privilege.PRIV_SYS_ALL
                }
            )
        );
        sysCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_SYS_ALL);

        publicCtx = sysCtx.impersonate(
            new Identity(new IdentityName("PUBLIC")),
            new Role(new RoleName("PUBLIC")),
            new SecurityType(new SecTypeName("PUBLIC"))
        );

        userType = new SecurityType(new SecTypeName("User"));
        userType.addEntry(sysCtx, userType, AccessType.CONTROL);

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
    public void objProtAllow()
        throws AccessDeniedException
    {
        AccessContext buddyCtx = sysCtx.impersonate(
            buddyId, buddyRole, userType
        );
        ObjectProtection prot = new ObjectProtection(creatorCtx);
        prot.addAclEntry(creatorCtx, buddyRole, AccessType.CHANGE);
        prot.requireAccess(buddyCtx, AccessType.VIEW);
        prot.requireAccess(buddyCtx, AccessType.USE);
        prot.requireAccess(buddyCtx, AccessType.CHANGE);
    }

    @Test(expected=AccessDeniedException.class)
    public void objProtAclDeny()
        throws AccessDeniedException
    {
        ObjectProtection prot = null;
        AccessContext banditCtx = null;
        try
        {
        // Create object as creator
        prot = new ObjectProtection(creatorCtx);
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

    @Test(expected=AccessDeniedException.class)
    public void objProtTypeDeny()
        throws AccessDeniedException
    {
        ObjectProtection prot = null;
        AccessContext banditCtx = null;
        try
        {
            // Create object as creator
            prot = new ObjectProtection(creatorCtx);
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

    // Test ACL modification by owner and authorized user
    @Test
    public void testBuddyAclModify()
        throws AccessDeniedException
    {
        AccessContext buddyCtx = sysCtx.impersonate(
            buddyId, buddyRole, creatorCtx.getDomain()
        );
        ObjectProtection prot = new ObjectProtection(creatorCtx);
        // Authorize buddy to change access control entries
        prot.addAclEntry(creatorCtx, buddyRole, AccessType.CONTROL);
        // As buddy, add an entry allowing public VIEW access
        prot.addAclEntry(buddyCtx, publicCtx.getRole(), AccessType.VIEW);
    }

    // Test ACL modification by unauthorized user
    @Test(expected=AccessDeniedException.class)
    public void testBanditAclModify()
        throws AccessDeniedException
    {
        ObjectProtection prot = null;
        AccessContext banditCtx = null;
        try
        {
            // Create object as creator
            prot = new ObjectProtection(creatorCtx);
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
    public void testSysTypeModify()
        throws AccessDeniedException
    {
        ObjectProtection prot = null;
        AccessContext banditCtx = null;
        try
        {
            // Create object as creator
            prot = new ObjectProtection(creatorCtx);
            prot.setSecurityType(sysCtx, sysCtx.getDomain());
        }
        catch (AccessDeniedException deniedExc)
        {
            fail("AccessDeniedException while preparing the test");
        }
    }

    @Test(expected=AccessDeniedException.class)
    public void testBanditTypeModify()
        throws AccessDeniedException
    {
        ObjectProtection prot = null;
        AccessContext banditCtx = null;
        try
        {
            // Create object as creator
            prot = new ObjectProtection(creatorCtx);

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
    public void testSysOwnerModify()
        throws AccessDeniedException
    {
        ObjectProtection prot = null;
        AccessContext banditCtx = null;
        try
        {
            // Create object as creator
            prot = new ObjectProtection(creatorCtx);

            prot.setOwner(sysCtx, buddyRole);
        }
        catch (AccessDeniedException deniedExc)
        {
            fail("AccessDeniedException while preparing the test");
        }
    }

    @Test(expected=AccessDeniedException.class)
    public void testBanditOwnerModify()
        throws AccessDeniedException
    {
        ObjectProtection prot = null;
        AccessContext banditCtx = null;
        try
        {
            // Create object as creator
            prot = new ObjectProtection(creatorCtx);

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
        throws AccessDeniedException
    {
        // Create object as creator
        ObjectProtection prot = new ObjectProtection(creatorCtx);

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
        throws AccessDeniedException
    {
        ObjectProtection prot = null;
        AccessContext buddyCtx = null;
        try
        {
            prot = new ObjectProtection(creatorCtx);
            buddyCtx = sysCtx.impersonate(
                buddyId, buddyRole, userType, Privilege.PRIV_OBJ_OWNER
            );
        }
        catch (AccessDeniedException deniedExc)
        {
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
        throws AccessDeniedException
    {
        ObjectProtection prot = null;
        AccessContext buddyCtx = null;
        try
        {
            prot = new ObjectProtection(creatorCtx);
            buddyCtx = sysCtx.impersonate(
                buddyId, buddyRole, userType, Privilege.PRIV_OBJ_OWNER
            );
        }
        catch (AccessDeniedException deniedExc)
        {
        }

        buddyCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_OBJ_OWNER);
        // This should succeed
        prot.setOwner(buddyCtx, publicCtx.getRole());

        buddyCtx.getLimitPrivs().disablePrivileges(Privilege.PRIV_SYS_ALL);
        try
        {
            // This should fail again
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
        throws AccessDeniedException
    {
        ObjectProtection prot = null;
        AccessContext banditCtx = null;
        try
        {
            // Create object as creator
            prot = new ObjectProtection(creatorCtx);
            prot.resetCreator(sysCtx);
        }
        catch (AccessDeniedException deniedExc)
        {
            fail("AccessDeniedException while preparing the test");
        }
    }

    @Test(expected=AccessDeniedException.class)
    public void testBanditCreatorModify()
        throws AccessDeniedException
    {
        ObjectProtection prot = null;
        AccessContext banditCtx = null;
        try
        {
            // Create object as creator
            prot = new ObjectProtection(creatorCtx);

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

    @Test(expected=AccessDeniedException.class)
    public void testImpersonateDeny()
        throws AccessDeniedException
    {
        publicCtx.impersonate(Identity.SYSTEM_ID, Role.SYSTEM_ROLE, userType);
    }
}
