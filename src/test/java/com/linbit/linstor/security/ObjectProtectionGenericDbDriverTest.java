package com.linbit.linstor.security;

import com.linbit.InvalidNameException;
import com.linbit.linstor.InitializationException;
import com.linbit.linstor.dbdrivers.DatabaseException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ObjectProtectionGenericDbDriverTest extends GenericDbBase
{
    private static final String OP_SELECT =
        "SELECT * FROM " + TBL_SEC_OBJECT_PROTECTION +
        " WHERE " + OBJECT_PATH + " = ?";
    private static final String OP_INSERT =
        "INSERT INTO " + TBL_SEC_OBJECT_PROTECTION + " VALUES (?, ?, ?, ?)";

    private static final String ACL_SELECT =
        "SELECT * FROM " + TBL_SEC_ACL_MAP +
        " WHERE " + OBJECT_PATH + " = ?";
    private static final String ACL_SELECT_BY_OBJPATH_AND_ROLE =
        "SELECT * FROM " + TBL_SEC_ACL_MAP +
        " WHERE " + OBJECT_PATH + " = ? AND " +
                    ROLE_NAME   + " = ?";
    private static final String ACL_INSERT =
        "INSERT INTO " + TBL_SEC_ACL_MAP + " VALUES (?, ?, ?)";

    private static final String ROLES_INSERT =
        "INSERT INTO " + TBL_SEC_ROLES + " VALUES (?, ?, ?, ?, ?)";

    @Before
    public void setUp() throws Exception
    {
        super.setUpAndEnterScope();
    }

    @Test
    public void testCreateSimpleObjProt() throws DatabaseException, SQLException, AccessDeniedException
    {
        @SuppressWarnings("resource")
        final Connection con = getConnection();

        final String objPath = "testPath";
        objectProtectionFactory.getInstance(SYS_CTX, objPath, true);

        commit();

        final PreparedStatement stmt = con.prepareStatement(OP_SELECT);
        stmt.setString(1, objPath);
        final ResultSet resultSet = stmt.executeQuery();

        assertTrue("Database did not persist objectProtection instance", resultSet.next());

        assertEquals(objPath, resultSet.getString(OBJECT_PATH));
        assertEquals(SYS_CTX.subjectId.name.value, resultSet.getString(CREATOR_IDENTITY_NAME));
        assertEquals(SYS_CTX.subjectRole.name.value, resultSet.getString(OWNER_ROLE_NAME));
        assertEquals(SYS_CTX.subjectDomain.name.value, resultSet.getString(SECURITY_TYPE_NAME));

        assertFalse("Database contains more data than expected", resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test (expected = DatabaseException.class)
    public void testCreateUnknownIdObjProt() throws Exception
    {
        final String objPath = "testPath";

        AccessContext accCtx = new AccessContext(
            new Identity(new IdentityName("UNKNOWN")),
            SYS_CTX.subjectRole,
            SYS_CTX.subjectDomain,
            SYS_CTX.privEffective
        );
        objectProtectionFactory.getInstance(accCtx, objPath, true);
        fail("Creating an ObjectProtection with an unknown identity should have failed");
    }

    @Test (expected = DatabaseException.class)
    public void testCreateUnknownRoleObjProt() throws Exception
    {
        final String objPath = "testPath";

        AccessContext accCtx = new AccessContext(
            SYS_CTX.subjectId,
            new Role(new RoleName("UNKNOWN")),
            SYS_CTX.subjectDomain,
            SYS_CTX.privEffective
        );
        objectProtectionFactory.getInstance(accCtx, objPath, true);
        fail("Creating an ObjectProtection with an unknown role should have failed");
    }

    @Test (expected = DatabaseException.class)
    public void testCreateUnknownSecTypeObjProt() throws Exception
    {
        final String objPath = "testPath";

        AccessContext accCtx = new AccessContext(
            SYS_CTX.subjectId,
            SYS_CTX.subjectRole,
            new SecurityType(new SecTypeName("UNKNOWN")),
            SYS_CTX.privEffective
        );
        objectProtectionFactory.getInstance(accCtx, objPath, true);
        fail("Creating an ObjectProtection with an unknown identity should have failed");
    }

    @SuppressWarnings({"checkstyle:magicnumber"})
    @Test
    public void testLoadSimpleObjProt()
        throws DatabaseException, SQLException, AccessDeniedException, InitializationException
    {
        Connection con = getNewConnection();
        PreparedStatement stmt = con.prepareStatement(OP_INSERT);
        final String objPath = "testPath";
        stmt.setString(1, objPath);
        stmt.setString(2, SYS_CTX.subjectId.name.value);
        stmt.setString(3, SYS_CTX.subjectRole.name.value);
        stmt.setString(4, SYS_CTX.subjectDomain.name.value);
        stmt.executeUpdate();
        stmt.close();

        con.commit();
        con.close();

        reloadSecurityObjects();
        ObjectProtection objProt = objectProtectionFactory.getInstance(SYS_CTX, objPath, false);

        assertEquals(SYS_CTX.subjectId, objProt.getCreator());
        assertEquals(SYS_CTX.subjectRole, objProt.getOwner());
        assertEquals(SYS_CTX.subjectDomain, objProt.getSecurityType());
    }

    @SuppressWarnings("checkstyle:magicnumber")
    @Test
    public void testAddAcl() throws DatabaseException, SQLException, AccessDeniedException, InvalidNameException
    {
        @SuppressWarnings("resource")
        Connection con = getConnection();

        final String objPath = "testPath";

        Role testRole = Role.create(SYS_CTX, new RoleName("test"));

        PreparedStatement stmt = con.prepareStatement(ROLES_INSERT);
        stmt.setString(1, testRole.name.value);
        stmt.setString(2, testRole.name.displayValue);
        stmt.setString(3, SYS_CTX.subjectDomain.name.value);
        stmt.setBoolean(4, true);
        stmt.setLong(5, Privilege.PRIV_SYS_ALL.id);
        stmt.executeUpdate();
        stmt.close();

        ObjectProtection objProt = objectProtectionFactory.getInstance(SYS_CTX, objPath, true);
        objProt.addAclEntry(SYS_CTX, testRole, AccessType.CHANGE);

        commit();

        stmt = con.prepareStatement(ACL_SELECT_BY_OBJPATH_AND_ROLE);
        stmt.setString(1, objPath);
        stmt.setString(2, testRole.name.value);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue("Database did not persist acl entry", resultSet.next());

        assertEquals(objPath, resultSet.getString(OBJECT_PATH));
        assertEquals(testRole.name.value, resultSet.getString(ROLE_NAME));
        assertEquals(AccessType.CHANGE.getAccessMask(), resultSet.getLong(ACCESS_TYPE));

        assertFalse("Database contains more data than expected", resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testRemoveAcl() throws Exception
    {
        final String objPath = "testPath";
        ObjectProtection objProt = objectProtectionFactory.getInstance(SYS_CTX, objPath, true);
        objProt.addAclEntry(SYS_CTX, SYS_CTX.subjectRole, AccessType.CHANGE);

        commit();

        objProt.delAclEntry(SYS_CTX, SYS_CTX.subjectRole);

        commit();

        @SuppressWarnings("resource")
        Connection con = getConnection();

        PreparedStatement stmt = con.prepareStatement(ACL_SELECT);
        stmt.setString(1, objPath);
        ResultSet resultSet = stmt.executeQuery();

        assertFalse("Database did not remove acl entry", resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @SuppressWarnings("checkstyle:magicnumber")
    @Test
    public void testLoadObjProtWithAcl() throws Exception
    {
        Connection con = getNewConnection();

        final String objPath = "testPath";

        PreparedStatement stmt = con.prepareStatement(OP_INSERT);
        stmt.setString(1, objPath);
        stmt.setString(2, SYS_CTX.subjectId.name.value);
        stmt.setString(3, SYS_CTX.subjectRole.name.value);
        stmt.setString(4, SYS_CTX.subjectDomain.name.value);
        stmt.executeUpdate();
        stmt.close();

        stmt = con.prepareStatement(ACL_INSERT);
        stmt.setString(1, objPath);
        stmt.setString(2, SYS_CTX.subjectRole.name.value);
        stmt.setShort(3, AccessType.CHANGE.getAccessMask());
        stmt.executeUpdate();
        stmt.close();

        con.commit();
        con.close();

        reloadSecurityObjects();
        ObjectProtection objProt = objectProtectionFactory.getInstance(SYS_CTX, objPath, false);

        assertEquals(AccessType.CHANGE, objProt.getAcl().getEntry(SYS_CTX));
    }
}
