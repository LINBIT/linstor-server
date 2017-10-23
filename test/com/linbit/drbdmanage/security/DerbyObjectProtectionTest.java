package com.linbit.drbdmanage.security;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.derby.shared.common.error.DerbySQLIntegrityConstraintViolationException;
import org.junit.Test;

import com.linbit.InvalidNameException;
import com.linbit.TransactionMgr;

public class DerbyObjectProtectionTest extends DerbyBase
{
    private static final String OP_SELECT =
        "SELECT * FROM " + TBL_SEC_OBJECT_PROTECTION +
        " WHERE " + OBJECT_PATH + " <> '/storpooldefinitions/DFLTSTORPOOL'";
    private static final String OP_INSERT =
        "INSERT INTO " + TBL_SEC_OBJECT_PROTECTION + " VALUES (?, ?, ?, ?)";

    private static final String ACL_SELECT =
        "SELECT * FROM " + TBL_SEC_ACL_MAP +
        " WHERE " + OBJECT_PATH + " <> '/storpooldefinitions/DFLTSTORPOOL'";
    private static final String ACL_SELECT_BY_OBJPATH_AND_ROLE =
        "SELECT * FROM " + TBL_SEC_ACL_MAP +
        " WHERE " + OBJECT_PATH + " = ? AND " +
                    ROLE_NAME   + " = ?";
    private static final String ACL_INSERT =
        "INSERT INTO " + TBL_SEC_ACL_MAP + " VALUES (?, ?, ?)";

    private static final String ROLES_INSERT =
        "INSERT INTO " + TBL_SEC_ROLES + " VALUES (?, ?, ?, ?, ?)";

    @Test
    public void testCreateSimpleObjProt() throws SQLException, AccessDeniedException
    {
        @SuppressWarnings("resource")
        final Connection con = getConnection();
        final TransactionMgr transMgr = new TransactionMgr(con);

        final String objPath = "testPath";
        ObjectProtection.getInstance(sysCtx, objPath, true, transMgr);

        final PreparedStatement stmt = con.prepareStatement(OP_SELECT);
        final ResultSet resultSet = stmt.executeQuery();

        assertTrue("Database did not persist objectProtection instance", resultSet.next());

        assertEquals(objPath, resultSet.getString(OBJECT_PATH));
        assertEquals(sysCtx.subjectId.name.value, resultSet.getString(CREATOR_IDENTITY_NAME));
        assertEquals(sysCtx.subjectRole.name.value, resultSet.getString(OWNER_ROLE_NAME));
        assertEquals(sysCtx.subjectDomain.name.value, resultSet.getString(SECURITY_TYPE_NAME));

        assertFalse("Database contains more data than expected", resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test (expected = DerbySQLIntegrityConstraintViolationException.class)
    public void testCreateUnknownIdObjProt() throws Exception
    {
        @SuppressWarnings("resource")
        final Connection con = getConnection();
        final TransactionMgr transMgr = new TransactionMgr(con);

        final String objPath = "testPath";

        AccessContext accCtx = new AccessContext(
            new Identity(new IdentityName("UNKNOWN")),
            sysCtx.subjectRole,
            sysCtx.subjectDomain,
            sysCtx.privEffective
        );
        ObjectProtection.getInstance(accCtx, objPath, true, transMgr);
        fail("Creating an ObjectProtection with an unknown identity should have failed");
    }

    @Test (expected = DerbySQLIntegrityConstraintViolationException.class)
    public void testCreateUnknownRoleObjProt() throws Exception
    {
        @SuppressWarnings("resource")
        final Connection con = getConnection();
        final TransactionMgr transMgr = new TransactionMgr(con);

        final String objPath = "testPath";

        AccessContext accCtx = new AccessContext(
            sysCtx.subjectId,
            new Role(new RoleName("UNKNOWN")),
            sysCtx.subjectDomain,
            sysCtx.privEffective
        );
        ObjectProtection.getInstance(accCtx, objPath, true, transMgr);
        fail("Creating an ObjectProtection with an unknown role should have failed");
    }

    @Test (expected = DerbySQLIntegrityConstraintViolationException.class)
    public void testCreateUnknownSecTypeObjProt() throws Exception
    {
        @SuppressWarnings("resource")
        final Connection con = getConnection();
        final TransactionMgr transMgr = new TransactionMgr(con);

        final String objPath = "testPath";

        AccessContext accCtx = new AccessContext(
            sysCtx.subjectId,
            sysCtx.subjectRole,
            new SecurityType(new SecTypeName("UNKNOWN")),
            sysCtx.privEffective
        );
        ObjectProtection.getInstance(accCtx, objPath, true, transMgr);
        fail("Creating an ObjectProtection with an unknown identity should have failed");
    }

    @SuppressWarnings("resource")
    @Test
    public void testLoadSimpleObjProt() throws SQLException, AccessDeniedException
    {
        Connection con = getConnection();
        PreparedStatement stmt = con.prepareStatement(OP_INSERT);
        final String objPath = "testPath";
        stmt.setString(1, objPath);
        stmt.setString(2, sysCtx.subjectId.name.value);
        stmt.setString(3, sysCtx.subjectRole.name.value);
        stmt.setString(4, sysCtx.subjectDomain.name.value);
        stmt.executeUpdate();
        stmt.close();

        con.commit();
        con.close();

        con = getConnection();
        TransactionMgr transMgr = new TransactionMgr(con);
        ObjectProtection objProt = ObjectProtection.getInstance(sysCtx, objPath, false, transMgr);

        assertEquals(sysCtx.subjectId, objProt.getCreator());
        assertEquals(sysCtx.subjectRole, objProt.getOwner());
        assertEquals(sysCtx.subjectDomain, objProt.getSecurityType());
    }


    @Test
    public void testAddAcl() throws SQLException, AccessDeniedException, InvalidNameException
    {
        @SuppressWarnings("resource")
        Connection con = getConnection();
        TransactionMgr transMgr = new TransactionMgr(con);

        final String objPath = "testPath";

        ObjectProtection objProt = ObjectProtection.getInstance(sysCtx, objPath, true, transMgr);

        Role testRole = Role.create(sysCtx, new RoleName("test"));

        PreparedStatement stmt = con.prepareStatement(ROLES_INSERT);
        stmt.setString(1, testRole.name.value);
        stmt.setString(2, testRole.name.displayValue);
        stmt.setString(3, sysCtx.subjectDomain.name.value);
        stmt.setBoolean(4, true);
        stmt.setLong(5, Privilege.PRIV_SYS_ALL.id);
        stmt.executeUpdate();
        stmt.close();

        objProt.addAclEntry(sysCtx, testRole, AccessType.CHANGE);
        transMgr.commit();

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
        @SuppressWarnings("resource")
        Connection con = getConnection();
        TransactionMgr transMgr = new TransactionMgr(con);

        final String objPath = "testPath";
        ObjectProtection objProt = ObjectProtection.getInstance(sysCtx, objPath, true, transMgr);
        objProt.addAclEntry(sysCtx, sysCtx.subjectRole, AccessType.CHANGE);

        objProt.delAclEntry(sysCtx, sysCtx.subjectRole);

        PreparedStatement stmt = con.prepareStatement(ACL_SELECT);
        ResultSet resultSet = stmt.executeQuery();

        assertFalse("Database did not remove acl entry", resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @SuppressWarnings("resource")
    @Test
    public void testLoadObjProtWithAcl() throws Exception
    {
        Connection con = getConnection();

        final String objPath = "testPath";

        PreparedStatement stmt = con.prepareStatement(OP_INSERT);
        stmt.setString(1, objPath);
        stmt.setString(2, sysCtx.subjectId.name.value);
        stmt.setString(3, sysCtx.subjectRole.name.value);
        stmt.setString(4, sysCtx.subjectDomain.name.value);
        stmt.executeUpdate();
        stmt.close();

        stmt = con.prepareStatement(ACL_INSERT);
        stmt.setString(1, objPath);
        stmt.setString(2, sysCtx.subjectRole.name.value);
        stmt.setShort(3, AccessType.CHANGE.getAccessMask());
        stmt.executeUpdate();
        stmt.close();

        con.commit();
        con.close();

        con = getConnection();
        TransactionMgr transMgr = new TransactionMgr(con);
        ObjectProtection objProt = ObjectProtection.getInstance(sysCtx, objPath, false, transMgr);

        assertEquals(AccessType.CHANGE, objProt.getAcl().getEntry(sysCtx));
    }
}
