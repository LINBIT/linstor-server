package com.linbit.linstor.security;

import com.linbit.linstor.ControllerSQLDatabase;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.pojo.IdentityRoleEntryPojo;
import com.linbit.linstor.security.pojo.SignInEntryPojo;
import com.linbit.linstor.security.pojo.TypeEnforcementRulePojo;

import static com.linbit.linstor.dbdrivers.derby.DbConstants.ACCESS_TYPE;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.DOMAIN_NAME;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.ENTRY_DSP_KEY;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.ENTRY_KEY;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.ENTRY_VALUE;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.IDENTITY_DSP_NAME;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.IDENTITY_NAME;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.ID_ENABLED;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.ID_LOCKED;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.PASS_HASH;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.PASS_SALT;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.ROLE_DSP_NAME;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.ROLE_NAME;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.ROLE_PRIVILEGES;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.TBL_SEC_CONFIGURATION;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.TBL_SEC_DFLT_ROLES;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.TBL_SEC_IDENTITIES;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.TBL_SEC_ID_ROLE_MAP;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.TBL_SEC_ROLES;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.TYPE_DSP_NAME;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.TYPE_NAME;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.VIEW_SEC_IDENTITIES_LOAD;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.VIEW_SEC_ROLES_LOAD;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.VIEW_SEC_TYPES_LOAD;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.VIEW_SEC_TYPE_RULES_LOAD;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
@Singleton
public class DbSQLPersistence implements DbAccessor<ControllerSQLDatabase>
{
    private static final String SLCT_SIGNIN_ENTRY =
        "SELECT " +
        TBL_SEC_IDENTITIES + "." + IDENTITY_NAME + ", " +
        ID_LOCKED + ", " + ID_ENABLED + ", " +
        PASS_SALT + ", " + PASS_HASH + ", " +
        TBL_SEC_DFLT_ROLES + "." + ROLE_NAME + ", " +
        TBL_SEC_ROLES + "." + DOMAIN_NAME + ", " +
        TBL_SEC_ROLES + "." + ROLE_PRIVILEGES + " " +
        "FROM " + TBL_SEC_IDENTITIES + "\n" +
        "    LEFT JOIN " + TBL_SEC_DFLT_ROLES + " ON " + TBL_SEC_IDENTITIES + "." + IDENTITY_NAME + " = " +
        TBL_SEC_DFLT_ROLES + "." + IDENTITY_NAME + "\n" +
        "    LEFT JOIN " + TBL_SEC_ROLES + " ON " + TBL_SEC_DFLT_ROLES + "." + ROLE_NAME + " = " +
        TBL_SEC_ROLES + "." + ROLE_NAME + "\n" +
        "    WHERE " + TBL_SEC_IDENTITIES + "." + IDENTITY_NAME + " = ?";

    private static final String SLCT_ID_ROLE_MAP_ENTRY =
        "SELECT " + IDENTITY_NAME + ", " + ROLE_NAME + " FROM " + TBL_SEC_ID_ROLE_MAP +
        " WHERE " + IDENTITY_NAME + " = ?" +
        " AND " + ROLE_NAME + " = ?";

    private static final String SLCT_DFLT_ROLE =
        "SELECT " + IDENTITY_NAME + ", " + ROLE_NAME + " FROM " + TBL_SEC_DFLT_ROLES +
        " WHERE " + IDENTITY_NAME + " = '?'";

    private static final String SLCT_IDENTITIES =
        "SELECT * FROM " + VIEW_SEC_IDENTITIES_LOAD;

    private static final String SLCT_SEC_TYPES =
        "SELECT * FROM " + VIEW_SEC_TYPES_LOAD;

    private static final String SLCT_ROLES =
        "SELECT * FROM " + VIEW_SEC_ROLES_LOAD;

    private static final String SLCT_TE_RULES =
        "SELECT * FROM " + VIEW_SEC_TYPE_RULES_LOAD;

    private static final String SLCT_SEC_LEVEL =
        "SELECT " + ENTRY_KEY + ", " + ENTRY_VALUE +
        " FROM " + TBL_SEC_CONFIGURATION +
        " WHERE " + ENTRY_KEY + " = '" + SecurityDbConsts.KEY_SEC_LEVEL + "'";

    private static final String SLCT_AUTH_REQ =
        "SELECT " + ENTRY_KEY + ", " + ENTRY_VALUE +
        " FROM " + TBL_SEC_CONFIGURATION +
        " WHERE " + ENTRY_KEY + " = '" + SecurityDbConsts.KEY_AUTH_REQ + "'";

    private static final String DEL_SEC_LEVEL =
        "DELETE FROM " + TBL_SEC_CONFIGURATION + " WHERE " + ENTRY_KEY + " = '" + SecurityDbConsts.KEY_SEC_LEVEL + "'";

    private static final String INS_SEC_LEVEL =
        "INSERT INTO " + TBL_SEC_CONFIGURATION + " (" + ENTRY_KEY + ", " + ENTRY_DSP_KEY + ", " + ENTRY_VALUE +
        ") VALUES('" + SecurityDbConsts.KEY_SEC_LEVEL + "', '" + SecurityDbConsts.KEY_DSP_SEC_LEVEL + "', ?)";

    private static final String DEL_AUTH_REQUIRED =
        "DELETE FROM " + TBL_SEC_CONFIGURATION + " WHERE " + ENTRY_KEY + " = '" + SecurityDbConsts.KEY_AUTH_REQ + "'";

    private static final String INS_AUTH_REQUIRED =
        "INSERT INTO " + TBL_SEC_CONFIGURATION + " (" + ENTRY_KEY + ", " + ENTRY_DSP_KEY + ", " + ENTRY_VALUE +
        ") VALUES('" + SecurityDbConsts.KEY_AUTH_REQ + "', '" + SecurityDbConsts.KEY_DSP_AUTH_REQ + "', ?)";

    @Inject
    public DbSQLPersistence()
    {
    }

    @FunctionalInterface
    public interface ThrowingConsumer<T, E extends SQLException>
    {
        void accept(T obj) throws E;
    }

    private void runWithConnection(
        ControllerSQLDatabase ctrlDatabase,
        ThrowingConsumer<Connection, SQLException> consumer
    )
        throws DatabaseException
    {
        Connection dbConn = null;
        try
        {
            dbConn = ctrlDatabase.getConnection();

            consumer.accept(dbConn);
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        finally
        {
            ctrlDatabase.returnConnection(dbConn);
        }
    }

    @Override
    public SignInEntryPojo getSignInEntry(ControllerSQLDatabase ctrlDatabase, IdentityName idName)
        throws DatabaseException
    {
        SignInEntryPojo signInEntry = null;
        Connection dbConn = null;
        try
        {
            dbConn = ctrlDatabase.getConnection();

            try (ResultSet resultSet = dbQuery(
                    dbConn,
                    SLCT_SIGNIN_ENTRY,
                    new String[]{idName.value}
                )
            )
            {
                if (resultSet.next())
                {
                    Long rolePrivileges;
                    {
                        long tmp = resultSet.getLong(ROLE_PRIVILEGES);
                        rolePrivileges = resultSet.wasNull() ? null : tmp;
                    }

                    signInEntry = new SignInEntryPojo(
                        resultSet.getString(IDENTITY_NAME),
                        resultSet.getString(ROLE_NAME),
                        resultSet.getString(DOMAIN_NAME),
                        rolePrivileges,
                        resultSet.getString(PASS_SALT),
                        resultSet.getString(PASS_HASH)
                    );
                }
            }
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        finally
        {
            ctrlDatabase.returnConnection(dbConn);
        }
        return signInEntry;
    }

    @Override
    public IdentityRoleEntryPojo getIdRoleMapEntry(
        ControllerSQLDatabase ctrlDatabase,
        IdentityName idName,
        RoleName rlName
    )
        throws DatabaseException
    {
        IdentityRoleEntryPojo identityRoleEntry = null;
        Connection dbConn = null;
        try
        {
            dbConn = ctrlDatabase.getConnection();

            try (ResultSet resultSet = dbQuery(
                    dbConn,
                    SLCT_ID_ROLE_MAP_ENTRY,
                    new String[] {idName.value, rlName.value}
                )
            )
            {
                if (resultSet.next())
                {
                    identityRoleEntry = new IdentityRoleEntryPojo(
                        resultSet.getString(IDENTITY_NAME), resultSet.getString(ROLE_NAME)
                    );
                }
            }
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        finally
        {
            ctrlDatabase.returnConnection(dbConn);
        }
        return identityRoleEntry;
    }

    @Override
    public IdentityRoleEntryPojo getDefaultRole(ControllerSQLDatabase ctrlDatabase, IdentityName idName)
        throws DatabaseException
    {
        IdentityRoleEntryPojo identityRoleEntry = null;
        Connection dbConn = null;
        try
        {
            dbConn = ctrlDatabase.getConnection();

            try (ResultSet resultSet = dbQuery(
                dbConn,
                SLCT_DFLT_ROLE,
                new String[] {idName.value}
            )
            )
            {
                if (resultSet.next())
                {
                    identityRoleEntry = new IdentityRoleEntryPojo(
                        resultSet.getString(IDENTITY_NAME),
                        resultSet.getString(ROLE_NAME)
                    );
                }
            }
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        finally
        {
            ctrlDatabase.returnConnection(dbConn);
        }
        return identityRoleEntry;
    }

    @Override
    public List<String> loadIdentities(ControllerSQLDatabase ctrlDatabase) throws DatabaseException
    {
        List<String> identities = new ArrayList<>();

        runWithConnection(ctrlDatabase, (dbConn) ->
        {
            try (ResultSet resultSet = dbQuery(
                dbConn,
                SLCT_IDENTITIES
            )
            )
            {
                while (resultSet.next())
                {
                    identities.add(resultSet.getString(IDENTITY_DSP_NAME));
                }
            }
        });

        return identities;
    }

    @Override
    public List<String> loadSecurityTypes(ControllerSQLDatabase ctrlDatabase) throws DatabaseException
    {
        List<String> securityTypes = new ArrayList<>();

        runWithConnection(ctrlDatabase, (dbConn) ->
        {
            try (ResultSet resultSet = dbQuery(
                dbConn,
                SLCT_SEC_TYPES
            )
            )
            {
                while (resultSet.next())
                {
                    securityTypes.add(resultSet.getString(TYPE_DSP_NAME));
                }
            }
        });

        return securityTypes;
    }

    @Override
    public List<String> loadRoles(ControllerSQLDatabase ctrlDatabase) throws DatabaseException
    {
        List<String> roles = new ArrayList<>();

        runWithConnection(ctrlDatabase, (dbConn) ->
        {
            try (ResultSet resultSet = dbQuery(
                dbConn,
                SLCT_ROLES
            )
            )
            {
                while (resultSet.next())
                {
                    roles.add(resultSet.getString(ROLE_DSP_NAME));
                }
            }
        });

        return roles;
    }

    @Override
    public List<TypeEnforcementRulePojo> loadTeRules(ControllerSQLDatabase ctrlDatabase) throws DatabaseException
    {
        List<TypeEnforcementRulePojo> typeEnforcementRules = new ArrayList<>();

        runWithConnection(ctrlDatabase, (dbConn) ->
        {
            try (ResultSet resultSet = dbQuery(
                dbConn,
                SLCT_TE_RULES
            )
            )
            {
                while (resultSet.next())
                {
                    typeEnforcementRules.add(new TypeEnforcementRulePojo(
                        resultSet.getString(DOMAIN_NAME),
                        resultSet.getString(TYPE_NAME),
                        resultSet.getString(ACCESS_TYPE)
                    ));
                }
            }
        });

        return typeEnforcementRules;
    }

    @Override
    public String loadSecurityLevel(ControllerSQLDatabase ctrlDatabase) throws DatabaseException
    {
        String securityLevel = null;
        Connection dbConn = null;
        try
        {
            dbConn = ctrlDatabase.getConnection();

            try (ResultSet resultSet = dbQuery(dbConn, SLCT_SEC_LEVEL))
            {
                if (resultSet.next())
                {
                    securityLevel = resultSet.getString(2);
                }
            }
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        finally
        {
            ctrlDatabase.returnConnection(dbConn);
        }
        return securityLevel;
    }

    @Override
    public boolean loadAuthRequired(ControllerSQLDatabase ctrlDatabase) throws DatabaseException
    {
        boolean authRequired = true;
        Connection dbConn = null;
        try
        {
            dbConn = ctrlDatabase.getConnection();

            try (ResultSet resultSet = dbQuery(dbConn, SLCT_AUTH_REQ))
            {
                if (resultSet.next())
                {
                    authRequired = Boolean.toString(false).equalsIgnoreCase(resultSet.getString(2));
                }
            }
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        finally
        {
            ctrlDatabase.returnConnection(dbConn);
        }
        return authRequired;
    }

    @Override
    public void setSecurityLevel(ControllerSQLDatabase ctrlDatabase, SecurityLevel newLevel) throws DatabaseException
    {
        runWithConnection(ctrlDatabase, (dbConn) ->
        {
            try
            {
                // Delete any existing security level entry
                try (Statement delStmt = dbConn.createStatement())
                {

                    delStmt.execute(DEL_SEC_LEVEL);
                }

                // Insert the new security level entry
                try (PreparedStatement insStmt = dbConn.prepareStatement(INS_SEC_LEVEL))
                {
                    insStmt.setString(1, newLevel.name().toUpperCase());
                    insStmt.execute();
                }

                dbConn.commit();
            }
            catch (SQLException sqlExc)
            {
                dbConn.rollback();
            }
        });
    }

    @Override
    public void setAuthRequired(ControllerSQLDatabase ctrlDatabase, boolean newPolicy) throws DatabaseException
    {
        runWithConnection(ctrlDatabase, (dbConn) ->
        {
            try
                (
                    Statement delStmt = dbConn.createStatement();
                    PreparedStatement insStmt = dbConn.prepareStatement(INS_AUTH_REQUIRED)
                )
            {
                // Delete any existing authentication requirement entry
                delStmt.execute(DEL_AUTH_REQUIRED);

                // Insert the new authentication requirement
                String dbValue = Boolean.toString(newPolicy);
                insStmt.setString(1, dbValue);
                insStmt.executeUpdate();

                dbConn.commit();
            }
            catch (SQLException sqlExc)
            {
                dbConn.rollback();
                throw sqlExc;
            }
        });
    }

    private ResultSet dbQuery(Connection dbConn, String sqlQuery) throws SQLException
    {
        Statement stmt = dbConn.createStatement();
        return stmt.executeQuery(sqlQuery);
    }

    private ResultSet dbQuery(Connection dbConn, String sqlQuery, String[] arguments) throws SQLException
    {
        PreparedStatement stmt = dbConn.prepareStatement(sqlQuery);
        for (int idx = 0; idx < arguments.length; ++idx)
        {
            stmt.setString(idx + 1, arguments[idx]);
        }
        return stmt.executeQuery();
    }
}
