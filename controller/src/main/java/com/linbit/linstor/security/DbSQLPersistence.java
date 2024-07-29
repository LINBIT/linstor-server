package com.linbit.linstor.security;

import com.linbit.linstor.ControllerSQLDatabase;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.SecConfigDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecDefaultRoleDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecIdentityDatabaseDriver;
import com.linbit.linstor.security.pojo.IdentityRoleEntryPojo;
import com.linbit.linstor.security.pojo.SignInEntryPojo;

import static com.linbit.linstor.dbdrivers.derby.DbConstants.DOMAIN_NAME;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.IDENTITY_NAME;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.ID_ENABLED;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.ID_LOCKED;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.PASS_HASH;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.PASS_SALT;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.ROLE_NAME;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.ROLE_PRIVILEGES;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.TBL_SEC_DFLT_ROLES;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.TBL_SEC_IDENTITIES;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.TBL_SEC_ID_ROLE_MAP;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.TBL_SEC_ROLES;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
@Singleton
public class DbSQLPersistence extends BaseDbAccessor<ControllerSQLDatabase>
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

    @Inject
    public DbSQLPersistence(
        SecIdentityDatabaseDriver secIdDbDriverRef,
        SecConfigDatabaseDriver secCfgDbDriverRef,
        SecDefaultRoleDatabaseDriver secDfltRoleDriverRef
    )
    {
        super(secIdDbDriverRef, secCfgDbDriverRef, secDfltRoleDriverRef);
    }

    @FunctionalInterface
    public interface ThrowingConsumer<T, E extends SQLException>
    {
        void accept(T obj) throws E;
    }

    @Override
    public @Nullable SignInEntryPojo getSignInEntry(ControllerSQLDatabase ctrlDatabase, IdentityName idName)
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
    public @Nullable IdentityRoleEntryPojo getIdRoleMapEntry(
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
    public @Nullable IdentityRoleEntryPojo getDefaultRole(ControllerSQLDatabase ctrlDatabase, IdentityName idName)
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

    private ResultSet dbQuery(Connection dbConn, String sqlQuery, String[] arguments) throws SQLException
    {
        try (PreparedStatement stmt = dbConn.prepareStatement(sqlQuery);)
        {
            for (int idx = 0; idx < arguments.length; ++idx)
            {
                stmt.setString(idx + 1, arguments[idx]);
            }
            return stmt.executeQuery();
        }
    }
}
