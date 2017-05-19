package com.linbit.drbdmanage.security;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.drbdmanage.ControllerDatabase;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Security role
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public final class Role implements Comparable<Role>
{
    private static final Map<RoleName, Role> GLOBAL_ROLE_MAP = new TreeMap<>();

    // Name of this security role
    public final RoleName name;

    // Set of privileges assigned to this role
    public final PrivilegeSet privileges;

    static final Role SYSTEM_ROLE;
    static final Role PUBLIC_ROLE;

    static
    {
        try
        {
            SYSTEM_ROLE = new Role(new RoleName("SYSTEM"));
            PUBLIC_ROLE = new Role(new RoleName("PUBLIC"));
        }
        catch (InvalidNameException nameExc)
        {
            throw new ImplementationError(
                "The name constant of a builtin role is invalid",
                nameExc
            );
        }
    }

    Role(RoleName roleName)
    {
        name = roleName;
        privileges = new PrivilegeSet();
    }

    @Override
    public int compareTo(Role other)
    {
        return this.name.compareTo(other.name);
    }

    @Override
    public int hashCode()
    {
       return this.name.hashCode();
    }

    @Override
    public boolean equals(Object other)
    {
        boolean equals = false;
        if (other != null && other instanceof Role)
        {
            equals = this.name.equals(((Role) other).name);
        }
        return equals;
    }

    static void load(ControllerDatabase ctrlDb, DbAccessor secDb)
        throws SQLException, InvalidNameException
    {
        Connection dbConn = ctrlDb.getConnection();
        if (dbConn == null)
        {
            throw new SQLException(
                "The controller database connection pool failed to provide a database connection"
            );
        }
        try
        {
            GLOBAL_ROLE_MAP.clear();

            GLOBAL_ROLE_MAP.put(SYSTEM_ROLE.name, SYSTEM_ROLE);
            GLOBAL_ROLE_MAP.put(PUBLIC_ROLE.name, PUBLIC_ROLE);

            ResultSet loadData = secDb.loadRoles(dbConn);
            while (loadData.next())
            {
                String name = loadData.getString(SecurityDbFields.ROLE_DSP_NAME);
                RoleName rlName = new RoleName(name);
                if (!rlName.equals(SYSTEM_ROLE.name) &&
                    !rlName.equals(PUBLIC_ROLE.name))
                {
                    Role secRole = new Role(rlName);
                    GLOBAL_ROLE_MAP.put(rlName, secRole);
                }
            }
        }
        finally
        {
            ctrlDb.returnConnection(dbConn);
        }
    }

    public static Role get(RoleName rlName)
    {
        return GLOBAL_ROLE_MAP.get(rlName);
    }

    public static Set<Role> getAll()
    {
        Set<Role> result = new TreeSet<>();
        result.addAll(GLOBAL_ROLE_MAP.values());
        return result;
    }

    // FIXME: Replace constructor with static create() method
    public Role(AccessContext accCtx, RoleName roleName)
        throws AccessDeniedException
    {
        this(roleName);
        accCtx.getEffectivePrivs().requirePrivileges(Privilege.PRIV_SYS_ALL);
    }

    @Override
    public String toString()
    {
        return name.displayValue;
    }
}
