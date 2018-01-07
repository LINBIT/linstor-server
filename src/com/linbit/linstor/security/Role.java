package com.linbit.linstor.security;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.ControllerDatabase;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Security role
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public final class Role implements Comparable<Role>
{
    private static final Map<RoleName, Role> GLOBAL_ROLE_MAP = new TreeMap<>();
    private static final ReadWriteLock GLOBAL_ROLE_MAP_LOCK = new ReentrantReadWriteLock();

    // Name of this security role
    public final RoleName name;

    // Set of privileges assigned to this role
    public final PrivilegeSet privileges;

    public static final Role SYSTEM_ROLE;
    public static final Role PUBLIC_ROLE;

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

    public static Role create(AccessContext accCtx, RoleName roleName)
        throws AccessDeniedException
    {
        accCtx.privEffective.requirePrivileges(Privilege.PRIV_SYS_ALL);

        Lock writeLock = GLOBAL_ROLE_MAP_LOCK.writeLock();

        Role roleObj;
        try
        {
            writeLock.lock();
            roleObj = GLOBAL_ROLE_MAP.get(roleName);
            if (roleObj == null)
            {
                roleObj = new Role(roleName);
                GLOBAL_ROLE_MAP.put(roleName, roleObj);
            }
        }
        finally
        {
            writeLock.unlock();
        }
        return roleObj;
    }

    public static Role get(RoleName rlName)
    {
        Lock readLock = GLOBAL_ROLE_MAP_LOCK.readLock();

        Role roleObj;
        try
        {
            readLock.lock();
            roleObj = GLOBAL_ROLE_MAP.get(rlName);
        }
        finally
        {
            readLock.unlock();
        }
        return roleObj;
    }

    public static Set<Role> getAll()
    {
        Lock readLock = GLOBAL_ROLE_MAP_LOCK.readLock();

        Set<Role> result = new TreeSet<>();
        try
        {
            readLock.lock();
            result.addAll(GLOBAL_ROLE_MAP.values());
        }
        finally
        {
            readLock.unlock();
        }
        return result;
    }

    public static int getRoleCount()
    {
        int count = 0;

        Lock readLock = GLOBAL_ROLE_MAP_LOCK.readLock();
        try
        {
            readLock.lock();
            count = GLOBAL_ROLE_MAP.size();
        }
        finally
        {
            readLock.unlock();
        }
        return count;
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

        Lock writeLock = GLOBAL_ROLE_MAP_LOCK.writeLock();

        try
        {
            writeLock.lock();
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
            writeLock.unlock();
            ctrlDb.returnConnection(dbConn);
        }
    }

    @Override
    public String toString()
    {
        return name.displayValue;
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
}
