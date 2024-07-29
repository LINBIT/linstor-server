package com.linbit.linstor.security;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.annotation.Nullable;

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
            ensureDefaultsExist();
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

    Role(RoleName roleName, PrivilegeSet limitRef)
    {
        name = roleName;
        privileges = new PrivilegeSet(limitRef);
    }

    static void ensureDefaultsExist()
    {
        Lock writeLock = GLOBAL_ROLE_MAP_LOCK.writeLock();

        try
        {
            writeLock.lock();

            if (!GLOBAL_ROLE_MAP.containsKey(SYSTEM_ROLE.name))
            {
                GLOBAL_ROLE_MAP.put(SYSTEM_ROLE.name, SYSTEM_ROLE);
            }
            if (!GLOBAL_ROLE_MAP.containsKey(PUBLIC_ROLE.name))
            {
                GLOBAL_ROLE_MAP.put(PUBLIC_ROLE.name, PUBLIC_ROLE);
            }
        }
        finally
        {
            writeLock.unlock();
        }
    }

    public static Role create(AccessContext accCtx, RoleName roleName) throws AccessDeniedException
    {
        return create(accCtx, roleName, null);
    }

    public static Role create(AccessContext accCtx, RoleName roleName, @Nullable PrivilegeSet limitRef)
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
                if (limitRef == null)
                {
                    roleObj = new Role(roleName);
                }
                else
                {
                    roleObj = new Role(roleName, limitRef);
                }
                GLOBAL_ROLE_MAP.put(roleName, roleObj);
            }
        }
        finally
        {
            writeLock.unlock();
        }
        return roleObj;
    }

    public static @Nullable Role get(RoleName rlName)
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
