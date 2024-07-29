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
 * Identity of a security subject (user...)
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public final class Identity implements Comparable<Identity>
{
    private static final Map<IdentityName, Identity> GLOBAL_IDENTITY_MAP = new TreeMap<>();
    private static final ReadWriteLock GLOBAL_IDENTITY_MAP_LOCK = new ReentrantReadWriteLock();

    // Name of this security identity
    public final IdentityName name;

    public static final Identity SYSTEM_ID;
    public static final Identity PUBLIC_ID;

    static
    {
        try
        {
            SYSTEM_ID = new Identity(new IdentityName("SYSTEM"));
            PUBLIC_ID = new Identity(new IdentityName("PUBLIC"));
            ensureDefaultsExist();
        }
        catch (InvalidNameException nameExc)
        {
            throw new ImplementationError(
                "The name constant of a builtin identity is invalid",
                nameExc
            );
        }
    }

    Identity(IdentityName idName)
    {
        name = idName;
    }

    public static Identity create(AccessContext accCtx, IdentityName idName)
        throws AccessDeniedException
    {
        accCtx.privEffective.requirePrivileges(Privilege.PRIV_SYS_ALL);

        Lock writeLock = GLOBAL_IDENTITY_MAP_LOCK.writeLock();

        Identity idObj;
        try
        {
            writeLock.lock();
            idObj = GLOBAL_IDENTITY_MAP.get(idName);
            if (idObj == null)
            {
                idObj = new Identity(idName);
                GLOBAL_IDENTITY_MAP.put(idName, idObj);
            }
        }
        finally
        {
            writeLock.unlock();
        }
        return idObj;
    }

    public static @Nullable Identity get(IdentityName idName)
    {
        Lock readLock = GLOBAL_IDENTITY_MAP_LOCK.readLock();

        Identity idObj;
        try
        {
            readLock.lock();
            idObj = GLOBAL_IDENTITY_MAP.get(idName);
        }
        finally
        {
            readLock.unlock();
        }
        return idObj;
    }

    public static Set<Identity> getAll()
    {
        Lock readLock = GLOBAL_IDENTITY_MAP_LOCK.readLock();

        Set<Identity> result = new TreeSet<>();
        try
        {
            readLock.lock();
            result.addAll(GLOBAL_IDENTITY_MAP.values());
        }
        finally
        {
            readLock.unlock();
        }
        return result;
    }

    public static int getIdentityCount()
    {
        Lock readLock = GLOBAL_IDENTITY_MAP_LOCK.readLock();

        int count = 0;
        try
        {
            readLock.lock();
            count = GLOBAL_IDENTITY_MAP.size();
        }
        finally
        {
            readLock.unlock();
        }
        return count;
    }

    static void ensureDefaultsExist()
    {
        Lock writeLock = GLOBAL_IDENTITY_MAP_LOCK.writeLock();

        try
        {
            writeLock.lock();

            if (!GLOBAL_IDENTITY_MAP.containsKey(SYSTEM_ID.name))
            {
                GLOBAL_IDENTITY_MAP.put(SYSTEM_ID.name, SYSTEM_ID);
            }
            if (!GLOBAL_IDENTITY_MAP.containsKey(PUBLIC_ID.name))
            {
                GLOBAL_IDENTITY_MAP.put(PUBLIC_ID.name, PUBLIC_ID);
            }
        }
        finally
        {
            writeLock.unlock();
        }
    }

    @Override
    public String toString()
    {
        return name.displayValue;
    }

    @Override
    public int compareTo(Identity other)
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
        if (other != null && other instanceof Identity)
        {
            equals = this.name.equals(((Identity) other).name);
        }
        return equals;
    }
}
