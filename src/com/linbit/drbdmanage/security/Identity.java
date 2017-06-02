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

    static final Identity SYSTEM_ID;
    static final Identity PUBLIC_ID;

    static
    {
        try
        {
            SYSTEM_ID = new Identity(new IdentityName("SYSTEM"));
            PUBLIC_ID = new Identity(new IdentityName("PUBLIC"));
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

    public static Identity get(IdentityName idName)
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

        Lock writeLock = GLOBAL_IDENTITY_MAP_LOCK.writeLock();

        try
        {
            writeLock.lock();
            GLOBAL_IDENTITY_MAP.clear();

            GLOBAL_IDENTITY_MAP.put(SYSTEM_ID.name, SYSTEM_ID);
            GLOBAL_IDENTITY_MAP.put(PUBLIC_ID.name, PUBLIC_ID);

            ResultSet loadData = secDb.loadIdentities(dbConn);
            while (loadData.next())
            {
                String name = loadData.getString(SecurityDbFields.IDENTITY_DSP_NAME);
                IdentityName idName = new IdentityName(name);
                if (!idName.equals(SYSTEM_ID.name) &&
                    !idName.equals(PUBLIC_ID.name))
                {
                    Identity secId = new Identity(idName);
                    GLOBAL_IDENTITY_MAP.put(idName, secId);
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
