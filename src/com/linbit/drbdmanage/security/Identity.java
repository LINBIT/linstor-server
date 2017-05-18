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
 * Identity of a security subject (user...)
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public final class Identity implements Comparable<Identity>
{
    private static final Map<IdentityName, Identity> GLOBAL_IDENTITY_MAP = new TreeMap<>();

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
        return this.name.equals(other);
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
            ctrlDb.returnConnection(dbConn);
        }
    }

    public static Identity get(IdentityName idName)
    {
        return GLOBAL_IDENTITY_MAP.get(idName);
    }

    public static Set<Identity> getAll()
    {
        Set<Identity> result = new TreeSet<>();
        result.addAll(GLOBAL_IDENTITY_MAP.values());
        return result;
    }

    // FIXME: Replace constructor with static create() method
    public Identity(AccessContext accCtx, IdentityName idName)
        throws AccessDeniedException
    {
        this(idName);
        accCtx.getEffectivePrivs().requirePrivileges(Privilege.PRIV_SYS_ALL);
    }

    @Override
    public String toString()
    {
        return name.displayValue;
    }
}
