package com.linbit.linstor.security;

import com.linbit.ErrorCheck;
import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.dbdrivers.DatabaseException;

import java.security.MessageDigest;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Identity authentication
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public final class Authentication
{
    public static final String HASH_ALGORITHM = "SHA-512";
    public static final int HASH_SIZE = 64;
    public static final int SALT_SIZE = 16;

    private static final AtomicBoolean GLOBAL_AUTH_REQUIRED =
        new AtomicBoolean(true);

    private Authentication(
        AccessContext initCtx,
        ControllerDatabase ctrlDbRef,
        DbAccessor dbDriverRef
    )
        throws AccessDeniedException
    {
        ErrorCheck.ctorNotNull(Authentication.class, AccessContext.class, initCtx);
        ErrorCheck.ctorNotNull(Authentication.class, ControllerDatabase.class, ctrlDbRef);
        ErrorCheck.ctorNotNull(Authentication.class, DbAccessor.class, dbDriverRef);

        initCtx.getEffectivePrivs().requirePrivileges(Privilege.PRIV_SYS_ALL);
    }

    public static boolean isRequired()
    {
        return GLOBAL_AUTH_REQUIRED.get();
    }

    public static void setRequired(
        AccessContext accCtx,
        boolean newPolicy,
        ControllerDatabase ctrlDb,
        DbAccessor secDb
    )
        throws AccessDeniedException, DatabaseException
    {
        accCtx.getEffectivePrivs().requirePrivileges(Privilege.PRIV_SYS_ALL);

        if (ctrlDb != null && secDb != null)
        {
            secDb.setAuthRequired(ctrlDb, newPolicy);
        }

        GLOBAL_AUTH_REQUIRED.set(newPolicy);
    }

    static void load(ControllerDatabase ctrlDb, DbAccessor secDb)
        throws DatabaseException
    {
        GLOBAL_AUTH_REQUIRED.set(true);

        boolean authRequired = secDb.loadAuthRequired(ctrlDb);
        if (!authRequired)
        {
            GLOBAL_AUTH_REQUIRED.set(false);
        }
    }

    /**
     * Checks whether the hash created by salting and hashing the password
     * matches the specified salted hash
     *
     * {@code password}, {@code storedSalt}, {@code storedHash} may be null.
     *
     * Upon exit from this method, the contents of any supplied {@code password},
     * {@code storedSalt} and {@code storedHash} arrays will be cleared
     * (all bytes in the array will be set to zero).
     *
     * @param dgstAlgo The digest algorithm used for hashing
     * @param password The plaintext password to check
     * @param storedSalt The salt used for hashing
     * @param storedHash The stored hash to compare the password with
     * @return True if the password matches (is correct), false otherwise
     */
    static boolean passwordMatches(
        MessageDigest dgstAlgo,
        byte[] password,
        byte[] storedSalt,
        byte[] storedHash
    )
    {
        boolean matchFlag = false;

        if (password != null && storedSalt != null && storedHash != null)
        {
            byte[] enteredPasswordHash = null;
            // Hash the password that was supplied for the signin
            synchronized (dgstAlgo)
            {
                dgstAlgo.update(storedSalt);
                enteredPasswordHash = dgstAlgo.digest(password);
            }

            if (enteredPasswordHash != null)
            {
                if (enteredPasswordHash.length == storedHash.length)
                {
                    int idx = 0;
                    while (idx < storedHash.length)
                    {
                        if (enteredPasswordHash[idx] != storedHash[idx])
                        {
                            break;
                        }
                        ++idx;
                    }
                    if (idx == storedHash.length)
                    {
                        matchFlag = true;
                    }
                }
            }
            clearDataFields(enteredPasswordHash);
        }
        clearDataFields(password, storedSalt, storedHash);

        return matchFlag;
    }

    /**
     * Clears the specified byte arrays by setting all elements to zero.
     *
     * Any element of {@code dataFieldList} may be a null reference.
     * The {@code dataFieldList} argument itself may NOT be a null reference.
     * @param dataFieldList The list of byte arrays to clear
     */
    static void clearDataFields(byte[]... dataFieldList)
    {
        for (byte[] dataField : dataFieldList)
        {
            if (dataField != null)
            {
                Arrays.fill(dataField, (byte) 0);
            }
        }
    }
}
