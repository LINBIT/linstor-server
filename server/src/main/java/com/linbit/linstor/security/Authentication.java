package com.linbit.linstor.security;

import com.linbit.ErrorCheck;
import com.linbit.ImplementationError;
import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.ControllerSQLDatabase;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.utils.UnicodeConversion;
import java.security.spec.InvalidKeySpecException;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

/**
 * Identity authentication
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public final class Authentication
{
    public static final String HASH_ALGORITHM = "SHA-512";
    public static final int ITERATIONS  = 5000;
    public static final int HASH_SIZE   = 512;
    public static final int SALT_SIZE   = 16;

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

    public void createSignInEntry(
        AccessContext           accCtx,
        ControllerSQLDatabase   ctrlDb,
        IdentityName            idName,
        RoleName                dfltRlName,
        SecTypeName             dmnName,
        byte[]                  password
    )
        throws DatabaseException, AccessDeniedException
    {
        final PrivilegeSet effPriv = accCtx.getEffectivePrivs();
        effPriv.requirePrivileges(Privilege.PRIV_SYS_ALL);

        // TODO: Create a salt
        // TODO: Create the password hash
        // TODO: Check for duplicate sign-in entry
        // TODO: Call persistence layer to create the sign-in entry
        // TODO: Cleanup byte arrays
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
        SecretKeyFactory keyFact,
        byte[] password,
        byte[] storedSalt,
        byte[] storedHash
    )
        throws SignInException
    {
        boolean matchFlag = false;

        byte[] enteredPasswordHash = null;
        try
        {
            if (storedHash != null)
            {
                final SecretKey derivedKey = getPasswordHash(keyFact, password, storedSalt);
                enteredPasswordHash = derivedKey.getEncoded();

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
            }
        }
        finally
        {
            clearDataFields(enteredPasswordHash);
            clearDataFields(storedHash);
        }

        return matchFlag;
    }

    static SecretKey getPasswordHash(
        SecretKeyFactory keyFact,
        byte[] password,
        byte[] salt
    )
        throws SignInException
    {
        final SecretKey derivedKey;
        try
        {
            if (keyFact != null && password != null && salt != null)
            {
                final char[] passwordChars;
                try
                {
                    passwordChars = UnicodeConversion.utf8BytesToUtf16Chars(password, true);
                }
                catch (UnicodeConversion.InvalidSequenceException exc)
                {
                    throw new SignInException("The password contains a byte sequence that is not a " +
                                              "valid UTF-8 sequence");
                }
                PBEKeySpec keySpec = new PBEKeySpec(passwordChars, salt, ITERATIONS, HASH_SIZE);

                synchronized (keyFact)
                {
                    try
                    {
                        derivedKey = keyFact.generateSecret(keySpec);
                    }
                    catch (InvalidKeySpecException exc)
                    {
                        throw new ImplementationError(
                            "The PBKDF2 key derivation generated an InvalidKeySpecException", exc
                        );
                    }
                }
            }
            else
            {
                throw new ImplementationError(
                    Authentication.class.getSimpleName() + " method getPasswordHash called " +
                    "with a null pointer argument"
                );
            }
        }
        finally
        {
            clearDataFields(password, salt);
        }
        return derivedKey;
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

    /**
     * Clears the specified char arrays by setting all elements to zero.
     *
     * Any element of {@code dataFieldList} may be a null reference.
     * The {@code dataFieldList} argument itself may NOT be a null reference.
     * @param dataFieldList The list of byte arrays to clear
     */
    static void clearDataFields(char[]... dataFieldList)
    {
        for (char[] dataField : dataFieldList)
        {
            if (dataField != null)
            {
                Arrays.fill(dataField, (char) 0);
            }
        }
    }
}
