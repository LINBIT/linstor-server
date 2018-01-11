package com.linbit.linstor.security;

import java.lang.reflect.Field;

public class TestAccessContextProvider
{
    public static final AccessContext SYS_CTX;
    public static final AccessContext PUBLIC_CTX;

    public static final AccessContext ALICE_ACC_CTX;
    public static final AccessContext BOB_ACC_CTX;

    static
    {
        Initializer initializer = new Initializer();

        try
        {
            SYS_CTX = (AccessContext) getDeclaredField(Initializer.class, "SYSTEM_CTX", initializer);
            PUBLIC_CTX = (AccessContext) getDeclaredField(Initializer.class, "PUBLIC_CTX", initializer);

            PrivilegeSet alicePrivSet = new PrivilegeSet();
            ALICE_ACC_CTX = new AccessContext(
                new Identity(new IdentityName("Alice")),
                new Role(new RoleName("AliceRole")),
                new SecurityType(new SecTypeName("AliceSecurityType")),
                alicePrivSet
            );
            PrivilegeSet bobPrivSet = new PrivilegeSet();
            BOB_ACC_CTX = new AccessContext(
                new Identity(new IdentityName("Bob")),
                new Role(new RoleName("BobRole")),
                new SecurityType(new SecTypeName("BobSecurityType")),
                bobPrivSet
            );
        }
        catch (Exception exc)
        {
            throw new RuntimeException("Could not get system / public access context", exc);
        }
    }

    private static <T> Object getDeclaredField(Class<T> clazz, String fieldName, T obj)
        throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException
    {
        Field field = clazz.getDeclaredField(fieldName);
        boolean accessible = field.isAccessible();
        Object ret;
        if (accessible)
        {
            ret = field.get(obj);
        }
        else
        {
            field.setAccessible(true);
            ret = field.get(obj);
            field.setAccessible(accessible);
        }
        return ret;
    }
}

