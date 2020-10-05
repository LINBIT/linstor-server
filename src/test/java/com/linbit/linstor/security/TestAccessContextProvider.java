package com.linbit.linstor.security;

import java.lang.reflect.Field;

public class TestAccessContextProvider
{
    public static final AccessContext SYS_CTX;
    public static final AccessContext INIT_CTX;
    public static final AccessContext PUBLIC_CTX;

    public static final AccessContext ALICE_ACC_CTX;
    public static final AccessContext BOB_ACC_CTX;

    static
    {
        SecurityModule securityModule = new SecurityModule();

        try
        {
            SYS_CTX = (AccessContext) getDeclaredField(SecurityModule.class, "SYSTEM_CTX", securityModule);
            PUBLIC_CTX = (AccessContext) getDeclaredField(SecurityModule.class, "PUBLIC_CTX", securityModule);

            INIT_CTX = SYS_CTX.clone();
            INIT_CTX.privEffective.enablePrivileges(Privilege.PRIVILEGE_LIST);

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

    private TestAccessContextProvider()
    {
    }
}

