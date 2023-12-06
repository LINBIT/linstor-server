package com.linbit.linstor.api.pojo;

@Deprecated(forRemoval = true)
public class ExosDefaultsPojo
{
    private final String username;
    private final String usernameEnv;
    private final String password;
    private final String passwordEnv;

    public ExosDefaultsPojo(String usernameRef, String usernameEnvRef, String passwordRef, String passwordEnvRef)
    {
        username = usernameRef;
        usernameEnv = usernameEnvRef;
        password = passwordRef;
        passwordEnv = passwordEnvRef;
    }

    public String getUsername()
    {
        return username;
    }

    public String getUsernameEnv()
    {
        return usernameEnv;
    }

    public String getPassword()
    {
        return password;
    }

    public String getPasswordEnv()
    {
        return passwordEnv;
    }
}
