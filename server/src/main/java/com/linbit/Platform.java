package com.linbit;

public class Platform
{
    private static boolean isPlatform(String platform)
    {
        String osName = System.getProperties().getProperty("os.name");

        return osName.startsWith(platform);
    }

    public static boolean isWindows()
    {
        return isPlatform("Windows");
    }

    public static boolean isLinux()
    {
        return isPlatform("Linux");
    }

    static
    {
        if (isWindows() == isLinux())
        {
            String msg = String.format("Neither Linux nor Windows (or even stranger: both) os.name is %s",
                System.getProperties().getProperty("os.name"));

            throw new RuntimeException(msg);
        }
    }

    public static String nullDevice()
    {
        String path = null;
        if (isLinux())
        {
            path = "/dev/null";
        }
        else if (isWindows())
        {
            path = "\\\\.\\NUL";
        }
        else
        {
            throw new ImplementationError("Platform is neither Linux nor Windows, please add support for it to LINSTOR");
        }

        return path;
    }

    public static String toBlockDeviceName(String dev_or_guid)
    {
        String path = null;
        if (isLinux())
        {
            path = dev_or_guid;
        }
        else if (isWindows())
        {
            path = String.format("\\\\.\\Volume{%s}", dev_or_guid);
        }
        else
        {
            throw new ImplementationError("Platform is neither Linux nor Windows, please add support for it to LINSTOR");
        }

        return path;
    }
}
