package com.linbit;

import com.linbit.linstor.api.ApiConsts;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class Platform
{
    private static final String UNKNOWN_OS_VARIANT = "Unknown";

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

    public static ApiConsts.Platform apiPlatform()
    {
        return isWindows() ? ApiConsts.Platform.WINDOWS : ApiConsts.Platform.LINUX;
    }

    public static String osVariant()
    {
        String variant = UNKNOWN_OS_VARIANT;

        if (isWindows())
        {
            variant = System.getProperties().getProperty("os.name", UNKNOWN_OS_VARIANT);
        }
        else
        {
            try (BufferedReader br = new BufferedReader(
                new FileReader("/etc/os-release", StandardCharsets.UTF_8)))
            {
                String line;
                while ((line = br.readLine()) != null)
                {
                    if (line.startsWith("PRETTY_NAME="))
                    {
                        int first = line.indexOf('"');
                        int last = line.lastIndexOf('"');
                        if (first > 0 && last > 0 && last > first)
                        {
                            variant = line.substring(first + 1, last);
                        }
                    }
                }
            }
            catch (IOException exp)     /* no such file, ... */
            {
                variant = UNKNOWN_OS_VARIANT + " (cannot open /etc/os-release)";
            }
        }
        return variant;
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
