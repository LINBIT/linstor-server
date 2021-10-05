package com.linbit.linstor.security;

import java.io.FileDescriptor;
import java.net.InetAddress;
import java.security.Permission;

/**
 * This DummySecurityManager does not perform any checks. Therefore it should behave exactly the same
 * as if we did not have a SecurityManger in the first place. However, there are some libraries checking
 * for existing SecruityManger and behave in a way we want to prevent in case there is no security manager.
 * Mostly needed to prevent the Nashorn engine to providing 'this.engine' to the executed JS.
 * For more information see LINBIT internal gitlab issue 675.
 */
public class DummySecurityManager extends SecurityManager
{
    @Override
    public void checkAccept(String host, int port)
    {
        // ignored
    }

    @Override
    public void checkAccess(Thread tRef)
    {
        // ignored
    }

    @Override
    public void checkAccess(ThreadGroup gRef)
    {
        // ignored
    }

    @Override
    public void checkConnect(String hostRef, int portRef)
    {
        // ignored
    }

    @Override
    public void checkConnect(String hostRef, int portRef, Object contextRef)
    {
        // ignored
    }

    @Override
    public void checkCreateClassLoader()
    {
        // ignored
    }

    @Override
    public void checkDelete(String fileRef)
    {
        // ignored
    }

    @Override
    public void checkExec(String cmdRef)
    {
        // ignored
    }

    @Override
    public void checkExit(int statusRef)
    {
        // ignored
    }

    @Override
    public void checkLink(String libRef)
    {
        // ignored
    }

    @Override
    public void checkListen(int portRef)
    {
        // ignored
    }

    @Override
    public void checkMulticast(InetAddress maddrRef)
    {
        // ignored
    }

    @Override
    public void checkPackageAccess(String pkgRef)
    {
        // ignored
    }

    @Override
    public void checkPackageDefinition(String pkgRef)
    {
        // ignored
    }

    @Override
    public void checkPermission(Permission permRef)
    {
        // ignored
    }

    @Override
    public void checkPermission(Permission permRef, Object contextRef)
    {
        // ignored
    }

    @Override
    public void checkPrintJobAccess()
    {
        // ignored
    }

    @Override
    public void checkPropertiesAccess()
    {
        // ignored
    }

    @Override
    public void checkPropertyAccess(String keyRef)
    {
        // ignored
    }

    @Override
    public void checkRead(FileDescriptor fdRef)
    {
        // ignored
    }

    @Override
    public void checkRead(String fileRef)
    {
        // ignored
    }

    @Override
    public void checkRead(String fileRef, Object contextRef)
    {
        // ignored
    }

    @Override
    public void checkSecurityAccess(String targetRef)
    {
        // ignored
    }

    @Override
    public void checkSetFactory()
    {
        // ignored
    }

    @Override
    public void checkWrite(FileDescriptor fdRef)
    {
        // ignored
    }

    @Override
    public void checkWrite(String fileRef)
    {
        // ignored
    }
}
