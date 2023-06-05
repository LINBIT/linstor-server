package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.dbdrivers.ControllerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecIdentityCtrlDatabaseDriver.SecIdentityInitObj;
import com.linbit.linstor.security.Identity;

public interface SecIdentityCtrlDatabaseDriver extends SecIdentityDatabaseDriver,
    ControllerDatabaseDriver<Identity, SecIdentityInitObj, Void>
{
    class SecIdentityInitObj
    {
        private final byte[] passHash;
        private final byte[] passSalt;
        private final boolean enabled;
        private final boolean locked;

        public SecIdentityInitObj(byte[] passHashRef, byte[] passSaltRef, boolean enabledRef, boolean lockedRef)
        {
            passHash = passHashRef;
            passSalt = passSaltRef;
            enabled = enabledRef;
            locked = lockedRef;
        }

        byte[] getPassHash()
        {
            return passHash;
        }

        byte[] getPassSalt()
        {
            return passSalt;
        }

        boolean isEnabled()
        {
            return enabled;
        }

        boolean isLocked()
        {
            return locked;
        }
    }
}
