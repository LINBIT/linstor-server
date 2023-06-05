package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.dbdrivers.interfaces.SecIdentityDatabaseDriver.SecIdentityDbObj;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.security.Identity;

import java.util.Objects;

/**
 * Database driver for Security configurations.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface SecIdentityDatabaseDriver extends GenericDatabaseDriver<SecIdentityDbObj>
{
    SingleColumnDatabaseDriver<SecIdentityDbObj, byte[]> getPassHashDriver();

    SingleColumnDatabaseDriver<SecIdentityDbObj, byte[]> getPassSaltDriver();

    SingleColumnDatabaseDriver<SecIdentityDbObj, Boolean> getIdEnabledDriver();

    SingleColumnDatabaseDriver<SecIdentityDbObj, Boolean> getIdLockedDriver();

    class SecIdentityDbObj implements Comparable<SecIdentityDbObj>
    {
        private final Identity id;
        private final byte[] passHash;
        private final byte[] passSalt;
        private final boolean enabled;
        private final boolean locked;

        public SecIdentityDbObj(
            Identity idRef,
            byte[] passHashRef,
            byte[] passSaltRef,
            boolean enabledRef,
            boolean lockedRef
        )
        {
            id = idRef;
            passHash = passHashRef;
            passSalt = passSaltRef;
            enabled = enabledRef;
            locked = lockedRef;
        }

        public Identity getIdentity()
        {
            return id;
        }

        public byte[] getPassHash()
        {
            return passHash;
        }

        public byte[] getPassSalt()
        {
            return passSalt;
        }

        public boolean isEnabled()
        {
            return enabled;
        }

        public boolean isLocked()
        {
            return locked;
        }

        @Override
        public int compareTo(SecIdentityDbObj oRef)
        {
            return id.compareTo(oRef.id);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(id);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
            {
                return true;
            }
            if (!(obj instanceof SecIdentityDbObj))
            {
                return false;
            }
            SecIdentityDbObj other = (SecIdentityDbObj) obj;
            return Objects.equals(id, other.id);
        }
    }
}
