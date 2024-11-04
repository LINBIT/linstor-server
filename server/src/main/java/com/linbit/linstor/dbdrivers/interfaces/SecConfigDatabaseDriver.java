package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecConfiguration;
import com.linbit.linstor.dbdrivers.interfaces.SecConfigDatabaseDriver.SecConfigDbEntry;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;

import java.util.Objects;

/**
 * Database driver for {@link SecConfiguration}.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface SecConfigDatabaseDriver extends GenericDatabaseDriver<SecConfigDbEntry>
{
    SingleColumnDatabaseDriver<SecConfigDbEntry, String> getValueDriver();

    final class SecConfigDbEntry implements Comparable<SecConfigDbEntry>
    {
        public final String key;
        public final String value;

        public SecConfigDbEntry(String propKeyRef, String propValueRef)
        {
            key = propKeyRef;
            value = propValueRef;
        }

        @Override
        public int compareTo(SecConfigDbEntry otherRef)
        {
            int cmp;
            if (otherRef == null)
            {
                cmp = 1;
            }
            else
            {
                cmp = key.compareTo(otherRef.key);
            }
            return cmp;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(key, value);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
            {
                return true;
            }
            if (!(obj instanceof SecConfigDbEntry))
            {
                return false;
            }
            SecConfigDbEntry other = (SecConfigDbEntry) obj;
            return Objects.equals(key, other.key) && Objects.equals(value, other.value);
        }
    }
}
