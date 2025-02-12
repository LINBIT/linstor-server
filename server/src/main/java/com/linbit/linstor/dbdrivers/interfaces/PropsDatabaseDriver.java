package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.dbdrivers.interfaces.PropsDatabaseDriver.PropsDbEntry;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;

import java.util.Map;
import java.util.Objects;

/**
 * Database driver for {@link Node}.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface PropsDatabaseDriver extends GenericDatabaseDriver<PropsDbEntry>
{
    final class PropsDbEntry implements Comparable<PropsDbEntry>
    {
        public final String propsInstance;
        public final String propKey;
        public final String propValue;

        public PropsDbEntry(String propsInstanceRef, String propKeyRef, String propValueRef)
        {
            propsInstance = propsInstanceRef;
            propKey = propKeyRef;
            propValue = propValueRef;
        }

        @Override
        public int compareTo(PropsDbEntry oRef)
        {
            int cmp = propsInstance.compareTo(oRef.propsInstance);
            if (cmp == 0)
            {
                cmp = propKey.compareTo(oRef.propKey);
            }
            return cmp;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(propKey, propValue, propsInstance);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
            {
                return true;
            }
            if (!(obj instanceof PropsDbEntry))
            {
                return false;
            }
            PropsDbEntry other = (PropsDbEntry) obj;
            return Objects.equals(propKey, other.propKey) &&
                Objects.equals(propValue, other.propValue) &&
                Objects.equals(propsInstance, other.propsInstance);
        }
    }

    /**
     * Loads all key/value pairs for a given instance
     */
    Map<String, String> loadCachedInstance(String propsInstanceRef);

    /**
     * A special sub-driver to update the persisted value of a given instance/key pair
     */
    SingleColumnDatabaseDriver<PropsDbEntry, String> getValueDriver();
}
