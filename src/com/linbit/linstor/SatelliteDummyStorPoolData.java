package com.linbit.linstor;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import com.linbit.ImplementationError;
import com.linbit.TransactionMgr;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageDriver;
import com.linbit.linstor.storage.StorageDriverKind;
import com.linbit.linstor.storage.StorageException;

public class SatelliteDummyStorPoolData extends StorPoolData
{
    public SatelliteDummyStorPoolData()
    {
        super();
    }

    private static final Props PROPS = new DummyProps();

    private static final String EXC_MSG =
        "This is a dummy remote storPool. This instance's getter must not be called";


    @Override
    public UUID getUuid()
    {
        throw new UnsupportedOperationException(EXC_MSG);
    }

    @Override
    public StorPoolName getName()
    {
        throw new UnsupportedOperationException(EXC_MSG);
    }

    @Override
    public Node getNode()
    {
        throw new UnsupportedOperationException(EXC_MSG);
    }

    @Override
    public StorPoolDefinition getDefinition(AccessContext accCtx) throws AccessDeniedException
    {
        throw new UnsupportedOperationException(EXC_MSG);
    }

    @Override
    public StorageDriver createDriver(AccessContext accCtx, SatelliteCoreServices coreSvc)
        throws AccessDeniedException
    {
        throw new UnsupportedOperationException(EXC_MSG);
    }

    @Override
    public StorageDriverKind getDriverKind(AccessContext accCtx)
        throws AccessDeniedException
    {
        throw new UnsupportedOperationException(EXC_MSG);
    }

    @Override
    public String getDriverName()
    {
        throw new UnsupportedOperationException(EXC_MSG);
    }

    @Override
    public Props getProps(AccessContext accCtx) throws AccessDeniedException
    {
        return PROPS; // needed for querying Preferred_NIC
    }

    @Override
    public void putVolume(AccessContext accCtx, Volume volume) throws AccessDeniedException
    {
        // no-op
    }

    @Override
    public void removeVolume(AccessContext accCtx, Volume volume) throws AccessDeniedException
    {
        // no-op
    }

    @Override
    public Collection<Volume> getVolumes(AccessContext accCtx) throws AccessDeniedException
    {
        throw new UnsupportedOperationException(EXC_MSG);
    }

    @Override
    public void reconfigureStorageDriver(StorageDriver storageDriver) throws StorageException
    {
        // no-op
    }

    @Override
    public void delete(AccessContext accCtx) throws AccessDeniedException, SQLException
    {
        // no-op
    }

    private static class DummyProps implements Props
    {

        @Override
        public void initialized()
        {
            // no-op
        }

        @Override
        public boolean isInitialized()
        {
            return true;
        }

        @Override
        public void setConnection(TransactionMgr transMgr) throws ImplementationError
        {
            // no-op
        }

        @Override
        public void commit()
        {
            // no-op
        }

        @Override
        public void rollback()
        {
            // no-op
        }

        @Override
        public boolean isDirty()
        {
            return false;
        }

        @Override
        public boolean isDirtyWithoutTransMgr()
        {
            return false;
        }

        @Override
        public boolean hasTransMgr()
        {
            return false;
        }

        @Override
        public String getProp(String key) throws InvalidKeyException
        {
            return null;
        }

        @Override
        public String getProp(String key, String namespace) throws InvalidKeyException
        {
            return null;
        }

        @Override
        public String setProp(String key, String value)
            throws InvalidKeyException, InvalidValueException, AccessDeniedException, SQLException
        {
            return null;
        }

        @Override
        public String setProp(String key, String value, String namespace)
            throws InvalidKeyException, InvalidValueException, AccessDeniedException, SQLException
        {
            return null;
        }

        @Override
        public String removeProp(String key) throws InvalidKeyException, AccessDeniedException, SQLException
        {
            return null;
        }

        @Override
        public String removeProp(String key, String namespace)
            throws InvalidKeyException, AccessDeniedException, SQLException
        {
            return null;
        }

        @Override
        public void clear() throws AccessDeniedException, SQLException
        {
            // no-op
        }

        @Override
        public int size()
        {
            return 0;
        }

        @Override
        public boolean isEmpty()
        {
            return true;
        }

        @Override
        public String getPath()
        {
            return null;
        }

        @Override
        public Map<String, String> map()
        {
            return Collections.emptyMap();
        }

        @Override
        public Set<Entry<String, String>> entrySet()
        {
            return Collections.emptySet();
        }

        @Override
        public Set<String> keySet()
        {
            return Collections.emptySet();
        }

        @Override
        public Collection<String> values()
        {
            return Collections.emptySet();
        }

        @Override
        public Iterator<Entry<String, String>> iterator()
        {
            return Collections.<Entry<String, String>>emptySet().iterator();
        }

        @Override
        public Iterator<String> keysIterator()
        {
            return Collections.<String> emptySet().iterator();
        }

        @Override
        public Iterator<String> valuesIterator()
        {
            return Collections.<String> emptySet().iterator();
        }

        @Override
        public Props getNamespace(String namespace) throws InvalidKeyException
        {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Iterator<String> iterateNamespaces()
        {
            return Collections.<String> emptySet().iterator();
        }

    }
}
