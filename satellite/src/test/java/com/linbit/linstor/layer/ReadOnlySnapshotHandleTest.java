package com.linbit.linstor.layer;

import com.linbit.linstor.storage.StorageException;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ReadOnlySnapshotHandleTest
{
    @Test
    public void closeOnceClosesOnlyOnce() throws Exception
    {
        ReadOnlySnapshotHandle inner = mock(ReadOnlySnapshotHandle.class);
        ReadOnlySnapshotHandle handle = ReadOnlySnapshotHandle.closeOnce(inner);

        handle.close();
        handle.close();

        verify(inner).close();
    }

    @Test
    public void closeAfterFailureAddsCloseFailureAsSuppressed() throws Exception
    {
        StorageException closeFailure = new StorageException("close failed");
        ReadOnlySnapshotHandle handle = () ->
        {
            throw closeFailure;
        };
        StorageException openFailure = new StorageException("open failed");

        handle.closeAfterFailure(openFailure);

        assertThat(openFailure.getSuppressed()).containsExactly(closeFailure);
    }
}
