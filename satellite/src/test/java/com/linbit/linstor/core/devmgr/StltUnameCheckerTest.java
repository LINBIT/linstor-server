package com.linbit.linstor.core.devmgr;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.Props;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StltUnameCheckerTest
{
    private ErrorReporter errorReporter;
    private Props props;
    private Node localNode;
    private final AtomicReference<String> liveUname = new AtomicReference<>();
    private StltUnameChecker checker;

    @Before
    public void setUp()
    {
        errorReporter = Mockito.mock(ErrorReporter.class);
        props = Mockito.mock(Props.class);
        localNode = Mockito.mock(Node.class);
        when(localNode.getProps()).thenReturn(props);
        checker = new StltUnameChecker(errorReporter, liveUname::get);
    }

    private void storedUname(String uname)
    {
        when(props.getProp(InternalApiConsts.NODE_UNAME)).thenReturn(uname);
    }

    @Test
    public void noWarningIfUnchanged()
    {
        storedUname("stor-a");
        liveUname.set("stor-a");

        assertThat(checker.check(localNode)).isFalse();
        verify(errorReporter, never()).logWarning(anyString(), any(Object[].class));
    }

    @Test
    public void warnsOnceAfterRename()
    {
        storedUname("stor-a");
        liveUname.set("stor-b");

        assertThat(checker.check(localNode)).isTrue();
        assertThat(checker.check(localNode)).isTrue();
        verify(errorReporter, times(1)).logWarning(anyString(), any(Object[].class));
    }

    @Test
    public void warnsAgainOnFurtherRename()
    {
        storedUname("stor-a");
        liveUname.set("stor-b");
        checker.check(localNode);
        liveUname.set("stor-c");

        assertThat(checker.check(localNode)).isTrue();
        verify(errorReporter, times(2)).logWarning(anyString(), any(Object[].class));
    }

    @Test
    public void infoWhenMatchingAgain()
    {
        storedUname("stor-a");
        liveUname.set("stor-b");
        checker.check(localNode);
        liveUname.set("stor-a");

        assertThat(checker.check(localNode)).isFalse();
        verify(errorReporter, times(1)).logInfo(anyString(), any(Object[].class));

        // a new mismatch warns again
        liveUname.set("stor-b");
        assertThat(checker.check(localNode)).isTrue();
        verify(errorReporter, times(2)).logWarning(anyString(), any(Object[].class));
    }

    @Test
    public void ignoredIfNotDeterminable()
    {
        liveUname.set("stor-b");
        storedUname(null);
        assertThat(checker.check(localNode)).isFalse();

        storedUname("stor-a");
        liveUname.set("");
        assertThat(checker.check(localNode)).isFalse();

        verify(errorReporter, never()).logWarning(anyString(), any(Object[].class));
    }
}
