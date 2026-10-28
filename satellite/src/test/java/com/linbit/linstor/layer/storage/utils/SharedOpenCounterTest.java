package com.linbit.linstor.layer.storage.utils;

import com.linbit.ImplementationError;
import com.linbit.linstor.storage.StorageException;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SharedOpenCounterTest
{
    private final SharedOpenCounter<String, String> counter = new SharedOpenCounter<>();
    private final List<String> calls = new ArrayList<>();

    @Test
    public void onlyFirstOpenAndLastCloseRun() throws Exception
    {
        counter.open("lv", () -> record("open"));
        counter.open("lv", () -> record("second open"));
        counter.close("lv", state -> record("close " + state));
        assertThat(calls).containsExactly("open");

        counter.close("lv", state -> record("last close " + state));
        assertThat(calls).containsExactly("open", "last close open");
    }

    @Test
    public void keysAreCountedIndependently() throws Exception
    {
        counter.open("a", () -> record("open a"));
        counter.open("b", () -> record("open b"));
        counter.close("a", state -> record("close a"));

        assertThat(calls).containsExactly("open a", "open b", "close a");
    }

    @Test
    public void failedFirstOpenIsNotCounted() throws Exception
    {
        StorageException failure = new StorageException("activation failed");
        assertThatThrownBy(() -> counter.open("lv", () -> { throw failure; })).isSameAs(failure);

        // the next open is the first one again
        counter.open("lv", () -> record("open"));
        assertThat(calls).containsExactly("open");
    }

    @Test
    public void keyIsClosedEvenIfLastCloseFails() throws Exception
    {
        counter.open("lv", () -> record("open"));
        StorageException failure = new StorageException("deactivation failed");
        assertThatThrownBy(() -> counter.close("lv", state -> { throw failure; })).isSameAs(failure);

        counter.open("lv", () -> record("reopen"));
        assertThat(calls).containsExactly("open", "reopen");
    }

    @Test
    public void closingUnopenedKeyIsAnImplementationError()
    {
        assertThatThrownBy(() -> counter.close("lv", state -> record("close")))
            .isInstanceOf(ImplementationError.class);
        assertThat(calls).isEmpty();
    }

    private String record(String call)
    {
        calls.add(call);
        return call;
    }
}
