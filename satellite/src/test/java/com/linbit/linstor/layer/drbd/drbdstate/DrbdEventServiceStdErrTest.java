package com.linbit.linstor.layer.drbd.drbdstate;

import com.linbit.linstor.layer.drbd.drbdstate.DrbdEventService.StdErrKind;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The satellite restarts the "drbdsetup events2" stream on stderr output, but drbd-utils also writes harmless
 * debug/info lines there (the dbg() macro of libgenl prefixes them with the level in angle brackets). Those must
 * not trigger a restart, and the empty lines the macro produces must not even be logged.
 */
public class DrbdEventServiceStdErrTest
{
    @Test
    public void emptyAndWhitespaceLinesAreEmpty()
    {
        assertThat(DrbdEventService.classifyStdErr("")).isEqualTo(StdErrKind.EMPTY);
        assertThat(DrbdEventService.classifyStdErr("\n")).isEqualTo(StdErrKind.EMPTY);
        assertThat(DrbdEventService.classifyStdErr("  \n\t")).isEqualTo(StdErrKind.EMPTY);
    }

    @Test
    public void libgenlDebugLinesAreInfo()
    {
        // the line drbd-utils' genl_connect() writes when SO_RCVBUF cannot be raised, dbg level 1
        assertThat(
            DrbdEventService.classifyStdErr(
                "<1>tried to set SO_RCVBUF 1048576, got 212992; you may need to adjust sysctl net.core.rmem_max\n"
            )
        ).isEqualTo(StdErrKind.INFO);
        // higher dbg levels use the same prefix
        assertThat(DrbdEventService.classifyStdErr("<3>bound socket to nl_pid:1234, my pid:1234\n"))
            .isEqualTo(StdErrKind.INFO);
        assertThat(DrbdEventService.classifyStdErr("<12>some future two digit level\n"))
            .isEqualTo(StdErrKind.INFO);
    }

    @Test
    public void multiLineDebugOutputIsClassifiedByItsFirstLine()
    {
        assertThat(
            DrbdEventService.classifyStdErr("<1>tried to set SO_RCVBUF 1048576, got 212992;\nsecond line\n")
        ).isEqualTo(StdErrKind.INFO);
    }

    @Test
    public void realErrorsRestartTheStream()
    {
        assertThat(DrbdEventService.classifyStdErr("Could not connect to 'drbd' generic netlink family\n"))
            .isEqualTo(StdErrKind.ERROR);
        assertThat(DrbdEventService.classifyStdErr("drbdsetup: command not found")).isEqualTo(StdErrKind.ERROR);
    }

    @Test
    public void levelPrefixOnlyCountsAtTheStart()
    {
        // an error message that merely mentions a "<1>" further in is still an error
        assertThat(DrbdEventService.classifyStdErr("failed: <1> is not a valid resource name\n"))
            .isEqualTo(StdErrKind.ERROR);
        // leading whitespace is not a dbg() line either
        assertThat(DrbdEventService.classifyStdErr(" <1>indented\n")).isEqualTo(StdErrKind.ERROR);
        // the prefix must be numeric
        assertThat(DrbdEventService.classifyStdErr("<x>not a level\n")).isEqualTo(StdErrKind.ERROR);
    }
}
