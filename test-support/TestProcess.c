// Note: With gcc, compile with -std=gnu11 instead of -std=c11, otherwise it will
//       not process the necessary definitions in header files signal.h, time.h
#include <stdbool.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <time.h>
#include <errno.h>

void syntaxError(void);

const int IDX_EXIT_CODE = 1;
const int IDX_DELAY = 2;
const int IDX_SIGTERM = 3;

int main(int argc, char *argv[])
{
    int exit_code = 1;
    if (argc == 4)
    {
        bool have_exit_code = false;
        bool have_delay = false;
        bool have_sigterm = false;

        // Parse requested exit code
        char *parse_end;
        int req_exit_code = strtol(argv[1], &parse_end, 10);
        if (*parse_end == '\0')
        {
            have_exit_code = true;
        }

        // Parse delay value
        unsigned long long delay = strtoull(argv[2], &parse_end, 10);
        if (*parse_end == '\0')
        {
            have_delay = true;
        }

        // Parse exit handling
        if (strcmp("sigterm", argv[3]) == 0)
        {
            have_sigterm = true;
            // no-op, use the default SIGTERM handler
        }
        else
        if (strcmp("never", argv[3]) == 0)
        {
            have_sigterm = true;

            // block SIGTERM
            sigset_t signal_mask;
            int sig_check = sigemptyset(&signal_mask);
            sig_check |= sigaddset(&signal_mask, SIGTERM);
            sig_check |= sigprocmask(SIG_BLOCK, &signal_mask, NULL);
            if (sig_check != 0)
            {
                fputs("Warning: Blocking SIGTERM failed\n", stderr);
            }
        }

        // Delay exiting
        if (have_exit_code && have_delay && have_sigterm)
        {
            if (delay > 0)
            {
                struct timespec timer;
                struct timespec remaining_time;

                timer.tv_sec = (time_t) (delay / 1000);
                timer.tv_nsec = (long) (delay % 1000) * 1000000;

                do
                {
                    errno = 0;
                    if (nanosleep(&timer, &remaining_time) != 0)
                    {
                        timer = remaining_time;
                    }
                }
                while (errno == EINTR);
            }

            exit_code = req_exit_code;
        }
        else
        {
            syntaxError();
        }
    }
    else
    {
        syntaxError();
    }
    return exit_code;
}

void syntaxError(void)
{
    fputs("Syntax: TestProcess <exit_code> <delay>  { sigterm | never }\n", stdout);
}
