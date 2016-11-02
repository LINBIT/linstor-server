#include <stdbool.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <loremipsum.h>

const int IDX_EXIT_CODE = 1;
const int IDX_LENGTH = 2;
const int IDX_OUTPUT = 3;
const int IDX_HANG = 4;

const size_t BUFFER_SIZE = 4096;

void syntaxError(void);

int main(int argc, char *argv[])
{
    int exit_code = 1;
    if (argc == 5)
    {
        bool have_exit_code = false;
        bool have_length = false;
        bool have_output = false;
        bool have_exit = false;

        bool exit_flag = true;

        FILE *output = stdout;

        // Parse requested exit code
        char *parse_end;
        int req_exit_code = strtol(argv[1], &parse_end, 10);
        if (*parse_end == '\0')
        {
            have_exit_code = true;
        }

        // Parse length value
        unsigned long long length = strtoull(argv[2], &parse_end, 10);
        if (*parse_end == '\0')
        {
            have_length = true;
        }

        // Parse output
        if (strcmp("stdout", argv[3]) == 0)

        {
            have_output = true;
        }
        else
        if (strcmp("stderr", argv[3]) == 0)
        {
            have_output = true;
            output = stderr;
        }

        // Parse exit handling
        if (strcmp("exit", argv[4]) == 0)
        {
            have_exit = true;
        }
        else
        if (strcmp("hang", argv[4]) == 0)
        {
            have_exit = true;
            exit_flag = false;
        }

        // Write output
        if (have_exit_code && have_length && have_output && have_exit)
        {
            if (length > 0)
            {
                char *buffer = malloc(BUFFER_SIZE);
                if (buffer != NULL)
                {
                    (void) memset(buffer, ' ', BUFFER_SIZE);
                    (void) strncpy(buffer, LOREM_IPSUM, BUFFER_SIZE);
                    size_t blocks = length / BUFFER_SIZE;
                    size_t fraction = length % BUFFER_SIZE;
                    for (size_t block_idx = 0; block_idx < blocks; ++block_idx)
                    {
                        fwrite(buffer, 1, BUFFER_SIZE, output);
                        fflush(output);
                    }
                    if (fraction > 0)
                    {
                        fwrite(buffer, 1, fraction, output);
                        fflush(output);
                    }
                    exit_code = req_exit_code;
                }
                else
                {
                    fputs("Error: Out of memory\n", stderr);
                }
            }
        }
        else
        {
            syntaxError();
        }

        // Exit or hang
        if (!exit_flag)
        {
            int pipefd[2];
            if (pipe(pipefd) == 0)
            {
                char in_char;
                while (true)
                {
                    (void) read(pipefd[0], &in_char, 1);
                }
            }
            else
            {
                fputs("Error: Pipe creation failed\n", stderr);
            }
        }
    }
    else
    {
        syntaxError();
    }
    return exit_code;
    return (EXIT_SUCCESS);
}

void syntaxError(void)
{
    fputs("Syntax: TestOutput <exit_code> <length> { stdout | stderr } { exit | hang }\n", stdout);
}
