
.PHONY: clean

all: TestOutput TestProcess

TestOutput: TestOutput.c loremipsum.c loremipsum.h
	$(CC) -o TestOutput loremipsum.c TestOutput.c

TestProcess: TestProcess.c
	$(CC) -std=gnu11 -o TestProcess TestProcess.c

clean:
	rm -f TestOutput TestProcess
