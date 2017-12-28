
.PHONY: clean

all: TestOutput TestProcess

TestOutput: TestOutput.c loremipsum.c loremipsum.h
	$(CC) -std=gnu99 -o TestOutput loremipsum.c TestOutput.c

TestProcess: TestProcess.c
	$(CC) -std=gnu99 -o TestProcess TestProcess.c

clean:
	rm -f TestOutput TestProcess
