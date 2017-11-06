SRC=$(wildcard *.proto)
NOEND=$(SRC:.proto=)

JAVAOUT=./
JAVASUFF=OuterClass.java
JAVAS=$(patsubst %,$(JAVAOUT)/%$(JAVASUFF),$(NOEND))

# make java the default one
all: java

%.proto:
	;

$(JAVAOUT)/%$(JAVASUFF): %.proto
#	echo $@ $<
	protoc -I=. --java_out=../../../../.. $<
#assumes this makefile is in the "com/linbit/drdbamange/proto/internal" folder

java: $(JAVAS) javacommon

javacommon:
	make -C ../../../../../../linstor-common java

cleanjava:
	rm -f $(JAVAS)

clean: cleanjava
