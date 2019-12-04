# protobuf
COMMONSRC=$(wildcard ./*.proto)  $(wildcard ./common/*.proto) $(wildcard ./eventdata/*.proto)  $(wildcard ./requests/*.proto) $(wildcard ./responses/*.proto)
JAVASRC=$(wildcard ./javainternal/*.proto) $(wildcard ./javainternal/c2s/*.proto) $(wildcard ./javainternal/s2c/*.proto)
COMMONNOEND=$(COMMONSRC:.proto=)
JAVANOEND=$(COMMONNOEND) $(JAVASRC:.proto=)
JAVABASEOUT=../generated-src
JAVAOUT=$(JAVABASEOUT)/com/linbit
JAVASUFF=OuterClass.java
JAVAS=$(patsubst %,$(JAVAOUT)/%$(JAVASUFF),$(JAVANOEND))
PROTOC=protoc
PROTOC_JAVA_OPT=

all: proto

$(JAVABASEOUT):
	mkdir $@

$(JAVAOUT)/%$(JAVASUFF): %.proto $(JAVABASEOUT)
	${PROTOC} -I=. --java_out=$(PROTOC_JAVA_OPT)$(JAVABASEOUT) $<

proto: $(JAVAS)

clean-proto:
	rm -Rf $(JAVABASEOUT)
