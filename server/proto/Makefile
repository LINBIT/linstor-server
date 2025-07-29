# protobuf
COMMONSRC=$(wildcard ./*.proto) $(shell find ./{common,eventdata,requests,responses} -type f -name "*.proto" )
JAVASRC=$(shell find ./{javainternal,misc} -type f -name "*.proto" )
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
