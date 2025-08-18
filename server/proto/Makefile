# protobuf
JAVASRC=$(shell find . -type f -name "*.proto" )
JAVANOEND=$(JAVASRC:.proto=)
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
