# protobuf
COMMONSRC=$(wildcard linstor/proto/*.proto)  $(wildcard linstor/proto/common/*.proto) $(wildcard linstor/proto/eventdata/*.proto)  $(wildcard linstor/proto/requests/*.proto) $(wildcard linstor/proto/responses/*.proto)
JAVASRC=$(wildcard linstor/proto/javainternal/*.proto) $(wildcard linstor/proto/javainternal/c2s/*.proto) $(wildcard linstor/proto/javainternal/s2c/*.proto)
COMMONNOEND=$(COMMONSRC:.proto=)
JAVANOEND=$(COMMONNOEND) $(JAVASRC:.proto=)
JAVABASEOUT=../generated-src
JAVAOUT=$(JAVABASEOUT)/com/linbit
JAVASUFF=OuterClass.java
JAVAS=$(patsubst %,$(JAVAOUT)/%$(JAVASUFF),$(JAVANOEND))
PROTOC=protoc
PROTOC_JAVA_OPT=lite:

all: proto

$(JAVABASEOUT):
	mkdir $@

$(JAVAOUT)/%$(JAVASUFF): %.proto $(JAVABASEOUT)
	${PROTOC} -I=. --java_out=$(PROTOC_JAVA_OPT)$(JAVABASEOUT) $<

proto: $(JAVAS)

clean-proto:
	rm -Rf $(JAVABASEOUT)
