// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: MsgDelNodeConn.proto

package com.linbit.drbdmanage.proto;

public final class MsgDelNodeConnOuterClass {
  private MsgDelNodeConnOuterClass() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistryLite registry) {
  }

  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
    registerAllExtensions(
        (com.google.protobuf.ExtensionRegistryLite) registry);
  }
  public interface MsgDelNodeConnOrBuilder extends
      // @@protoc_insertion_point(interface_extends:com.linbit.drbdmanage.proto.MsgDelNodeConn)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <pre>
     * UUID
     * </pre>
     *
     * <code>optional bytes uuid = 1;</code>
     */
    boolean hasUuid();
    /**
     * <pre>
     * UUID
     * </pre>
     *
     * <code>optional bytes uuid = 1;</code>
     */
    com.google.protobuf.ByteString getUuid();

    /**
     * <pre>
     * Node 1 name
     * </pre>
     *
     * <code>required string node_name_1 = 2;</code>
     */
    boolean hasNodeName1();
    /**
     * <pre>
     * Node 1 name
     * </pre>
     *
     * <code>required string node_name_1 = 2;</code>
     */
    java.lang.String getNodeName1();
    /**
     * <pre>
     * Node 1 name
     * </pre>
     *
     * <code>required string node_name_1 = 2;</code>
     */
    com.google.protobuf.ByteString
        getNodeName1Bytes();

    /**
     * <pre>
     * Node 2 name
     * </pre>
     *
     * <code>required string node_name_2 = 3;</code>
     */
    boolean hasNodeName2();
    /**
     * <pre>
     * Node 2 name
     * </pre>
     *
     * <code>required string node_name_2 = 3;</code>
     */
    java.lang.String getNodeName2();
    /**
     * <pre>
     * Node 2 name
     * </pre>
     *
     * <code>required string node_name_2 = 3;</code>
     */
    com.google.protobuf.ByteString
        getNodeName2Bytes();
  }
  /**
   * <pre>
   * linstor - Delete node connection
   * </pre>
   *
   * Protobuf type {@code com.linbit.drbdmanage.proto.MsgDelNodeConn}
   */
  public  static final class MsgDelNodeConn extends
      com.google.protobuf.GeneratedMessageV3 implements
      // @@protoc_insertion_point(message_implements:com.linbit.drbdmanage.proto.MsgDelNodeConn)
      MsgDelNodeConnOrBuilder {
    // Use MsgDelNodeConn.newBuilder() to construct.
    private MsgDelNodeConn(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
      super(builder);
    }
    private MsgDelNodeConn() {
      uuid_ = com.google.protobuf.ByteString.EMPTY;
      nodeName1_ = "";
      nodeName2_ = "";
    }

    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
    getUnknownFields() {
      return this.unknownFields;
    }
    private MsgDelNodeConn(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      this();
      int mutable_bitField0_ = 0;
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            default: {
              if (!parseUnknownField(input, unknownFields,
                                     extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
            case 10: {
              bitField0_ |= 0x00000001;
              uuid_ = input.readBytes();
              break;
            }
            case 18: {
              com.google.protobuf.ByteString bs = input.readBytes();
              bitField0_ |= 0x00000002;
              nodeName1_ = bs;
              break;
            }
            case 26: {
              com.google.protobuf.ByteString bs = input.readBytes();
              bitField0_ |= 0x00000004;
              nodeName2_ = bs;
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e).setUnfinishedMessage(this);
      } finally {
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return com.linbit.drbdmanage.proto.MsgDelNodeConnOuterClass.internal_static_com_linbit_drbdmanage_proto_MsgDelNodeConn_descriptor;
    }

    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return com.linbit.drbdmanage.proto.MsgDelNodeConnOuterClass.internal_static_com_linbit_drbdmanage_proto_MsgDelNodeConn_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              com.linbit.drbdmanage.proto.MsgDelNodeConnOuterClass.MsgDelNodeConn.class, com.linbit.drbdmanage.proto.MsgDelNodeConnOuterClass.MsgDelNodeConn.Builder.class);
    }

    private int bitField0_;
    public static final int UUID_FIELD_NUMBER = 1;
    private com.google.protobuf.ByteString uuid_;
    /**
     * <pre>
     * UUID
     * </pre>
     *
     * <code>optional bytes uuid = 1;</code>
     */
    public boolean hasUuid() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <pre>
     * UUID
     * </pre>
     *
     * <code>optional bytes uuid = 1;</code>
     */
    public com.google.protobuf.ByteString getUuid() {
      return uuid_;
    }

    public static final int NODE_NAME_1_FIELD_NUMBER = 2;
    private volatile java.lang.Object nodeName1_;
    /**
     * <pre>
     * Node 1 name
     * </pre>
     *
     * <code>required string node_name_1 = 2;</code>
     */
    public boolean hasNodeName1() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <pre>
     * Node 1 name
     * </pre>
     *
     * <code>required string node_name_1 = 2;</code>
     */
    public java.lang.String getNodeName1() {
      java.lang.Object ref = nodeName1_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          nodeName1_ = s;
        }
        return s;
      }
    }
    /**
     * <pre>
     * Node 1 name
     * </pre>
     *
     * <code>required string node_name_1 = 2;</code>
     */
    public com.google.protobuf.ByteString
        getNodeName1Bytes() {
      java.lang.Object ref = nodeName1_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        nodeName1_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    public static final int NODE_NAME_2_FIELD_NUMBER = 3;
    private volatile java.lang.Object nodeName2_;
    /**
     * <pre>
     * Node 2 name
     * </pre>
     *
     * <code>required string node_name_2 = 3;</code>
     */
    public boolean hasNodeName2() {
      return ((bitField0_ & 0x00000004) == 0x00000004);
    }
    /**
     * <pre>
     * Node 2 name
     * </pre>
     *
     * <code>required string node_name_2 = 3;</code>
     */
    public java.lang.String getNodeName2() {
      java.lang.Object ref = nodeName2_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          nodeName2_ = s;
        }
        return s;
      }
    }
    /**
     * <pre>
     * Node 2 name
     * </pre>
     *
     * <code>required string node_name_2 = 3;</code>
     */
    public com.google.protobuf.ByteString
        getNodeName2Bytes() {
      java.lang.Object ref = nodeName2_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        nodeName2_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      if (!hasNodeName1()) {
        memoizedIsInitialized = 0;
        return false;
      }
      if (!hasNodeName2()) {
        memoizedIsInitialized = 0;
        return false;
      }
      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        output.writeBytes(1, uuid_);
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        com.google.protobuf.GeneratedMessageV3.writeString(output, 2, nodeName1_);
      }
      if (((bitField0_ & 0x00000004) == 0x00000004)) {
        com.google.protobuf.GeneratedMessageV3.writeString(output, 3, nodeName2_);
      }
      unknownFields.writeTo(output);
    }

    public int getSerializedSize() {
      int size = memoizedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(1, uuid_);
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        size += com.google.protobuf.GeneratedMessageV3.computeStringSize(2, nodeName1_);
      }
      if (((bitField0_ & 0x00000004) == 0x00000004)) {
        size += com.google.protobuf.GeneratedMessageV3.computeStringSize(3, nodeName2_);
      }
      size += unknownFields.getSerializedSize();
      memoizedSize = size;
      return size;
    }

    private static final long serialVersionUID = 0L;
    @java.lang.Override
    public boolean equals(final java.lang.Object obj) {
      if (obj == this) {
       return true;
      }
      if (!(obj instanceof com.linbit.drbdmanage.proto.MsgDelNodeConnOuterClass.MsgDelNodeConn)) {
        return super.equals(obj);
      }
      com.linbit.drbdmanage.proto.MsgDelNodeConnOuterClass.MsgDelNodeConn other = (com.linbit.drbdmanage.proto.MsgDelNodeConnOuterClass.MsgDelNodeConn) obj;

      boolean result = true;
      result = result && (hasUuid() == other.hasUuid());
      if (hasUuid()) {
        result = result && getUuid()
            .equals(other.getUuid());
      }
      result = result && (hasNodeName1() == other.hasNodeName1());
      if (hasNodeName1()) {
        result = result && getNodeName1()
            .equals(other.getNodeName1());
      }
      result = result && (hasNodeName2() == other.hasNodeName2());
      if (hasNodeName2()) {
        result = result && getNodeName2()
            .equals(other.getNodeName2());
      }
      result = result && unknownFields.equals(other.unknownFields);
      return result;
    }

    @java.lang.Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptor().hashCode();
      if (hasUuid()) {
        hash = (37 * hash) + UUID_FIELD_NUMBER;
        hash = (53 * hash) + getUuid().hashCode();
      }
      if (hasNodeName1()) {
        hash = (37 * hash) + NODE_NAME_1_FIELD_NUMBER;
        hash = (53 * hash) + getNodeName1().hashCode();
      }
      if (hasNodeName2()) {
        hash = (37 * hash) + NODE_NAME_2_FIELD_NUMBER;
        hash = (53 * hash) + getNodeName2().hashCode();
      }
      hash = (29 * hash) + unknownFields.hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static com.linbit.drbdmanage.proto.MsgDelNodeConnOuterClass.MsgDelNodeConn parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.linbit.drbdmanage.proto.MsgDelNodeConnOuterClass.MsgDelNodeConn parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.linbit.drbdmanage.proto.MsgDelNodeConnOuterClass.MsgDelNodeConn parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.linbit.drbdmanage.proto.MsgDelNodeConnOuterClass.MsgDelNodeConn parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.linbit.drbdmanage.proto.MsgDelNodeConnOuterClass.MsgDelNodeConn parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static com.linbit.drbdmanage.proto.MsgDelNodeConnOuterClass.MsgDelNodeConn parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }
    public static com.linbit.drbdmanage.proto.MsgDelNodeConnOuterClass.MsgDelNodeConn parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input);
    }
    public static com.linbit.drbdmanage.proto.MsgDelNodeConnOuterClass.MsgDelNodeConn parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
    }
    public static com.linbit.drbdmanage.proto.MsgDelNodeConnOuterClass.MsgDelNodeConn parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static com.linbit.drbdmanage.proto.MsgDelNodeConnOuterClass.MsgDelNodeConn parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }

    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder() {
      return DEFAULT_INSTANCE.toBuilder();
    }
    public static Builder newBuilder(com.linbit.drbdmanage.proto.MsgDelNodeConnOuterClass.MsgDelNodeConn prototype) {
      return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() {
      return this == DEFAULT_INSTANCE
          ? new Builder() : new Builder().mergeFrom(this);
    }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * <pre>
     * linstor - Delete node connection
     * </pre>
     *
     * Protobuf type {@code com.linbit.drbdmanage.proto.MsgDelNodeConn}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
        // @@protoc_insertion_point(builder_implements:com.linbit.drbdmanage.proto.MsgDelNodeConn)
        com.linbit.drbdmanage.proto.MsgDelNodeConnOuterClass.MsgDelNodeConnOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return com.linbit.drbdmanage.proto.MsgDelNodeConnOuterClass.internal_static_com_linbit_drbdmanage_proto_MsgDelNodeConn_descriptor;
      }

      protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return com.linbit.drbdmanage.proto.MsgDelNodeConnOuterClass.internal_static_com_linbit_drbdmanage_proto_MsgDelNodeConn_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                com.linbit.drbdmanage.proto.MsgDelNodeConnOuterClass.MsgDelNodeConn.class, com.linbit.drbdmanage.proto.MsgDelNodeConnOuterClass.MsgDelNodeConn.Builder.class);
      }

      // Construct using com.linbit.drbdmanage.proto.MsgDelNodeConnOuterClass.MsgDelNodeConn.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessageV3
                .alwaysUseFieldBuilders) {
        }
      }
      public Builder clear() {
        super.clear();
        uuid_ = com.google.protobuf.ByteString.EMPTY;
        bitField0_ = (bitField0_ & ~0x00000001);
        nodeName1_ = "";
        bitField0_ = (bitField0_ & ~0x00000002);
        nodeName2_ = "";
        bitField0_ = (bitField0_ & ~0x00000004);
        return this;
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return com.linbit.drbdmanage.proto.MsgDelNodeConnOuterClass.internal_static_com_linbit_drbdmanage_proto_MsgDelNodeConn_descriptor;
      }

      public com.linbit.drbdmanage.proto.MsgDelNodeConnOuterClass.MsgDelNodeConn getDefaultInstanceForType() {
        return com.linbit.drbdmanage.proto.MsgDelNodeConnOuterClass.MsgDelNodeConn.getDefaultInstance();
      }

      public com.linbit.drbdmanage.proto.MsgDelNodeConnOuterClass.MsgDelNodeConn build() {
        com.linbit.drbdmanage.proto.MsgDelNodeConnOuterClass.MsgDelNodeConn result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public com.linbit.drbdmanage.proto.MsgDelNodeConnOuterClass.MsgDelNodeConn buildPartial() {
        com.linbit.drbdmanage.proto.MsgDelNodeConnOuterClass.MsgDelNodeConn result = new com.linbit.drbdmanage.proto.MsgDelNodeConnOuterClass.MsgDelNodeConn(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
          to_bitField0_ |= 0x00000001;
        }
        result.uuid_ = uuid_;
        if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
          to_bitField0_ |= 0x00000002;
        }
        result.nodeName1_ = nodeName1_;
        if (((from_bitField0_ & 0x00000004) == 0x00000004)) {
          to_bitField0_ |= 0x00000004;
        }
        result.nodeName2_ = nodeName2_;
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }

      public Builder clone() {
        return (Builder) super.clone();
      }
      public Builder setField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          Object value) {
        return (Builder) super.setField(field, value);
      }
      public Builder clearField(
          com.google.protobuf.Descriptors.FieldDescriptor field) {
        return (Builder) super.clearField(field);
      }
      public Builder clearOneof(
          com.google.protobuf.Descriptors.OneofDescriptor oneof) {
        return (Builder) super.clearOneof(oneof);
      }
      public Builder setRepeatedField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          int index, Object value) {
        return (Builder) super.setRepeatedField(field, index, value);
      }
      public Builder addRepeatedField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          Object value) {
        return (Builder) super.addRepeatedField(field, value);
      }
      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof com.linbit.drbdmanage.proto.MsgDelNodeConnOuterClass.MsgDelNodeConn) {
          return mergeFrom((com.linbit.drbdmanage.proto.MsgDelNodeConnOuterClass.MsgDelNodeConn)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(com.linbit.drbdmanage.proto.MsgDelNodeConnOuterClass.MsgDelNodeConn other) {
        if (other == com.linbit.drbdmanage.proto.MsgDelNodeConnOuterClass.MsgDelNodeConn.getDefaultInstance()) return this;
        if (other.hasUuid()) {
          setUuid(other.getUuid());
        }
        if (other.hasNodeName1()) {
          bitField0_ |= 0x00000002;
          nodeName1_ = other.nodeName1_;
          onChanged();
        }
        if (other.hasNodeName2()) {
          bitField0_ |= 0x00000004;
          nodeName2_ = other.nodeName2_;
          onChanged();
        }
        this.mergeUnknownFields(other.unknownFields);
        onChanged();
        return this;
      }

      public final boolean isInitialized() {
        if (!hasNodeName1()) {
          return false;
        }
        if (!hasNodeName2()) {
          return false;
        }
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        com.linbit.drbdmanage.proto.MsgDelNodeConnOuterClass.MsgDelNodeConn parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (com.linbit.drbdmanage.proto.MsgDelNodeConnOuterClass.MsgDelNodeConn) e.getUnfinishedMessage();
          throw e.unwrapIOException();
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      private com.google.protobuf.ByteString uuid_ = com.google.protobuf.ByteString.EMPTY;
      /**
       * <pre>
       * UUID
       * </pre>
       *
       * <code>optional bytes uuid = 1;</code>
       */
      public boolean hasUuid() {
        return ((bitField0_ & 0x00000001) == 0x00000001);
      }
      /**
       * <pre>
       * UUID
       * </pre>
       *
       * <code>optional bytes uuid = 1;</code>
       */
      public com.google.protobuf.ByteString getUuid() {
        return uuid_;
      }
      /**
       * <pre>
       * UUID
       * </pre>
       *
       * <code>optional bytes uuid = 1;</code>
       */
      public Builder setUuid(com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
        uuid_ = value;
        onChanged();
        return this;
      }
      /**
       * <pre>
       * UUID
       * </pre>
       *
       * <code>optional bytes uuid = 1;</code>
       */
      public Builder clearUuid() {
        bitField0_ = (bitField0_ & ~0x00000001);
        uuid_ = getDefaultInstance().getUuid();
        onChanged();
        return this;
      }

      private java.lang.Object nodeName1_ = "";
      /**
       * <pre>
       * Node 1 name
       * </pre>
       *
       * <code>required string node_name_1 = 2;</code>
       */
      public boolean hasNodeName1() {
        return ((bitField0_ & 0x00000002) == 0x00000002);
      }
      /**
       * <pre>
       * Node 1 name
       * </pre>
       *
       * <code>required string node_name_1 = 2;</code>
       */
      public java.lang.String getNodeName1() {
        java.lang.Object ref = nodeName1_;
        if (!(ref instanceof java.lang.String)) {
          com.google.protobuf.ByteString bs =
              (com.google.protobuf.ByteString) ref;
          java.lang.String s = bs.toStringUtf8();
          if (bs.isValidUtf8()) {
            nodeName1_ = s;
          }
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <pre>
       * Node 1 name
       * </pre>
       *
       * <code>required string node_name_1 = 2;</code>
       */
      public com.google.protobuf.ByteString
          getNodeName1Bytes() {
        java.lang.Object ref = nodeName1_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          nodeName1_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <pre>
       * Node 1 name
       * </pre>
       *
       * <code>required string node_name_1 = 2;</code>
       */
      public Builder setNodeName1(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
        nodeName1_ = value;
        onChanged();
        return this;
      }
      /**
       * <pre>
       * Node 1 name
       * </pre>
       *
       * <code>required string node_name_1 = 2;</code>
       */
      public Builder clearNodeName1() {
        bitField0_ = (bitField0_ & ~0x00000002);
        nodeName1_ = getDefaultInstance().getNodeName1();
        onChanged();
        return this;
      }
      /**
       * <pre>
       * Node 1 name
       * </pre>
       *
       * <code>required string node_name_1 = 2;</code>
       */
      public Builder setNodeName1Bytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
        nodeName1_ = value;
        onChanged();
        return this;
      }

      private java.lang.Object nodeName2_ = "";
      /**
       * <pre>
       * Node 2 name
       * </pre>
       *
       * <code>required string node_name_2 = 3;</code>
       */
      public boolean hasNodeName2() {
        return ((bitField0_ & 0x00000004) == 0x00000004);
      }
      /**
       * <pre>
       * Node 2 name
       * </pre>
       *
       * <code>required string node_name_2 = 3;</code>
       */
      public java.lang.String getNodeName2() {
        java.lang.Object ref = nodeName2_;
        if (!(ref instanceof java.lang.String)) {
          com.google.protobuf.ByteString bs =
              (com.google.protobuf.ByteString) ref;
          java.lang.String s = bs.toStringUtf8();
          if (bs.isValidUtf8()) {
            nodeName2_ = s;
          }
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <pre>
       * Node 2 name
       * </pre>
       *
       * <code>required string node_name_2 = 3;</code>
       */
      public com.google.protobuf.ByteString
          getNodeName2Bytes() {
        java.lang.Object ref = nodeName2_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          nodeName2_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <pre>
       * Node 2 name
       * </pre>
       *
       * <code>required string node_name_2 = 3;</code>
       */
      public Builder setNodeName2(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000004;
        nodeName2_ = value;
        onChanged();
        return this;
      }
      /**
       * <pre>
       * Node 2 name
       * </pre>
       *
       * <code>required string node_name_2 = 3;</code>
       */
      public Builder clearNodeName2() {
        bitField0_ = (bitField0_ & ~0x00000004);
        nodeName2_ = getDefaultInstance().getNodeName2();
        onChanged();
        return this;
      }
      /**
       * <pre>
       * Node 2 name
       * </pre>
       *
       * <code>required string node_name_2 = 3;</code>
       */
      public Builder setNodeName2Bytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000004;
        nodeName2_ = value;
        onChanged();
        return this;
      }
      public final Builder setUnknownFields(
          final com.google.protobuf.UnknownFieldSet unknownFields) {
        return super.setUnknownFields(unknownFields);
      }

      public final Builder mergeUnknownFields(
          final com.google.protobuf.UnknownFieldSet unknownFields) {
        return super.mergeUnknownFields(unknownFields);
      }


      // @@protoc_insertion_point(builder_scope:com.linbit.drbdmanage.proto.MsgDelNodeConn)
    }

    // @@protoc_insertion_point(class_scope:com.linbit.drbdmanage.proto.MsgDelNodeConn)
    private static final com.linbit.drbdmanage.proto.MsgDelNodeConnOuterClass.MsgDelNodeConn DEFAULT_INSTANCE;
    static {
      DEFAULT_INSTANCE = new com.linbit.drbdmanage.proto.MsgDelNodeConnOuterClass.MsgDelNodeConn();
    }

    public static com.linbit.drbdmanage.proto.MsgDelNodeConnOuterClass.MsgDelNodeConn getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    @java.lang.Deprecated public static final com.google.protobuf.Parser<MsgDelNodeConn>
        PARSER = new com.google.protobuf.AbstractParser<MsgDelNodeConn>() {
      public MsgDelNodeConn parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
          return new MsgDelNodeConn(input, extensionRegistry);
      }
    };

    public static com.google.protobuf.Parser<MsgDelNodeConn> parser() {
      return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<MsgDelNodeConn> getParserForType() {
      return PARSER;
    }

    public com.linbit.drbdmanage.proto.MsgDelNodeConnOuterClass.MsgDelNodeConn getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }

  }

  private static final com.google.protobuf.Descriptors.Descriptor
    internal_static_com_linbit_drbdmanage_proto_MsgDelNodeConn_descriptor;
  private static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_com_linbit_drbdmanage_proto_MsgDelNodeConn_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static  com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\024MsgDelNodeConn.proto\022\033com.linbit.drbdm" +
      "anage.proto\"H\n\016MsgDelNodeConn\022\014\n\004uuid\030\001 " +
      "\001(\014\022\023\n\013node_name_1\030\002 \002(\t\022\023\n\013node_name_2\030" +
      "\003 \002(\t"
    };
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
        new com.google.protobuf.Descriptors.FileDescriptor.    InternalDescriptorAssigner() {
          public com.google.protobuf.ExtensionRegistry assignDescriptors(
              com.google.protobuf.Descriptors.FileDescriptor root) {
            descriptor = root;
            return null;
          }
        };
    com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        }, assigner);
    internal_static_com_linbit_drbdmanage_proto_MsgDelNodeConn_descriptor =
      getDescriptor().getMessageTypes().get(0);
    internal_static_com_linbit_drbdmanage_proto_MsgDelNodeConn_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_com_linbit_drbdmanage_proto_MsgDelNodeConn_descriptor,
        new java.lang.String[] { "Uuid", "NodeName1", "NodeName2", });
  }

  // @@protoc_insertion_point(outer_class_scope)
}
