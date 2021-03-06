/**
 * Autogenerated by Thrift
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
package org.apache.accumulo.core.gc.thrift;

import org.apache.thrift.*;
import org.apache.thrift.meta_data.*;
import org.apache.thrift.protocol.*;

public class GCMonitorService {
  
  public interface Iface {
    
    public GCStatus getStatus(cloudtrace.thrift.TInfo tinfo, org.apache.accumulo.core.security.thrift.AuthInfo credentials)
        throws org.apache.accumulo.core.security.thrift.ThriftSecurityException, TException;
    
  }
  
  public static class Client implements TServiceClient, Iface {
    public static class Factory implements TServiceClientFactory<Client> {
      public Factory() {}
      
      public Client getClient(TProtocol prot) {
        return new Client(prot);
      }
      
      public Client getClient(TProtocol iprot, TProtocol oprot) {
        return new Client(iprot, oprot);
      }
    }
    
    public Client(TProtocol prot) {
      this(prot, prot);
    }
    
    public Client(TProtocol iprot, TProtocol oprot) {
      iprot_ = iprot;
      oprot_ = oprot;
    }
    
    protected TProtocol iprot_;
    protected TProtocol oprot_;
    
    protected int seqid_;
    
    public TProtocol getInputProtocol() {
      return this.iprot_;
    }
    
    public TProtocol getOutputProtocol() {
      return this.oprot_;
    }
    
    public GCStatus getStatus(cloudtrace.thrift.TInfo tinfo, org.apache.accumulo.core.security.thrift.AuthInfo credentials)
        throws org.apache.accumulo.core.security.thrift.ThriftSecurityException, TException {
      send_getStatus(tinfo, credentials);
      return recv_getStatus();
    }
    
    public void send_getStatus(cloudtrace.thrift.TInfo tinfo, org.apache.accumulo.core.security.thrift.AuthInfo credentials) throws TException {
      oprot_.writeMessageBegin(new TMessage("getStatus", TMessageType.CALL, ++seqid_));
      getStatus_args args = new getStatus_args();
      args.setTinfo(tinfo);
      args.setCredentials(credentials);
      args.write(oprot_);
      oprot_.writeMessageEnd();
      oprot_.getTransport().flush();
    }
    
    public GCStatus recv_getStatus() throws org.apache.accumulo.core.security.thrift.ThriftSecurityException, TException {
      TMessage msg = iprot_.readMessageBegin();
      if (msg.type == TMessageType.EXCEPTION) {
        TApplicationException x = TApplicationException.read(iprot_);
        iprot_.readMessageEnd();
        throw x;
      }
      if (msg.seqid != seqid_) {
        throw new TApplicationException(TApplicationException.BAD_SEQUENCE_ID, "getStatus failed: out of sequence response");
      }
      getStatus_result result = new getStatus_result();
      result.read(iprot_);
      iprot_.readMessageEnd();
      if (result.isSetSuccess()) {
        return result.success;
      }
      if (result.sec != null) {
        throw result.sec;
      }
      throw new TApplicationException(TApplicationException.MISSING_RESULT, "getStatus failed: unknown result");
    }
    
  }
  
  public static class Processor implements TProcessor {
    private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(Processor.class.getName());
    
    public Processor(Iface iface) {
      iface_ = iface;
      processMap_.put("getStatus", new getStatus());
    }
    
    protected static interface ProcessFunction {
      public void process(int seqid, TProtocol iprot, TProtocol oprot) throws TException;
    }
    
    private Iface iface_;
    protected final java.util.HashMap<String,ProcessFunction> processMap_ = new java.util.HashMap<String,ProcessFunction>();
    
    public boolean process(TProtocol iprot, TProtocol oprot) throws TException {
      TMessage msg = iprot.readMessageBegin();
      ProcessFunction fn = processMap_.get(msg.name);
      if (fn == null) {
        TProtocolUtil.skip(iprot, TType.STRUCT);
        iprot.readMessageEnd();
        TApplicationException x = new TApplicationException(TApplicationException.UNKNOWN_METHOD, "Invalid method name: '" + msg.name + "'");
        oprot.writeMessageBegin(new TMessage(msg.name, TMessageType.EXCEPTION, msg.seqid));
        x.write(oprot);
        oprot.writeMessageEnd();
        oprot.getTransport().flush();
        return true;
      }
      fn.process(msg.seqid, iprot, oprot);
      return true;
    }
    
    private class getStatus implements ProcessFunction {
      public void process(int seqid, TProtocol iprot, TProtocol oprot) throws TException {
        getStatus_args args = new getStatus_args();
        try {
          args.read(iprot);
        } catch (TProtocolException e) {
          iprot.readMessageEnd();
          TApplicationException x = new TApplicationException(TApplicationException.PROTOCOL_ERROR, e.getMessage());
          oprot.writeMessageBegin(new TMessage("getStatus", TMessageType.EXCEPTION, seqid));
          x.write(oprot);
          oprot.writeMessageEnd();
          oprot.getTransport().flush();
          return;
        }
        iprot.readMessageEnd();
        getStatus_result result = new getStatus_result();
        try {
          result.success = iface_.getStatus(args.tinfo, args.credentials);
        } catch (org.apache.accumulo.core.security.thrift.ThriftSecurityException sec) {
          result.sec = sec;
        } catch (Throwable th) {
          LOGGER.error("Internal error processing getStatus", th);
          TApplicationException x = new TApplicationException(TApplicationException.INTERNAL_ERROR, "Internal error processing getStatus");
          oprot.writeMessageBegin(new TMessage("getStatus", TMessageType.EXCEPTION, seqid));
          x.write(oprot);
          oprot.writeMessageEnd();
          oprot.getTransport().flush();
          return;
        }
        oprot.writeMessageBegin(new TMessage("getStatus", TMessageType.REPLY, seqid));
        result.write(oprot);
        oprot.writeMessageEnd();
        oprot.getTransport().flush();
      }
      
    }
    
  }
  
  @SuppressWarnings("serial")
  public static class getStatus_args implements TBase<getStatus_args,getStatus_args._Fields>, java.io.Serializable, Cloneable {
    private static final TStruct STRUCT_DESC = new TStruct("getStatus_args");
    
    private static final TField TINFO_FIELD_DESC = new TField("tinfo", TType.STRUCT, (short) 2);
    private static final TField CREDENTIALS_FIELD_DESC = new TField("credentials", TType.STRUCT, (short) 1);
    
    public cloudtrace.thrift.TInfo tinfo;
    public org.apache.accumulo.core.security.thrift.AuthInfo credentials;
    
    /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
    public enum _Fields implements TFieldIdEnum {
      TINFO((short) 2, "tinfo"), CREDENTIALS((short) 1, "credentials");
      
      private static final java.util.Map<String,_Fields> byName = new java.util.HashMap<String,_Fields>();
      
      static {
        for (_Fields field : java.util.EnumSet.allOf(_Fields.class)) {
          byName.put(field.getFieldName(), field);
        }
      }
      
      /**
       * Find the _Fields constant that matches fieldId, or null if its not found.
       */
      public static _Fields findByThriftId(int fieldId) {
        switch (fieldId) {
          case 2: // TINFO
            return TINFO;
          case 1: // CREDENTIALS
            return CREDENTIALS;
          default:
            return null;
        }
      }
      
      /**
       * Find the _Fields constant that matches fieldId, throwing an exception if it is not found.
       */
      public static _Fields findByThriftIdOrThrow(int fieldId) {
        _Fields fields = findByThriftId(fieldId);
        if (fields == null)
          throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
        return fields;
      }
      
      /**
       * Find the _Fields constant that matches name, or null if its not found.
       */
      public static _Fields findByName(String name) {
        return byName.get(name);
      }
      
      private final short _thriftId;
      private final String _fieldName;
      
      _Fields(short thriftId, String fieldName) {
        _thriftId = thriftId;
        _fieldName = fieldName;
      }
      
      public short getThriftFieldId() {
        return _thriftId;
      }
      
      public String getFieldName() {
        return _fieldName;
      }
    }
    
    // isset id assignments
    
    public static final java.util.Map<_Fields,FieldMetaData> metaDataMap;
    static {
      java.util.Map<_Fields,FieldMetaData> tmpMap = new java.util.EnumMap<_Fields,FieldMetaData>(_Fields.class);
      tmpMap.put(_Fields.TINFO, new FieldMetaData("tinfo", TFieldRequirementType.DEFAULT, new StructMetaData(TType.STRUCT, cloudtrace.thrift.TInfo.class)));
      tmpMap.put(_Fields.CREDENTIALS, new FieldMetaData("credentials", TFieldRequirementType.DEFAULT, new StructMetaData(TType.STRUCT,
          org.apache.accumulo.core.security.thrift.AuthInfo.class)));
      metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
      FieldMetaData.addStructMetaDataMap(getStatus_args.class, metaDataMap);
    }
    
    public getStatus_args() {}
    
    public getStatus_args(cloudtrace.thrift.TInfo tinfo, org.apache.accumulo.core.security.thrift.AuthInfo credentials) {
      this();
      this.tinfo = tinfo;
      this.credentials = credentials;
    }
    
    /**
     * Performs a deep copy on <i>other</i>.
     */
    public getStatus_args(getStatus_args other) {
      if (other.isSetTinfo()) {
        this.tinfo = new cloudtrace.thrift.TInfo(other.tinfo);
      }
      if (other.isSetCredentials()) {
        this.credentials = new org.apache.accumulo.core.security.thrift.AuthInfo(other.credentials);
      }
    }
    
    public getStatus_args deepCopy() {
      return new getStatus_args(this);
    }
    
    @Deprecated
    public getStatus_args clone() {
      return new getStatus_args(this);
    }
    
    public cloudtrace.thrift.TInfo getTinfo() {
      return this.tinfo;
    }
    
    public getStatus_args setTinfo(cloudtrace.thrift.TInfo tinfo) {
      this.tinfo = tinfo;
      return this;
    }
    
    public void unsetTinfo() {
      this.tinfo = null;
    }
    
    /** Returns true if field tinfo is set (has been asigned a value) and false otherwise */
    public boolean isSetTinfo() {
      return this.tinfo != null;
    }
    
    public void setTinfoIsSet(boolean value) {
      if (!value) {
        this.tinfo = null;
      }
    }
    
    public org.apache.accumulo.core.security.thrift.AuthInfo getCredentials() {
      return this.credentials;
    }
    
    public getStatus_args setCredentials(org.apache.accumulo.core.security.thrift.AuthInfo credentials) {
      this.credentials = credentials;
      return this;
    }
    
    public void unsetCredentials() {
      this.credentials = null;
    }
    
    /** Returns true if field credentials is set (has been asigned a value) and false otherwise */
    public boolean isSetCredentials() {
      return this.credentials != null;
    }
    
    public void setCredentialsIsSet(boolean value) {
      if (!value) {
        this.credentials = null;
      }
    }
    
    public void setFieldValue(_Fields field, Object value) {
      switch (field) {
        case TINFO:
          if (value == null) {
            unsetTinfo();
          } else {
            setTinfo((cloudtrace.thrift.TInfo) value);
          }
          break;
        
        case CREDENTIALS:
          if (value == null) {
            unsetCredentials();
          } else {
            setCredentials((org.apache.accumulo.core.security.thrift.AuthInfo) value);
          }
          break;
      
      }
    }
    
    public void setFieldValue(int fieldID, Object value) {
      setFieldValue(_Fields.findByThriftIdOrThrow(fieldID), value);
    }
    
    public Object getFieldValue(_Fields field) {
      switch (field) {
        case TINFO:
          return getTinfo();
          
        case CREDENTIALS:
          return getCredentials();
          
      }
      throw new IllegalStateException();
    }
    
    public Object getFieldValue(int fieldId) {
      return getFieldValue(_Fields.findByThriftIdOrThrow(fieldId));
    }
    
    /** Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise */
    public boolean isSet(_Fields field) {
      switch (field) {
        case TINFO:
          return isSetTinfo();
        case CREDENTIALS:
          return isSetCredentials();
      }
      throw new IllegalStateException();
    }
    
    public boolean isSet(int fieldID) {
      return isSet(_Fields.findByThriftIdOrThrow(fieldID));
    }
    
    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof getStatus_args)
        return this.equals((getStatus_args) that);
      return false;
    }
    
    public boolean equals(getStatus_args that) {
      if (that == null)
        return false;
      
      boolean this_present_tinfo = true && this.isSetTinfo();
      boolean that_present_tinfo = true && that.isSetTinfo();
      if (this_present_tinfo || that_present_tinfo) {
        if (!(this_present_tinfo && that_present_tinfo))
          return false;
        if (!this.tinfo.equals(that.tinfo))
          return false;
      }
      
      boolean this_present_credentials = true && this.isSetCredentials();
      boolean that_present_credentials = true && that.isSetCredentials();
      if (this_present_credentials || that_present_credentials) {
        if (!(this_present_credentials && that_present_credentials))
          return false;
        if (!this.credentials.equals(that.credentials))
          return false;
      }
      
      return true;
    }
    
    @Override
    public int hashCode() {
      return 0;
    }
    
    public int compareTo(getStatus_args other) {
      if (!getClass().equals(other.getClass())) {
        return getClass().getName().compareTo(other.getClass().getName());
      }
      
      int lastComparison = 0;
      getStatus_args typedOther = (getStatus_args) other;
      
      lastComparison = Boolean.valueOf(isSetTinfo()).compareTo(typedOther.isSetTinfo());
      if (lastComparison != 0) {
        return lastComparison;
      }
      if (isSetTinfo()) {
        lastComparison = TBaseHelper.compareTo(this.tinfo, typedOther.tinfo);
        if (lastComparison != 0) {
          return lastComparison;
        }
      }
      lastComparison = Boolean.valueOf(isSetCredentials()).compareTo(typedOther.isSetCredentials());
      if (lastComparison != 0) {
        return lastComparison;
      }
      if (isSetCredentials()) {
        lastComparison = TBaseHelper.compareTo(this.credentials, typedOther.credentials);
        if (lastComparison != 0) {
          return lastComparison;
        }
      }
      return 0;
    }
    
    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true) {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) {
          break;
        }
        switch (field.id) {
          case 2: // TINFO
            if (field.type == TType.STRUCT) {
              this.tinfo = new cloudtrace.thrift.TInfo();
              this.tinfo.read(iprot);
            } else {
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case 1: // CREDENTIALS
            if (field.type == TType.STRUCT) {
              this.credentials = new org.apache.accumulo.core.security.thrift.AuthInfo();
              this.credentials.read(iprot);
            } else {
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();
      
      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }
    
    public void write(TProtocol oprot) throws TException {
      validate();
      
      oprot.writeStructBegin(STRUCT_DESC);
      if (this.credentials != null) {
        oprot.writeFieldBegin(CREDENTIALS_FIELD_DESC);
        this.credentials.write(oprot);
        oprot.writeFieldEnd();
      }
      if (this.tinfo != null) {
        oprot.writeFieldBegin(TINFO_FIELD_DESC);
        this.tinfo.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }
    
    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("getStatus_args(");
      sb.append("tinfo:");
      if (this.tinfo == null) {
        sb.append("null");
      } else {
        sb.append(this.tinfo);
      }
      sb.append(", ");
      sb.append("credentials:");
      if (this.credentials == null) {
        sb.append("null");
      } else {
        sb.append(this.credentials);
      }
      sb.append(")");
      return sb.toString();
    }
    
    public void validate() throws TException {
      // check for required fields
    }
    
  }
  
  @SuppressWarnings("serial")
  public static class getStatus_result implements TBase<getStatus_result,getStatus_result._Fields>, java.io.Serializable, Cloneable {
    private static final TStruct STRUCT_DESC = new TStruct("getStatus_result");
    
    private static final TField SUCCESS_FIELD_DESC = new TField("success", TType.STRUCT, (short) 0);
    private static final TField SEC_FIELD_DESC = new TField("sec", TType.STRUCT, (short) 1);
    
    public GCStatus success;
    public org.apache.accumulo.core.security.thrift.ThriftSecurityException sec;
    
    /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
    public enum _Fields implements TFieldIdEnum {
      SUCCESS((short) 0, "success"), SEC((short) 1, "sec");
      
      private static final java.util.Map<String,_Fields> byName = new java.util.HashMap<String,_Fields>();
      
      static {
        for (_Fields field : java.util.EnumSet.allOf(_Fields.class)) {
          byName.put(field.getFieldName(), field);
        }
      }
      
      /**
       * Find the _Fields constant that matches fieldId, or null if its not found.
       */
      public static _Fields findByThriftId(int fieldId) {
        switch (fieldId) {
          case 0: // SUCCESS
            return SUCCESS;
          case 1: // SEC
            return SEC;
          default:
            return null;
        }
      }
      
      /**
       * Find the _Fields constant that matches fieldId, throwing an exception if it is not found.
       */
      public static _Fields findByThriftIdOrThrow(int fieldId) {
        _Fields fields = findByThriftId(fieldId);
        if (fields == null)
          throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
        return fields;
      }
      
      /**
       * Find the _Fields constant that matches name, or null if its not found.
       */
      public static _Fields findByName(String name) {
        return byName.get(name);
      }
      
      private final short _thriftId;
      private final String _fieldName;
      
      _Fields(short thriftId, String fieldName) {
        _thriftId = thriftId;
        _fieldName = fieldName;
      }
      
      public short getThriftFieldId() {
        return _thriftId;
      }
      
      public String getFieldName() {
        return _fieldName;
      }
    }
    
    // isset id assignments
    
    public static final java.util.Map<_Fields,FieldMetaData> metaDataMap;
    static {
      java.util.Map<_Fields,FieldMetaData> tmpMap = new java.util.EnumMap<_Fields,FieldMetaData>(_Fields.class);
      tmpMap.put(_Fields.SUCCESS, new FieldMetaData("success", TFieldRequirementType.DEFAULT, new StructMetaData(TType.STRUCT, GCStatus.class)));
      tmpMap.put(_Fields.SEC, new FieldMetaData("sec", TFieldRequirementType.DEFAULT, new FieldValueMetaData(TType.STRUCT)));
      metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
      FieldMetaData.addStructMetaDataMap(getStatus_result.class, metaDataMap);
    }
    
    public getStatus_result() {}
    
    public getStatus_result(GCStatus success, org.apache.accumulo.core.security.thrift.ThriftSecurityException sec) {
      this();
      this.success = success;
      this.sec = sec;
    }
    
    /**
     * Performs a deep copy on <i>other</i>.
     */
    public getStatus_result(getStatus_result other) {
      if (other.isSetSuccess()) {
        this.success = new GCStatus(other.success);
      }
      if (other.isSetSec()) {
        this.sec = new org.apache.accumulo.core.security.thrift.ThriftSecurityException(other.sec);
      }
    }
    
    public getStatus_result deepCopy() {
      return new getStatus_result(this);
    }
    
    @Deprecated
    public getStatus_result clone() {
      return new getStatus_result(this);
    }
    
    public GCStatus getSuccess() {
      return this.success;
    }
    
    public getStatus_result setSuccess(GCStatus success) {
      this.success = success;
      return this;
    }
    
    public void unsetSuccess() {
      this.success = null;
    }
    
    /** Returns true if field success is set (has been asigned a value) and false otherwise */
    public boolean isSetSuccess() {
      return this.success != null;
    }
    
    public void setSuccessIsSet(boolean value) {
      if (!value) {
        this.success = null;
      }
    }
    
    public org.apache.accumulo.core.security.thrift.ThriftSecurityException getSec() {
      return this.sec;
    }
    
    public getStatus_result setSec(org.apache.accumulo.core.security.thrift.ThriftSecurityException sec) {
      this.sec = sec;
      return this;
    }
    
    public void unsetSec() {
      this.sec = null;
    }
    
    /** Returns true if field sec is set (has been asigned a value) and false otherwise */
    public boolean isSetSec() {
      return this.sec != null;
    }
    
    public void setSecIsSet(boolean value) {
      if (!value) {
        this.sec = null;
      }
    }
    
    public void setFieldValue(_Fields field, Object value) {
      switch (field) {
        case SUCCESS:
          if (value == null) {
            unsetSuccess();
          } else {
            setSuccess((GCStatus) value);
          }
          break;
        
        case SEC:
          if (value == null) {
            unsetSec();
          } else {
            setSec((org.apache.accumulo.core.security.thrift.ThriftSecurityException) value);
          }
          break;
      
      }
    }
    
    public void setFieldValue(int fieldID, Object value) {
      setFieldValue(_Fields.findByThriftIdOrThrow(fieldID), value);
    }
    
    public Object getFieldValue(_Fields field) {
      switch (field) {
        case SUCCESS:
          return getSuccess();
          
        case SEC:
          return getSec();
          
      }
      throw new IllegalStateException();
    }
    
    public Object getFieldValue(int fieldId) {
      return getFieldValue(_Fields.findByThriftIdOrThrow(fieldId));
    }
    
    /** Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise */
    public boolean isSet(_Fields field) {
      switch (field) {
        case SUCCESS:
          return isSetSuccess();
        case SEC:
          return isSetSec();
      }
      throw new IllegalStateException();
    }
    
    public boolean isSet(int fieldID) {
      return isSet(_Fields.findByThriftIdOrThrow(fieldID));
    }
    
    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof getStatus_result)
        return this.equals((getStatus_result) that);
      return false;
    }
    
    public boolean equals(getStatus_result that) {
      if (that == null)
        return false;
      
      boolean this_present_success = true && this.isSetSuccess();
      boolean that_present_success = true && that.isSetSuccess();
      if (this_present_success || that_present_success) {
        if (!(this_present_success && that_present_success))
          return false;
        if (!this.success.equals(that.success))
          return false;
      }
      
      boolean this_present_sec = true && this.isSetSec();
      boolean that_present_sec = true && that.isSetSec();
      if (this_present_sec || that_present_sec) {
        if (!(this_present_sec && that_present_sec))
          return false;
        if (!this.sec.equals(that.sec))
          return false;
      }
      
      return true;
    }
    
    @Override
    public int hashCode() {
      return 0;
    }
    
    public int compareTo(getStatus_result other) {
      if (!getClass().equals(other.getClass())) {
        return getClass().getName().compareTo(other.getClass().getName());
      }
      
      int lastComparison = 0;
      getStatus_result typedOther = (getStatus_result) other;
      
      lastComparison = Boolean.valueOf(isSetSuccess()).compareTo(typedOther.isSetSuccess());
      if (lastComparison != 0) {
        return lastComparison;
      }
      if (isSetSuccess()) {
        lastComparison = TBaseHelper.compareTo(this.success, typedOther.success);
        if (lastComparison != 0) {
          return lastComparison;
        }
      }
      lastComparison = Boolean.valueOf(isSetSec()).compareTo(typedOther.isSetSec());
      if (lastComparison != 0) {
        return lastComparison;
      }
      if (isSetSec()) {
        lastComparison = TBaseHelper.compareTo(this.sec, typedOther.sec);
        if (lastComparison != 0) {
          return lastComparison;
        }
      }
      return 0;
    }
    
    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true) {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) {
          break;
        }
        switch (field.id) {
          case 0: // SUCCESS
            if (field.type == TType.STRUCT) {
              this.success = new GCStatus();
              this.success.read(iprot);
            } else {
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case 1: // SEC
            if (field.type == TType.STRUCT) {
              this.sec = new org.apache.accumulo.core.security.thrift.ThriftSecurityException();
              this.sec.read(iprot);
            } else {
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();
      
      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }
    
    public void write(TProtocol oprot) throws TException {
      oprot.writeStructBegin(STRUCT_DESC);
      
      if (this.isSetSuccess()) {
        oprot.writeFieldBegin(SUCCESS_FIELD_DESC);
        this.success.write(oprot);
        oprot.writeFieldEnd();
      } else if (this.isSetSec()) {
        oprot.writeFieldBegin(SEC_FIELD_DESC);
        this.sec.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }
    
    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("getStatus_result(");
      sb.append("success:");
      if (this.success == null) {
        sb.append("null");
      } else {
        sb.append(this.success);
      }
      sb.append(", ");
      sb.append("sec:");
      if (this.sec == null) {
        sb.append("null");
      } else {
        sb.append(this.sec);
      }
      sb.append(")");
      return sb.toString();
    }
    
    public void validate() throws TException {
      // check for required fields
    }
    
  }
  
}
