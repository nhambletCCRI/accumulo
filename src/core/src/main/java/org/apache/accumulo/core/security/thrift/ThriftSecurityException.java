/**
 * Autogenerated by Thrift
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
package org.apache.accumulo.core.security.thrift;

import org.apache.thrift.*;
import org.apache.thrift.meta_data.*;
import org.apache.thrift.protocol.*;

@SuppressWarnings("serial")
public class ThriftSecurityException extends Exception implements TBase<ThriftSecurityException,ThriftSecurityException._Fields>, java.io.Serializable,
    Cloneable {
  private static final TStruct STRUCT_DESC = new TStruct("ThriftSecurityException");
  
  private static final TField USER_FIELD_DESC = new TField("user", TType.STRING, (short) 1);
  private static final TField CODE_FIELD_DESC = new TField("code", TType.I32, (short) 2);
  
  public String user;
  /**
   * 
   * @see SecurityErrorCode
   */
  public SecurityErrorCode code;
  
  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements TFieldIdEnum {
    USER((short) 1, "user"),
    /**
     * 
     * @see SecurityErrorCode
     */
    CODE((short) 2, "code");
    
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
        case 1: // USER
          return USER;
        case 2: // CODE
          return CODE;
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
    tmpMap.put(_Fields.USER, new FieldMetaData("user", TFieldRequirementType.DEFAULT, new FieldValueMetaData(TType.STRING)));
    tmpMap.put(_Fields.CODE, new FieldMetaData("code", TFieldRequirementType.DEFAULT, new EnumMetaData(TType.ENUM, SecurityErrorCode.class)));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    FieldMetaData.addStructMetaDataMap(ThriftSecurityException.class, metaDataMap);
  }
  
  public ThriftSecurityException() {}
  
  public ThriftSecurityException(String user, SecurityErrorCode code) {
    this();
    this.user = user;
    this.code = code;
  }
  
  /**
   * Performs a deep copy on <i>other</i>.
   */
  public ThriftSecurityException(ThriftSecurityException other) {
    if (other.isSetUser()) {
      this.user = other.user;
    }
    if (other.isSetCode()) {
      this.code = other.code;
    }
  }
  
  public ThriftSecurityException deepCopy() {
    return new ThriftSecurityException(this);
  }
  
  @Deprecated
  public ThriftSecurityException clone() {
    return new ThriftSecurityException(this);
  }
  
  public String getUser() {
    return this.user;
  }
  
  public ThriftSecurityException setUser(String user) {
    this.user = user;
    return this;
  }
  
  public void unsetUser() {
    this.user = null;
  }
  
  /** Returns true if field user is set (has been asigned a value) and false otherwise */
  public boolean isSetUser() {
    return this.user != null;
  }
  
  public void setUserIsSet(boolean value) {
    if (!value) {
      this.user = null;
    }
  }
  
  /**
   * 
   * @see SecurityErrorCode
   */
  public SecurityErrorCode getCode() {
    return this.code;
  }
  
  /**
   * 
   * @see SecurityErrorCode
   */
  public ThriftSecurityException setCode(SecurityErrorCode code) {
    this.code = code;
    return this;
  }
  
  public void unsetCode() {
    this.code = null;
  }
  
  /** Returns true if field code is set (has been asigned a value) and false otherwise */
  public boolean isSetCode() {
    return this.code != null;
  }
  
  public void setCodeIsSet(boolean value) {
    if (!value) {
      this.code = null;
    }
  }
  
  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
      case USER:
        if (value == null) {
          unsetUser();
        } else {
          setUser((String) value);
        }
        break;
      
      case CODE:
        if (value == null) {
          unsetCode();
        } else {
          setCode((SecurityErrorCode) value);
        }
        break;
    
    }
  }
  
  public void setFieldValue(int fieldID, Object value) {
    setFieldValue(_Fields.findByThriftIdOrThrow(fieldID), value);
  }
  
  public Object getFieldValue(_Fields field) {
    switch (field) {
      case USER:
        return getUser();
        
      case CODE:
        return getCode();
        
    }
    throw new IllegalStateException();
  }
  
  public Object getFieldValue(int fieldId) {
    return getFieldValue(_Fields.findByThriftIdOrThrow(fieldId));
  }
  
  /** Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    switch (field) {
      case USER:
        return isSetUser();
      case CODE:
        return isSetCode();
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
    if (that instanceof ThriftSecurityException)
      return this.equals((ThriftSecurityException) that);
    return false;
  }
  
  public boolean equals(ThriftSecurityException that) {
    if (that == null)
      return false;
    
    boolean this_present_user = true && this.isSetUser();
    boolean that_present_user = true && that.isSetUser();
    if (this_present_user || that_present_user) {
      if (!(this_present_user && that_present_user))
        return false;
      if (!this.user.equals(that.user))
        return false;
    }
    
    boolean this_present_code = true && this.isSetCode();
    boolean that_present_code = true && that.isSetCode();
    if (this_present_code || that_present_code) {
      if (!(this_present_code && that_present_code))
        return false;
      if (!this.code.equals(that.code))
        return false;
    }
    
    return true;
  }
  
  @Override
  public int hashCode() {
    return 0;
  }
  
  public int compareTo(ThriftSecurityException other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }
    
    int lastComparison = 0;
    ThriftSecurityException typedOther = (ThriftSecurityException) other;
    
    lastComparison = Boolean.valueOf(isSetUser()).compareTo(typedOther.isSetUser());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetUser()) {
      lastComparison = TBaseHelper.compareTo(this.user, typedOther.user);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetCode()).compareTo(typedOther.isSetCode());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetCode()) {
      lastComparison = TBaseHelper.compareTo(this.code, typedOther.code);
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
        case 1: // USER
          if (field.type == TType.STRING) {
            this.user = iprot.readString();
          } else {
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case 2: // CODE
          if (field.type == TType.I32) {
            this.code = SecurityErrorCode.findByValue(iprot.readI32());
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
    if (this.user != null) {
      oprot.writeFieldBegin(USER_FIELD_DESC);
      oprot.writeString(this.user);
      oprot.writeFieldEnd();
    }
    if (this.code != null) {
      oprot.writeFieldBegin(CODE_FIELD_DESC);
      oprot.writeI32(this.code.getValue());
      oprot.writeFieldEnd();
    }
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("ThriftSecurityException(");
    sb.append("user:");
    if (this.user == null) {
      sb.append("null");
    } else {
      sb.append(this.user);
    }
    sb.append(", ");
    sb.append("code:");
    if (this.code == null) {
      sb.append("null");
    } else {
      sb.append(this.code);
    }
    sb.append(")");
    return sb.toString();
  }
  
  public void validate() throws TException {
    // check for required fields
  }
  
}
