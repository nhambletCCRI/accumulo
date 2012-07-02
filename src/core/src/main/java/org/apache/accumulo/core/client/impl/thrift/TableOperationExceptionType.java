/**
 * Autogenerated by Thrift
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
package org.apache.accumulo.core.client.impl.thrift;

import org.apache.thrift.TEnum;

public enum TableOperationExceptionType implements TEnum {
  EXISTS(0), NOTFOUND(1), OTHER(2);
  
  private final int value;
  
  private TableOperationExceptionType(int value) {
    this.value = value;
  }
  
  /**
   * Get the integer value of this enum value, as defined in the Thrift IDL.
   */
  public int getValue() {
    return value;
  }
  
  /**
   * Find a the enum type by its integer value, as defined in the Thrift IDL.
   * 
   * @return null if the value is not found.
   */
  public static TableOperationExceptionType findByValue(int value) {
    switch (value) {
      case 0:
        return EXISTS;
      case 1:
        return NOTFOUND;
      case 2:
        return OTHER;
      default:
        return null;
    }
  }
}