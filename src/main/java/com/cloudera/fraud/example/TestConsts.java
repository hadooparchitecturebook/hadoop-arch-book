package com.cloudera.fraud.example;

import java.util.ArrayList;

public class TestConsts {
  public static final ArrayList<String> SAFE_UP_ADDRESS_LIST;
  public static final ArrayList<String> UNSAFE_UP_ADDRESS_LIST;
  
  static {
    SAFE_UP_ADDRESS_LIST = new ArrayList<String>();
    SAFE_UP_ADDRESS_LIST.add("127.0.0.1");
    SAFE_UP_ADDRESS_LIST.add("127.0.0.2");
    SAFE_UP_ADDRESS_LIST.add("127.0.0.3");
    SAFE_UP_ADDRESS_LIST.add("127.0.0.4");
    SAFE_UP_ADDRESS_LIST.add("127.0.0.5");
    SAFE_UP_ADDRESS_LIST.add("127.0.0.6");
    SAFE_UP_ADDRESS_LIST.add("127.0.0.7");
    SAFE_UP_ADDRESS_LIST.add("127.0.0.8");
    SAFE_UP_ADDRESS_LIST.add("127.0.0.9");
    SAFE_UP_ADDRESS_LIST.add("127.0.0.0");
    SAFE_UP_ADDRESS_LIST.add("127.0.1.1");
    SAFE_UP_ADDRESS_LIST.add("127.0.1.2");
    SAFE_UP_ADDRESS_LIST.add("127.0.1.3");
    SAFE_UP_ADDRESS_LIST.add("127.0.1.4");
    SAFE_UP_ADDRESS_LIST.add("127.0.1.5");
    SAFE_UP_ADDRESS_LIST.add("127.0.1.6");
    SAFE_UP_ADDRESS_LIST.add("127.0.1.7");
    SAFE_UP_ADDRESS_LIST.add("127.0.1.8");
    SAFE_UP_ADDRESS_LIST.add("127.0.1.9");
    SAFE_UP_ADDRESS_LIST.add("127.0.1.0");
    SAFE_UP_ADDRESS_LIST.add("127.0.2.1");
    
    UNSAFE_UP_ADDRESS_LIST = new ArrayList<String>();
    UNSAFE_UP_ADDRESS_LIST.add("126.0.0.1");
    UNSAFE_UP_ADDRESS_LIST.add("126.0.0.2");
    UNSAFE_UP_ADDRESS_LIST.add("126.0.0.3");
    UNSAFE_UP_ADDRESS_LIST.add("126.0.0.4");
    UNSAFE_UP_ADDRESS_LIST.add("126.0.0.5");
    
    		
    
    
  }
}
