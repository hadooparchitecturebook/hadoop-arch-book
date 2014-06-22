package com.cloudera.fraud.example.service;

public class ServiceFactory {
  public static AbstractCacheFraudService initCacheService(String type, AbstractFraudHBaseService hBaseService) {
    if (type.equals("local")) {
      return new LocalCacheFraudService(hBaseService);
    } else {
      throw new RuntimeException("Unknown cache service '" + type + "'");
    }
  }
  
  public static AbstractFraudHBaseService initHBaseService(String type) {
    if (type.equals("basic")) {
      return new BasicFraudHBaseService();
    } else if (type.equals("eventual")) {
      return new EventualPutFraudHBaseService();
    } else {
      throw new RuntimeException("Unknown hbase service '" + type + "'");
    }
  }
}
