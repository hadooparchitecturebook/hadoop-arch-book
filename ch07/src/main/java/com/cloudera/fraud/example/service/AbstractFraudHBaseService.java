package com.cloudera.fraud.example.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;

import com.cloudera.fraud.example.model.ItemSaleEvent;
import com.cloudera.fraud.example.model.ProfilePojo;

public abstract class AbstractFraudHBaseService {
  
  protected byte[] generateProfileRowKey(long userId) {
    //This is a human readable rowkey
    //Optimized for teaching not space or performance
    
    int salt = (int)userId % 1000;
    
    return Bytes.toBytes(StringUtils.leftPad(Integer.toString(salt), 0, "0") + "-" + userId );
  }
  
  public abstract ProfilePojo[] getProfilesFromHBase(List<Long> userIds) throws IOException;
  
  public abstract void updateProfileCountsForSaleInHBase(Long buyerId, Long sellerId, ItemSaleEvent event) throws IOException, InterruptedException;
  
  public abstract void logInProfileInHBase(long userId, String ipAddress) throws IOException, Exception;

  public abstract void createProfile(long userId, ProfilePojo pojo, String ipAddress)
      throws Exception;
  
  public abstract void createBulkProfile(ArrayList<ProfileCreatePojo> pojoList)
      throws Exception;
  
  public static class ProfileCreatePojo {
    long userId;
    ProfilePojo pojo;
    String ipAddress;
    
    public ProfileCreatePojo(long userId, ProfilePojo pojo, String ipAddress) {
      super();
      this.userId = userId;
      this.pojo = pojo;
      this.ipAddress = ipAddress;
    }

    public long getUserId() {
      return userId;
    }

    public ProfilePojo getPojo() {
      return pojo;
    }

    public String getIpAddress() {
      return ipAddress;
    }
    
    
  }
  
}
