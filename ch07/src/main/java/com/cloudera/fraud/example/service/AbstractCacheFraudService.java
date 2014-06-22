package com.cloudera.fraud.example.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;

import com.cloudera.fraud.example.DataModelConsts;
import com.cloudera.fraud.example.model.Alert;
import com.cloudera.fraud.example.model.ItemSaleEvent;
import com.cloudera.fraud.example.model.ProfilePojo;

public abstract class AbstractCacheFraudService {

  static HashMap<String, ProfilePojo> localCache = new HashMap<String, ProfilePojo>();

  AbstractFraudHBaseService hbaseService;
  
  
  
  public AbstractCacheFraudService(AbstractFraudHBaseService hbaseService) {
    this.hbaseService = hbaseService;
  }

  public List<Alert> processEventForAlerts(ItemSaleEvent event) throws IOException {
    ArrayList<Alert> alertList = new ArrayList<Alert>();

    ProfilePurchasePar par = getProfile(event);
    
    checkForAbnormalAction( 
        par.buyer.getLastLogInIpAddress(),
        par.buyer.getLast20LogOnIpAddresses(), 
        par.buyer.getTotalPurchases(), 
        par.buyer.getTotalValueOfPastPurchases(),
        par.buyer.getCurrentLogInPurchasesValue(),
        event.getBuyingId(),
        alertList,
        "purchase");
    
    checkForAbnormalAction( 
        par.seller.getLastLogInIpAddress(),
        par.seller.getLast20LogOnIpAddresses(), 
        par.seller.getTotalSells(), 
        par.seller.getTotalValueOfPastPurchases(),
        par.seller.getCurrentLogInPurchasesValue(),
        event.getSellingId(),
        alertList,
        "purchase");
    
    return alertList;
  }
  
  protected void checkForAbnormalAction(
      String lastLogOnIpAddress,
      Set<String> last20IpAddresses, 
      long actions,
      long totalPastActionValue,
      long valueOfActionInThisLogIn,
      long eventValue,
      List<Alert> alertList,
      String actionType) {
    
    if (!last20IpAddresses.contains(lastLogOnIpAddress)) {
      if (eventValue + valueOfActionInThisLogIn > 2*(totalPastActionValue-valueOfActionInThisLogIn)/actions) {
        alertList.add(new Alert((byte)3, "Action '" + actionType + "' from new Ip Address and greater then 2x average"));
      } else {
        if (last20IpAddresses.size() > 0) {
          alertList.add(new Alert((byte)1, "Action '" + actionType + "' from new Ip Address"));
        }
      }
    } else {
      if (eventValue + valueOfActionInThisLogIn > 4*(totalPastActionValue-valueOfActionInThisLogIn)/actions) {
        alertList.add(new Alert((byte)2, "Action '" + actionType + "' greater then 4x average"));
      }
    }
  }

  protected abstract ProfilePurchasePar getProfile(ItemSaleEvent event) throws IOException;
  
  protected abstract void updateProfileCountsForSale(Long buyerId, Long sellerId, ItemSaleEvent event) throws IOException, InterruptedException;
  
  protected abstract void logInProfile(long userId, String ipAddress) throws IOException, Exception;
  
  public ProfilePojo[] getProfilesFromHBase(List<Long> userIds) throws IOException {
    return hbaseService.getProfilesFromHBase(userIds);
  }
  
  public void updateProfileCountsForSaleInHBase(Long buyerId, Long sellerId, ItemSaleEvent event) throws IOException, InterruptedException {
    hbaseService.updateProfileCountsForSaleInHBase(buyerId, sellerId, event);
  }
  
  public void logInProfileInHBase(long userId, String ipAddress) throws IOException, Exception {
    hbaseService.logInProfileInHBase(userId, ipAddress);
  }
  
  protected class ProfilePurchasePar {
    ProfilePojo buyer;
    ProfilePojo seller;
    
    public ProfilePurchasePar(ProfilePojo buyer, ProfilePojo seller) {
      super();
      this.buyer = buyer;
      this.seller = seller;
    }
  }
  
  
}
