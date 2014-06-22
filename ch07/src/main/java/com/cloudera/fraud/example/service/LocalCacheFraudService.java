package com.cloudera.fraud.example.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.cloudera.fraud.example.model.ItemSaleEvent;
import com.cloudera.fraud.example.model.ProfilePojo;

public class LocalCacheFraudService extends AbstractCacheFraudService{
  
  public LocalCacheFraudService(AbstractFraudHBaseService hbaseService) {
    super(hbaseService);
  }

  static ConcurrentMap<Long, ProfilePojo> localCache = new ConcurrentHashMap<Long, ProfilePojo>();
  public static int maxCacheSize = 50000;
  public static int cacheWaterMark = 30000;
  
  protected ProfilePurchasePar getProfile(ItemSaleEvent event) throws IOException {
    
    ProfilePojo buyer = localCache.get(event.getBuyingId());
    ProfilePojo seller = localCache.get(event.getSellingId());
    
    if (buyer == null || seller == null) {
      ArrayList<Long> userIdToFetch = new ArrayList<Long>();
      if (buyer == null) { userIdToFetch.add(event.getBuyingId()); }
      if (seller == null) { userIdToFetch.add(event.getSellingId()); }
    
      ProfilePojo[] profilesFromHBase = getProfilesFromHBase(userIdToFetch);
      int counter = 0;
      if (buyer == null) { 
        buyer = profilesFromHBase[counter++];
        localCache.putIfAbsent(event.getBuyingId(), buyer);
      }
      if (seller == null) { 
        seller = profilesFromHBase[counter++];
        localCache.putIfAbsent(event.getSellingId(), seller);
      }
    }
    trimCache();
    
    return new ProfilePurchasePar(buyer, seller);
  }

  @Override
  protected void updateProfileCountsForSale(Long buyerId, Long sellerId,
      ItemSaleEvent event) throws IOException, InterruptedException {
    
    ProfilePojo buyer = localCache.get(buyerId);
    buyer.incrementCurrentLogInPurchasesValue(event.getItemValue());
    buyer.incrementTotalPurchases(1);
    
    ProfilePojo seller = localCache.get(sellerId);
    seller.incrementCurrentLogInSellsValue(event.getItemValue());
    seller.incrementTotalSells(1);
    
    updateProfileCountsForSaleInHBase(buyerId, sellerId, event);
  }

  @Override
  protected void logInProfile(long userId, String ipAddress) throws Exception {
    logInProfileInHBase(userId, ipAddress);
  }
  
  protected void trimCache() {
    while (localCache.size() > cacheWaterMark) {
      synchronized(localCache) {
        long currentTime = System.currentTimeMillis();
        ArrayList<Long> deleteList = new ArrayList<Long>();
        for (Entry<Long, ProfilePojo> entry: localCache.entrySet()) {
          if (currentTime - entry.getValue().getCreatedTimeStamp() > 10000) {
            deleteList.add(entry.getKey());
          }
        }
        for (Long rowKey: deleteList) {
          localCache.remove(rowKey);
        }
      }
    } 

    while (localCache.size() > maxCacheSize) {
      synchronized(localCache) {
        Iterator<Long> it = localCache.keySet().iterator();
        ArrayList<Long> deleteList = new ArrayList<Long>();
        
        for (int i = 0; i < localCache.size()-maxCacheSize; i++) {
          deleteList.add(it.next());
        }
        
        for (Long rowKey: deleteList) {
          localCache.remove(rowKey);
        }
      }
    }
    
  }
}
