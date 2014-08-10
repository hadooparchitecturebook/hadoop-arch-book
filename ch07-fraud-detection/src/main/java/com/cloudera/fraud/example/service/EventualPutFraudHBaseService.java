package com.cloudera.fraud.example.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


import com.cloudera.fraud.example.model.ItemSaleEvent;
import com.cloudera.fraud.example.model.ProfilePojo;

public class EventualPutFraudHBaseService extends AbstractFraudHBaseService{

  static BasicFraudHBaseService rootService = new BasicFraudHBaseService();
  static ExecutorService executorService = Executors.newFixedThreadPool(100);
  
  public EventualPutFraudHBaseService() {
    
  }
  
  @Override
  public ProfilePojo[] getProfilesFromHBase(List<Long> userIds)
      throws IOException {
    
    return rootService.getProfilesFromHBase(userIds);
  }

  @Override
  public void updateProfileCountsForSaleInHBase(
      final Long buyerId, 
      final Long sellerId,
      final ItemSaleEvent event) throws IOException, InterruptedException {
    
    executorService.execute(new Runnable() {
      public void run() {
          try {
            rootService.updateProfileCountsForSaleInHBase(buyerId, sellerId, event);
          } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
      }
  });
  }

  @Override
  public void logInProfileInHBase(final long userId, final String ipAddress)
      throws IOException, Exception {
    
    executorService.execute(new Runnable() {
      public void run() {
        try {
          rootService.logInProfileInHBase(userId, ipAddress);
        } catch (IOException e) {
          e.printStackTrace();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
  }

  @Override
  public void createProfile(final long userId, final ProfilePojo pojo, final String ipAddress)
      throws Exception {
    executorService.execute(new Runnable() {
      public void run() {
        try {
          rootService.createProfile(userId, pojo, ipAddress);
        } catch (IOException e) {
          e.printStackTrace();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
    
  }

  @Override
  public void createBulkProfile(ArrayList<ProfileCreatePojo> pojoList)
      throws Exception {
    rootService.createBulkProfile(pojoList);
  }
  
   

}
