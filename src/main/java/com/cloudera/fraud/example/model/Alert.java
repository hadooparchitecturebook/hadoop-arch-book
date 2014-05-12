package com.cloudera.fraud.example.model;

public class Alert {
  public byte alertLevel;
	public String message;
	
  public Alert(byte alertLevel, String message) {
    super();
    this.alertLevel = alertLevel;
    this.message = message;
  }

  public byte getAlertLevel() {
    return alertLevel;
  }

  public void setAlertLevel(byte alertLevel) {
    this.alertLevel = alertLevel;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }
	

	
}
