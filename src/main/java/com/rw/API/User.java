package com.rw.API;

import java.io.Serializable;
import java.security.Principal;

public class User implements Serializable, Principal {
	public String tenant;
	public String username;
	public String userId;
	public User() {};
	
	public User(String uId) { userId = uId; }
	
	public String getName() {
		// TODO Auto-generated method stub
		return userId;
	}
	
}
