package com.rw.API;

import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.json.JSONArray;
import org.json.JSONObject;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBRef;
import com.mongodb.QueryBuilder;
import com.rw.persistence.RWJApplication;
import com.rw.persistence.RWJBusComp;
import com.rw.persistence.RWJBusObj;
import com.rw.persistence.RWObjectMgr;
import com.rw.persistence.mongoStore;
import com.rw.repository.RWAPI;


@SuppressWarnings("unused")
public class CronJobMgr extends RWReqHandle  {
	static final Logger log = Logger.getLogger(CronJobMgr.class.getName());

	public CronJobMgr() {
	}
	
	public CronJobMgr(String userid) {
		super(userid);
	}
	
	public CronJobMgr(RWObjectMgr context) throws Exception {
		super(context);
	}
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public void run() throws Exception {
		
		RfrlMgr r = new RfrlMgr(this);
		r.cron();
		
		UserMgr u = new UserMgr(this);
		u.cron();
		BusinessMgr b = new BusinessMgr(this);
		b.cron();
		CFBOutboxMgr cfb = new CFBOutboxMgr(this);
		cfb.cron();
		EventMgr e = new EventMgr(this);
		e.cron();
	}
	
}
