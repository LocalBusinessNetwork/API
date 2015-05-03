package com.rw.API;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import com.rw.persistence.RWObjectMgr;
import com.rw.persistence.mongoRepo;
import com.rw.repository.RWAPI;


public class RepoMgr extends RWReqHandle {
	static final Logger log = Logger.getLogger(RepoMgr.class.getName());
	private mongoRepo r = null;
	
	public RepoMgr () throws Exception {
	}
	
	public RepoMgr (String userid) throws Exception {
		super(userid);
	}
	
	public RepoMgr(RWObjectMgr context) throws Exception {
		super(context);
	}

	
	public mongoRepo getRepo() throws Exception {
		if ( r == null)
			r = new mongoRepo(getTenantKey());
		return r;
	}
	@RWAPI
	public JSONObject Applet(JSONObject data) throws Exception {
		log.trace( "GetAppletMetaData" + data.get("name").toString());
		return getRepo().GetAppletByName(data.get("name").toString());
	}

	@RWAPI
	public JSONObject BusComp(JSONObject data) throws Exception {
		log.trace( "GetBCMetaData");
		return getRepo().GetBCByName(data.get("name").toString());
	}

	@RWAPI
	public JSONObject BusObj(JSONObject data) throws Exception {
		log.trace( "GetBOMetaData");
		return getRepo().GetBCByName(data.get("name").toString());
	}

	@RWAPI
	public JSONObject ChartApplet(JSONObject data) throws Exception {
		log.trace( "GetChartAppletMetaData" + data.get("name").toString());
		return getRepo().GetChartAppletByName(data.get("name").toString());
	}
	public JSONObject ChartData(JSONObject data) throws Exception {
		log.trace( "GetChartAppletMetaData" + data.get("name").toString());
		return getRepo().GetChartDataByName(data.get("name").toString());
	}
}
