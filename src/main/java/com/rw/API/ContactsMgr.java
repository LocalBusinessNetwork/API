package com.rw.API;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import net.sourceforge.cardme.engine.VCardEngine;
import net.sourceforge.cardme.vcard.VCard;
import net.sourceforge.cardme.vcard.types.EmailType;
import net.sourceforge.cardme.vcard.types.NType;
import net.sourceforge.cardme.vcard.types.OrgType;
import net.sourceforge.cardme.vcard.types.TelType;
import net.sourceforge.cardme.vcard.types.TitleType;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.json.JSONArray;
import org.json.JSONObject;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.QueryBuilder;
import com.mongodb.util.JSON;
import com.rw.persistence.JedisMT;
import com.rw.persistence.RWJBusComp;
import com.rw.persistence.RWObjectMgr;
import com.rw.repository.RWAPI;


public class ContactsMgr extends RWReqHandle  {
	static final Logger log = Logger.getLogger(ContactsMgr.class.getName());

	public ContactsMgr() {
	}
	
	public ContactsMgr(String userid) {
		super(userid);
	}
	
	public ContactsMgr(RWObjectMgr context) throws Exception {
		super(context);
	}
	
	// Contact is just another way of creating a party
	// Additionally, a REFERRAL_CONTACT relationship is created automatically.
	// When a user imports  a contact and if that contact exists in the system as a member already, why can't that member become a partner to the user automatically ? 
	// Would it simplify the chore of invitations ?
	
	@RWAPI
	public JSONObject create(JSONObject data) throws Exception {
		log.trace( "create"); // this is really an upsert
		RWJBusComp bc = app.GetBusObject("Contact").getBusComp("Contact");

		String firstName = data.getString("firstName");
		String lastName = data.getString("lastName");
		String emailAddress = null;
		String partyType = (!data.isNull("partytype"))?data.getString("partytype"):"CONTACT";
		
		// email is a must. If none specified create a dummy one. 
		if ( !data.isNull("emailAddress") ) {
			emailAddress = data.getString("emailAddress").trim();
			if ( emailAddress.isEmpty() ) 
				emailAddress =  firstName.toLowerCase().concat(".").concat(lastName.toLowerCase());
		}
		else 
			emailAddress =  firstName.toLowerCase().concat(".").concat(lastName.toLowerCase());
		
		
		BasicDBObject query = new BasicDBObject();
		
		// see if someone with this email address already exists in the system.
		// It could be someone else's contact or a member
		
		query.put("emailAddress", emailAddress );
		int nRecs = bc.UpsertQuery(query);

		data.put("firstName", getProperName(firstName));
		data.put("lastName", getProperName(lastName));
		data.put("emailAddress", emailAddress);	
		String fullName = firstName.concat(" ").concat(lastName);
		//fullName = fullName.toUpperCase();
		data.put("fullName", fullName);	
		data.put("enrichmentStatus", "NOT_ENRICHED");
		data.put("MLID", 0L);
		String partytype_denorm = "CONTACT";

		if ( nRecs == 0) {
			// No body with email, so create the master for this contact
				bc.NewRecord();
				super.create(data);
				bc.SetFieldValue("parentId", GetPartyId());
				bc.SetFieldValue("partytype", partyType);
				bc.SetFieldValue("fc_enriched", "NO");
				bc.SetFieldValue("wp_enriched", "NO");
				bc.SetFieldValue("originalSource", "user");
				bc.SaveRecord(data);
				
				
		} else {
			partytype_denorm = (String)bc.get("partytype");
			
		}

		String contactId = bc.GetFieldValue("_id").toString();
		
		// kick off Enrichment for this new party
		EnrichParty(contactId);

		// Create a contact relationship with the user.
		// Preserve a private copy of this contact with the user, with the information
		// provided by the user.
		
		setupContactRelatonship(contactId,partytype_denorm, data);
		
		JSONObject retVal = new JSONObject() ;
		retVal.put("data", StringifyDBObject(bc.currentRecord));
		
		return retVal;
	}

	public void setupContactRelatonship(String partnerid,String partytype_denorm, JSONObject data_in) throws Exception {
    	
		JSONObject data = new JSONObject();
	
		data.put("act", "create");
		data.put("userid", getUserId());
		data.put("partnerId",partnerid);
		data.put("type","REFERRAL_CONTACT");
		data.put("partytype_denorm",partytype_denorm);
		
		Iterator<String> keys = data_in.keys();
		
		while( keys.hasNext() ) {
			String key = keys.next();
			String newKey = key;
			//String newKey = key.concat("_prv");//values stored in the partner table have a _prv suffix. 
			data.put(newKey, data_in.get(key));
			//data.put(key, data_in.get(key));
		}
		
		try {
			PartnerMgr pm = new PartnerMgr(this);
			data = pm.handleRequest(data);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.debug("API Error: ", e);
			System.out.println("error on Parnter Mgr setup contact");
		}
    }

	/*
	 * copy the user's relationship with a contact to another user
	 */
	@RWAPI
	public JSONObject copy(JSONObject data) throws Exception {
		log.trace( "copy");
		// Copy this contact from Original Partner to a new Partner
		// id is really a relationship id, that contains the userid and contactid.
		JSONObject retVal = new JSONObject();
		if ( !data.isNull("id")) {
			BasicDBObject query = new BasicDBObject();
			query.put("_id", new ObjectId(data.get("id").toString())); 
			RWJBusComp bc = app.GetBusObject("Partner").getBusComp("Partner");
			int nRecs = bc.UpsertQuery(query);
			if ( nRecs == 1) {
				bc.cloneRecord();
				super.create(data);
				bc.SetFieldValue("userid", getUserId());
				data.remove("id");
				bc.SaveRecord(data);
				retVal.put("data", StringifyDBObject(bc.currentRecord));
			}
		}
		return retVal;
	}

	/*
	 * (non-Javadoc)
	 * @see com.rw.API.RWReqHandle#read(org.json.JSONObject)
	 * Fetch specific contact style record from the Party Master
	 * id is the id of the party master
	 * or textsearch into contacts list - but this should never a use case.
	 * contacts are always referenced from the relationship
	 */
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject read(JSONObject data) throws Exception {
		log.trace( "read");
		RWJBusComp bc = app.GetBusObject("Contact").getBusComp(getShapedBC(data,"Contact"));

		BasicDBObject query = new BasicDBObject();

		JSONObject retVal = new JSONObject() ;

		if ( !data.isNull("id")) {
			Object id = data.get("id");
			query.put("_id", new ObjectId(id.toString())); 
			int nRecs = bc.ExecQuery(query,null);
			if ( nRecs > 0 ) {
				// Contacts may have composite fields
				BasicDBObject cRec = bc.GetCompositeRecord();
				retVal.put("data", StringifyDBObject(cRec));
			}
			else {
				String errorMessage = getMessage("RECORD_NOTFOUND"); 
				log.warn( errorMessage);
				throw new Exception(errorMessage);
			}
		}
		/*  TODO: Remove this, if there are no use cases.
		else { 
			// request for a set of records, filtered by a criteria
			// But, this should never happen. Need to lookinto usecases.
			return textSearch(data);
		}
		*/
		return retVal;
	}

	/*
	 * (non-Javadoc)
	 * @see com.rw.API.RWReqHandle#update(org.json.JSONObject)
	 * update a specific contact style of a party record
	 */
	@RWAPI
	public JSONObject update(JSONObject data) throws Exception {

		log.trace( "update");
		super.update(data);
		
		Object id = data.get("id");

		JSONObject retVal = new JSONObject() ;
		RWJBusComp bc = app.GetBusObject("Contact").getBusComp("Contact");
		
		BasicDBObject query = new BasicDBObject();
        query.put("_id", new ObjectId(id.toString())); 
	
		if (bc.UpsertQuery(query) == 0 ) {
			String errorMessage = getMessage("RECORD_NOTFOUND"); 
			log.warn( errorMessage);
			throw new Exception(errorMessage);
		}
		
		if ( !data.isNull("firstName") || !data.isNull("lastName") ) { 
			String firstNameProper = "";
			String lastNameProper = "";
			if (!data.isNull("firstName")){
				firstNameProper = getProperName(data.getString("firstName"));
			}
			if (!data.isNull("lastName")){
				lastNameProper = getProperName(data.getString("lastName"));
			}
			
			//String fullName =  (!data.isNull("firstName"))?data.getString("firstName").concat(" "):"";
			String fullName = (firstNameProper != "")?firstNameProper.concat(" ").concat(lastNameProper):lastNameProper;
			fullName = fullName.toUpperCase();
			//fullName = (!data.isNull("lastName"))?fullName.concat(data.getString("lastName")):fullName;
			//bc.SetFieldValue("fullName", fullName);
			data.put("firstName",firstNameProper);
			data.put("lastName",lastNameProper);
			data.put("fullName", fullName);
		}
		
		bc.SaveRecord(data);

		// Kick off enrichment
		// TODO : Peter, What updates trigger the enrichemnt ?
		// EnrichParty(id.toString());

		//refresh with latest data, but in a buscomp
		bc.ExecQuery(query);

		// process composite fields.
		BasicDBObject cRec = bc.GetCompositeRecord();
		retVal.put("data", StringifyDBObject(cRec));
		
		return retVal;
	}
	
	// Delete customer just deletes the relationship
	// But the actual customer party object will remain in the system
	// So that all enrichment data can be reused by other users
	// This should also improve our upload performance.
	
	@RWAPI
	public JSONObject delete(JSONObject data) throws Exception {

		log.trace( "delete");
		super.delete(data);

		Object id = data.get("id");
		JSONObject retVal = new JSONObject() ;
		RWJBusComp bc = app.GetBusObject("Partner").getBusComp("Partner");

		BasicDBObject query = new BasicDBObject();

		query.put("userid", getUserId());
		query.put("partnerId", id.toString());
		
		// Let us just make sure we are deleting a contact, not a partner.
		query.put("type", "REFERRAL_CONTACT");
	
		if (bc.ExecQuery(query,null) == 0 ) {
			String errorMessage = getMessage("RECORD_NOTFOUND"); 
			log.warn( errorMessage);
			throw new Exception(errorMessage);
		}

		retVal.put("data", StringifyDBObject(bc.currentRecord));
		bc.DeleteRecord();
		
		return retVal;
	}

	// Delete all my customer objects..
	// Similar to delete, we will just delete the relationships.
	// Actual customer objects will remain in the system.
	
	@RWAPI
	public JSONObject deleteAll(JSONObject data) throws Exception {
		log.trace( "delete all");

		RWJBusComp bc = app.GetBusObject("Partner").getBusComp("Partner");

		BasicDBObject query = new BasicDBObject();

		query.put("userid", getUserId());
		// Let us just make sure we are deleting a contacts, not a partner.
		query.put("type", "REFERRAL_CONTACT");
	
		JSONObject retVal = new JSONObject() ;

		if (bc.ExecQuery(query,null) > 0 ) {
			retVal.put("data", StringifyDBList(bc.recordSet));
			while ( bc.DeleteRecord() ) ;
		}
		else {
			String errorMessage = getMessage("RECORD_NOTFOUND"); 
			log.warn(errorMessage);
			throw new Exception(errorMessage);
		}
		return retVal;
	}

	// This is obsolete..
	// Parties are enriched off line as they get added..
	
	@RWAPI
	public JSONObject EnrichAll(JSONObject data) {
		// RunOffline("ContactsMgr:KickOffEnrichment:" + userid.toString());
		return new JSONObject();
	}

	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject textSearch(JSONObject data) throws Exception {
		log.trace( "textSearch");
		RWJBusComp bc = app.GetBusObject("Contact").getBusComp(getShapedBC(data,"Contact"));
		BasicDBObject query = new BasicDBObject();
		String party_id = GetPartyId();

		// We don't want Morpheus Crow to show up on the results. So, exclude this special account 
		QueryBuilder qm = new QueryBuilder().start().put("_id").notEquals(new ObjectId("519d6cde4728220cb798ec97"));

		JSONObject retVal = new JSONObject() ;
		
		if ( !data.isNull("searchText")) {
			Pattern text = Pattern.compile(data.get("searchText").toString(), Pattern.CASE_INSENSITIVE);
			qm = new QueryBuilder().
					or(new QueryBuilder().start().put("fullName").regex(text).get(),
					new QueryBuilder().put("jobTitle").regex(text).get(),
					new QueryBuilder().put("business").regex(text).get());
			query.putAll(qm.get());
		}

		if (data.has("excludeId")) {
			BasicDBObject excludeId = new BasicDBObject();
			excludeId.put("$ne", new ObjectId(data.getString("excludeId")));
			query.put("_id", excludeId);
		}

		int skip = !data.isNull("skip") ? Integer.parseInt(data.getString("skip")) : -1;
		int limit = !data.isNull("limit")? Integer.parseInt(data.getString("limit")) :-1;
		int nRecs = bc.ExecQuery(query,getSortSpec(data), limit, skip);

		BasicDBList cList = bc.GetCompositeRecordList();
		retVal.put("data", StringifyDBList(cList));
		
		return retVal;
	}
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject importContacts(JSONObject data) throws Exception {
		log.trace( "importContacts");

		JSONArray contacts = data.getJSONArray("contacts");
		
		int totalContacts = contacts.length();
		
		// We shall import 200 contacts at a time, so that user response is 
		// quick as well as machine resource are not tied up by any single user at a given time.
		
		// 200 contacts should go thru with in a second on the production server
		
		int totalImported = totalContacts < 200 ? totalContacts : 200 ;
		
		Long start = System.currentTimeMillis();
		
		for ( int i = 0; i < totalImported; i++) {
			JSONObject c = contacts.getJSONObject(i);
			create(c);
		}

		JSONArray offLineImportable = new JSONArray();				
		for ( int j = totalImported; j < totalContacts; j++) {
			offLineImportable.put(contacts.getJSONObject(j));
		}
		
		int totalScheduled = offLineImportable.length();
		
		if ( totalScheduled > 0 ) {
			JSONObject payload = new JSONObject();
			payload.put("contacts", offLineImportable);
			pushToBackground("importContacts", payload);
		}
		else {
			// We are done with importing, let us dedup
			jedisMt.publish(JedisMT.CONTACT_DEDUP_CHANNEL, getUserId());
		}
		
		JSONObject retVal = new JSONObject() ;
		
		retVal.put("Requested", totalContacts);
		retVal.put("Imported", totalImported );
		retVal.put("Scheduled", totalScheduled );
		
		retVal.put("time", System.currentTimeMillis() - start );
		
		if ( totalImported == totalScheduled )
			CompleteImport(retVal);

		return retVal;

	}
	
	public JSONObject ImportVCards(JSONObject data) throws Exception {
		
		JSONObject retVal = new JSONObject() ;

		if ( inUIMode() ) {
			String pid = pushToBackground("ImportVCards", data);
			jedisMt.set(pid + "_counter", "0");
			retVal.put(pid, pid + "_counter" );
			retVal.put("Imported", 0 );		
			return retVal;
		}

		// get background process id
		String pid = data.getString("_pid");
		
		String existingBucket = data.getString("bucket");
		String fn = data.getString("name");
		if ( !existingBucket.equals("local")) {
			S3DataVault dataVault = new S3DataVault();
			fn = dataVault.S3ObjectToFile(existingBucket, fn);
			dataVault.deleteS3Object(data.getString("bucket"), fn);
		}
		
		File bigVCardFile = new File(fn);
		
		VCardEngine vcardEngine = new VCardEngine();
		List<VCard> vcardsList = vcardEngine.parseMultiple(bigVCardFile);
		retVal.put("Requested", vcardsList.size());
		int totalImported = 0;
		for ( VCard v : vcardsList) {
			JSONObject c = new JSONObject();
			NType n = v.getN();
			if ( n != null ) {
				c.put("firstName", n.getGivenName());
				c.put("lastName", n.getFamilyName());
			}
			else continue; 
				
			OrgType org = v.getOrg();
			if ( org != null )
				c.put("business", getProperName(org.getOrgName()));
			
			List<EmailType> emails = v.getEmails();
			if ( emails != null ) {
				for ( int i = 0; i < emails.size(); i++) {
					c.put("emailAddress" + ( (i > 0) ? String.valueOf(i+1) : "") ,((EmailType)emails.get(i)).getEmail());
				}
			}
			else c.put("emailAddress", n.getGivenName() + "." + n.getFamilyName());
				
			List<TelType> tels = v.getTels();
			if ( tels != null ) {
				int total = tels.size();
				
				if ( total > 0) 
					c.put("workPhone", ( (TelType) tels.get(0)).getTelephone());
				if ( total > 1) 
					c.put("mobilePhone", ( (TelType) tels.get(1)).getTelephone());
				if ( total > 2) 
					c.put("homePhone", ( (TelType) tels.get(2)).getTelephone());
			}
			
			TitleType t = v.getTitle();
			if ( t != null ) {
				c.put("jobTitle", t.getTitle());
			}

			c.put("originalSource", "vcf");

			create(c);
			totalImported++;
			jedisMt.set(pid + "_counter", Integer.toString(totalImported));

		}
		
		// TODO: Send an email notification here
		retVal.put("Imported", totalImported );
		CompleteImport(retVal);
		
		jedisMt.del(pid + "_counter");

		// We are done with importing, let us dedup
		jedisMt.publish(JedisMT.CONTACT_DEDUP_CHANNEL, getUserId());

		return retVal;
		
	}

	public JSONObject phoneCards(JSONObject data) throws Exception {
		
		JSONObject retVal = new JSONObject() ;
		
		if ( inUIMode() ) {
			String pid = pushToBackground("phoneCards", data);
			jedisMt.set(pid + "_counter", "0");
			retVal.put("counter", pid + "_counter" );
			retVal.put("Imported", 0 );		
			return retVal;
		}
		
		// get background process id
		String pid = data.getString("_pid");

		int totalImported = 0;
		BasicDBList contacts = (BasicDBList) JSON.parse(data.getString("contacts"));

		for ( int i=0; i < contacts.size(); i++) {
			BasicDBObject c = (BasicDBObject) contacts.get(i);
			JSONObject d = new JSONObject();
			
			BasicDBObject name = (BasicDBObject) c.get("name");
			
			if ( name.containsField("familyName")) 
				d.put("lastName", name.getString("familyName"));

			if ( name.containsField("givenName")) 
				d.put("firstName", name.getString("givenName"));
			
			if ( c.containsField("phoneNumbers")){
				BasicDBList phones = (BasicDBList) c.get("phoneNumbers");

				if ( phones != null ) {
					System.out.println(phones);

					for (int j =0; j < phones.size(); j++) {
						BasicDBObject px = (BasicDBObject) phones.get(j);
						if ( px.containsField("type")) {
							String type = px.getString("type"); 
							if ( type.equals("home")) {
								d.put("homePhone", px.getString("value"));
							}
							else if ( type.equals("work")) {
								d.put("workPhone", px.getString("value"));
							}
							else if ( type.equals("other")) {
								d.put("mobilePhone", px.getString("value"));
							}
						}
					}
				}
			}

			if ( c.containsField("emails")){
				BasicDBList emails = (BasicDBList) c.get("emails");

				if ( emails != null ) {
					System.out.println(emails);

					for (int j =0; j < emails.size(); j++) {
						BasicDBObject ex = (BasicDBObject) emails.get(j);
						if ( ex.containsField("type")) {
							d.put("emailAddress" + ( (j > 0) ? String.valueOf(j+1) : "") ,ex.getString("value"));
						}
					}
				}
			}

			if ( c.containsField("addresses")){
				BasicDBList a = (BasicDBList) c.get("addresses");
				System.out.println(a);
			}

			if ( c.containsField("organizations")){
				BasicDBList o = (BasicDBList) c.get("organizations");
				System.out.println(o);			
			}
			
			if ( c.containsField("photos")){
				BasicDBList o = (BasicDBList) c.get("photos");
				System.out.println(o);			
			}
			
			d.put("originalSource", "phone");
			
			create(d);
			totalImported++;
			jedisMt.set(pid + "_counter", Integer.toString(totalImported));


		}
		
		retVal.put("Imported", totalImported );		
		CompleteImport(retVal);

		jedisMt.del(pid + "_counter");
		
		// We are done with importing, let us dedup
		jedisMt.publish(JedisMT.CONTACT_DEDUP_CHANNEL, getUserId());

		return retVal;
	}	
	
	public void CompleteImport( JSONObject data ) throws Exception
	{
		Mail m = new Mail(this);
		JSONObject tenantObj = getTenant(null);
		
		data.put("firstName", GetPartyAttr(null, "firstName").toString());
		
		JSONObject header = m.header( tenantObj.getString("AdminEmailAddress"),
				tenantObj.getString("domainName"), tenantObj.getString("AdminEmailAddress"), tenantObj.getString("domainName"),
				"Contacts Import Complete", GetPartyAttr(null, "emailAddress").toString());					

		m.SendHtmlMailMessage(header, getMessage("ET_IMPORT_COMPLETE"), data, null);
	}
}
