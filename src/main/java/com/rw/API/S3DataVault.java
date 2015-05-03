package com.rw.API;

import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;











import java.net.URLEncoder;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.UploadPartRequest;

import javax.imageio.ImageIO;

public class S3DataVault {
	
	private static final Logger log = Logger.getLogger(S3DataVault.class.getName());

	public S3DataVault(){};
	
	public String staticWeb(String existingBucketName, String keyName, InputStream imgFile, long size) throws IOException {
		log.trace("store() : " + existingBucketName + ":" + keyName);
		AmazonS3 s3Client = new AmazonS3Client(new PropertiesCredentials(
        		this.getClass().getResourceAsStream(
        				                  "/AwsCredentials.properties")));        
        List<PartETag> partETags = new ArrayList<PartETag>();
        InitiateMultipartUploadRequest initRequest = new 
             InitiateMultipartUploadRequest(existingBucketName, keyName);
        InitiateMultipartUploadResult initResponse = 
        	                   s3Client.initiateMultipartUpload(initRequest);
        UploadPartRequest uploadRequest = new UploadPartRequest()
	        .withBucketName(existingBucketName).withKey(keyName)
	        .withUploadId(initResponse.getUploadId()).withPartNumber(1)
	        .withInputStream(imgFile)
	        .withPartSize(size);
        partETags.add(
        		s3Client.uploadPart(uploadRequest).getPartETag());
        CompleteMultipartUploadRequest compRequest = new 
                     CompleteMultipartUploadRequest(
                                existingBucketName, 
                                keyName, 
                                initResponse.getUploadId(), 
                                partETags);
        s3Client.completeMultipartUpload(compRequest);
        
        ObjectMetadata md = s3Client.getObjectMetadata(existingBucketName, keyName);
        md.setContentType("text/html; charset=ISO-8859-4");
        
        CopyObjectRequest request = new CopyObjectRequest(existingBucketName, keyName, existingBucketName, keyName)
        .withSourceBucketName(existingBucketName)
        .withSourceKey(keyName)
        .withNewObjectMetadata(md);

        s3Client.copyObject(request);
        s3Client.setObjectAcl(existingBucketName, keyName, CannedAccessControlList.PublicRead);
        
        return "http://".concat(existingBucketName).concat("/").concat(keyName);
		
	}

	public String store(String existingBucketName, String keyName, InputStream imgFile, long size) throws IOException {
		log.trace("store() : " + existingBucketName + ":" + keyName);
		AmazonS3 s3Client = new AmazonS3Client(new PropertiesCredentials(
        		this.getClass().getResourceAsStream(
        				                  "/AwsCredentials.properties")));        
        List<PartETag> partETags = new ArrayList<PartETag>();
        InitiateMultipartUploadRequest initRequest = new 
             InitiateMultipartUploadRequest(existingBucketName, keyName);
        InitiateMultipartUploadResult initResponse = 
        	                   s3Client.initiateMultipartUpload(initRequest);
        UploadPartRequest uploadRequest = new UploadPartRequest()
	        .withBucketName(existingBucketName).withKey(keyName)
	        .withUploadId(initResponse.getUploadId()).withPartNumber(1)
	        .withInputStream(imgFile)
	        .withPartSize(size);
        partETags.add(
        		s3Client.uploadPart(uploadRequest).getPartETag());
        CompleteMultipartUploadRequest compRequest = new 
                     CompleteMultipartUploadRequest(
                                existingBucketName, 
                                keyName, 
                                initResponse.getUploadId(), 
                                partETags);
        s3Client.completeMultipartUpload(compRequest);
        s3Client.setObjectAcl(existingBucketName, keyName, CannedAccessControlList.PublicRead);
        
        String S3root = ResourceBundle.getBundle("referralwire", new Locale("en", "US")).getString("S3ROOT." + existingBucketName );        
        return S3root.concat("/").concat(URLEncoder.encode(keyName,"UTF-8"));
		
	}
	
	public String storeTN(String existingBucketName, String s3Location, String origFileName, String origFileType, InputStream imgFile) throws Exception {
		log.trace("store() : " + existingBucketName + ":" + s3Location);
		
		String keyNameTN = s3Location + File.separator + origFileName + "_tn" ;
		
		String[] filesParts = origFileName.split("\\.");
		String[] filesTypeParts = origFileType.split("/");

        BufferedImage originalImage = ImageIO.read(imgFile);
        
        if ( originalImage == null )
    		throw new Exception("Unsupported File Type : " + origFileName);
        
        int type = originalImage.getType() == 0? BufferedImage.TYPE_INT_ARGB : originalImage.getType();
        
        BufferedImage resizedImage = new BufferedImage(90, 120, type);
    	Graphics2D g = resizedImage.createGraphics();
    	g.drawImage(originalImage, 0, 0, 90, 120, null);
    	g.dispose();
    	
    	File tn = File.createTempFile(origFileName, null);
    	ImageIO.write(resizedImage, filesTypeParts[filesTypeParts.length-1], tn );
    
		AmazonS3 s3Client = new AmazonS3Client(new PropertiesCredentials(
        		this.getClass().getResourceAsStream(
        				                  "/AwsCredentials.properties")));        
        List<PartETag> partETags = new ArrayList<PartETag>();
        InitiateMultipartUploadRequest initRequest = new 
             InitiateMultipartUploadRequest(existingBucketName, keyNameTN);
        InitiateMultipartUploadResult initResponse = 
        	                   s3Client.initiateMultipartUpload(initRequest);
        UploadPartRequest uploadRequest = new UploadPartRequest()
	        .withBucketName(existingBucketName).withKey(keyNameTN)
	        .withUploadId(initResponse.getUploadId()).withPartNumber(1)
	        .withInputStream(new FileInputStream(tn))
	        .withPartSize(tn.length());
        partETags.add(
        		s3Client.uploadPart(uploadRequest).getPartETag());
    	
        CompleteMultipartUploadRequest compRequest = new 
                     CompleteMultipartUploadRequest(
                                existingBucketName, 
                                keyNameTN, 
                                initResponse.getUploadId(), 
                                partETags);

        s3Client.completeMultipartUpload(compRequest);
        s3Client.setObjectAcl(existingBucketName, keyNameTN, CannedAccessControlList.PublicRead);        
        String S3root = ResourceBundle.getBundle("referralwire", new Locale("en", "US")).getString("S3ROOT." + existingBucketName );        
        return S3root.concat("/").concat(URLEncoder.encode(keyNameTN,"UTF-8")) ;

     }
	
	public String S3ObjectToFile(String existingBucketName, String keyName) throws IOException {
		log.trace("S3ObjectToFile() : " + existingBucketName + ":" + keyName);
		AmazonS3 s3Client = new AmazonS3Client(new PropertiesCredentials(
        		this.getClass().getResourceAsStream(
        				                  "/AwsCredentials.properties")));    
		S3Object s3Object = s3Client.getObject(existingBucketName, keyName);
		File tempFile = File.createTempFile(keyName, null);
		IOUtils.copy(s3Object.getObjectContent(), new FileOutputStream(tempFile));
		return tempFile.getAbsolutePath();
     }
	 public void deleteS3Object(String existingBucketName, String keyName) throws IOException {
		log.trace("S3ObjectToFile() : " + existingBucketName + ":" + keyName);
		AmazonS3 s3Client = new AmazonS3Client(new PropertiesCredentials(
        		this.getClass().getResourceAsStream(
        				                  "/AwsCredentials.properties")));    		
		s3Client.deleteObject(existingBucketName, keyName);
     }
}
