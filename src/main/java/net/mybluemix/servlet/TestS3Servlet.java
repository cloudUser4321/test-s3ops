package net.mybluemix.servlet;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.FileUtils;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * Servlet implementation class TestS3Servlet
 */
@WebServlet("/TestS3Servlet")
public class TestS3Servlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public TestS3Servlet() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		//URL fileToSaveToS3AsURL =//getClass().getResource(".\text.txt");
        
        File fileToSaveToS3 =  buildFileForExample();  //FileUtils.toFile(fileToSaveToS3AsURL);
		
		AWSCredentialsProvider awsCredentialsProvider = createAWSCredentialsProvider();
		
		AmazonS3 s3 = AmazonS3ClientBuilder.standard()
				.withRegion(Regions.US_EAST_1).withForceGlobalBucketAccessEnabled(true)                
				.withCredentials(awsCredentialsProvider)		                        
		        .build();
		        
		        
		        
				File fileToReadFromS3 = null;
				//get file
				
				String bucketName = "test-proj-search-1";
				
				try {
					
					/*				DeleteObjectsRequest delObjRequest = new DeleteObjectsRequest(bucketName);
									delObjRequest.setKeys(getAllObjectKeys(bucketName, s3));
									s3.deleteObjects(delObjRequest);
					*/				
					
					List<String> keys = getItemsByFilter(bucketName,s3);
					
					String keyInBucket = fileToSaveToS3.getName() ;
					
					//s3.createBucket(bucketName, region)
					s3.putObject(bucketName, keyInBucket ,  fileToSaveToS3);
					
					
					
					S3Object o = s3.getObject(bucketName, keyInBucket);
					
					
					
					S3ObjectInputStream s3is = o.getObjectContent();
				    
					fileToReadFromS3 = File.createTempFile("copy"+fileToSaveToS3.getName().substring(0, fileToSaveToS3.getName().indexOf(".")), ".txt");//new File("copy_"+keyInBucket);
				    FileOutputStream fos = new FileOutputStream(fileToReadFromS3);
				    byte[] read_buf = new byte[1024];
				    int read_len = 0;
				    while ((read_len = s3is.read(read_buf)) > 0) {
				        fos.write(read_buf, 0, read_len);
				    }
				    s3is.close();
				    fos.close();
				} catch (AmazonServiceException e) {
				    System.err.println(e.getErrorMessage());
				    //System.exit(1);
				} catch (FileNotFoundException e) {
				    System.err.println(e.getMessage());
				    //System.exit(1);
				} catch (IOException e) {
				    System.err.println(e.getMessage());
				    //System.exit(1);
				}
		if (fileToReadFromS3 == null ) {
			
			response.getWriter().write("Error getting item from s3");
		}
		
		response.setContentType("application/octet-stream");
		response.setHeader("Content-Disposition", "attachment; filename=\""
	            + fileToReadFromS3.getName() + "\"");
		
		 response.setContentLength((int) fileToReadFromS3.length());
		    FileInputStream fileInputStream = new FileInputStream(fileToReadFromS3);
		    int i = 0;
		    while ((i = fileInputStream.read()) != -1) {
		       response.getOutputStream().write(i);
		    }
		    fileInputStream.close();
		
	}
	
	
	private List<String> getItemsByFilter(String bucketName,AmazonS3 s3){
		final ListObjectsRequest req = new ListObjectsRequest().withBucketName(bucketName).withPrefix("text");
		ObjectListing result;
		int i =0;
		final List <String> keys = new ArrayList<>();
		do {   
		     result = s3.listObjects(req);
		       List<S3ObjectSummary> objSummaries = result.getObjectSummaries();
		       objSummaries.forEach( (it) ->{  System.out.println("key:"+ it.getKey() + " tag:"+it.getETag());  System.out.println("read from s3:"  + s3.getObject(bucketName,it.getKey()).getKey()); });
		       Iterator<String> keysIt = objSummaries.stream().map(it -> it.getKey() ).iterator();
		       keysIt.forEachRemaining(it -> keys.add(it));
		       result.setNextMarker(result.getMarker() + i++);
		}
		while(result.isTruncated() == true );
		return keys;
	}
	
	private List<KeyVersion> getAllObjectKeys(String bucketName,AmazonS3 s3client){
		List<KeyVersion> keys =	 new ArrayList<KeyVersion>(); 
		try {
	            System.out.println("Listing objects");
	            //final ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucketName);//.withMaxKeys(5);
	            
	            //ListObjectsV2Result result;
	           // do {               
	              ObjectListing result = s3client.listObjects(bucketName);
	               
	               for (S3ObjectSummary objectSummary : 
	                   result.getObjectSummaries()) {
	                     
	            	   /*System.out.println(" - " + objectSummary.getKey() + "  " +
	                           "(size = " + objectSummary.getSize() + 
	                           ")");
	            	   */
	            	   keys.add(new DeleteObjectsRequest.KeyVersion( objectSummary.getKey()));
	               }
	              // System.out.println("Next Continuation Token : " + result.getNextContinuationToken());
	              // req.setContinuationToken(result.getNextContinuationToken());
	            //} while(result.isTruncated() == true ); 
	            
	         } catch (AmazonServiceException ase) {
	            System.out.println("Caught an AmazonServiceException, " +
	            		"which means your request made it " +
	                    "to Amazon S3, but was rejected with an error response " +
	                    "for some reason.");
	            System.out.println("Error Message:    " + ase.getMessage());
	            System.out.println("HTTP Status Code: " + ase.getStatusCode());
	            System.out.println("AWS Error Code:   " + ase.getErrorCode());
	            System.out.println("Error Type:       " + ase.getErrorType());
	            System.out.println("Request ID:       " + ase.getRequestId());
	        } catch (AmazonClientException ace) {
	            System.out.println("Caught an AmazonClientException, " +
	            		"which means the client encountered " +
	                    "an internal error while trying to communicate" +
	                    " with S3, " +
	                    "such as not being able to access the network.");
	            System.out.println("Error Message: " + ace.getMessage());
	        }finally {
				return keys;
			}
	    }
	
	

	private AWSCredentialsProvider createAWSCredentialsProvider() {
		String testMode = System.getProperty("test-mode");
		AWSCredentialsProvider awsCredentialsProvider = null;
		if (testMode  == null){
			return new EnvironmentVariableCredentialsProvider();	
		}
		if (testMode.equals("prod")) {
			return new EnvironmentVariableCredentialsProvider();

		}
		if (testMode.equals("dev")) {
			BasicAWSCredentials awsCreds = new BasicAWSCredentials("*********", "*******");
			return new AWSStaticCredentialsProvider(awsCreds);
		}
		return  new EnvironmentVariableCredentialsProvider();
		
	}
    private File buildFileForExample(){
    	File result = null;
    	String str = "\"Modified Version\" refers to any derivative made by adding to, deleting,"+
"or substituting -- in part or in whole -- any of the components of the"+
"Original Version, by changing formats or by porting the Font Software to a"+
"new environment."+

"\"Author\" refers to any designer, engineer, programmer, technical"+
"writer or other person who contributed to the Font Software."+

"PERMISSION & CONDITIONS"+
"Permission is hereby granted, free of charge, to any person obtaining"+
"a copy of the Font Software, to use, study, copy, merge, embed, modify,"+
"redistribute, and sell modified and unmodified copies of the Font"+
"Software, subject to the following conditions:"+
"\"";
    	try {
		  result =  File.createTempFile("text",".txt");	FileUtils.writeStringToFile(result,str);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return result;
    }
}
