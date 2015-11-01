package org.rundeck.plugins;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.dtolabs.rundeck.core.logging.ExecutionFileStorageException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;

import java.io.*;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * $INTERFACE is ... User: greg Date: 6/11/13 Time: 1:59 PM
 */
@RunWith(JUnit4.class)
public class S3LogFileStoragePluginTest {

    final String DEFAULT_FILETYPE = "rdlog";

    private Map<String, Object> testContext;
    private AmazonS3 amazonS3;
    private S3LogFileStoragePlugin testPlugin;
    private OutputStream testOutputStream;
    private InputStream testInputStream;

    @Before
    public void before() throws IOException {

        testContext = new HashMap<String, Object>();
        testContext.put("execid", "testexecid");
        testContext.put("project", "testproject");
        testContext.put("url", "http://rundeck:4440/execution/5/show");
        testContext.put("serverUrl", "http://rundeck:4440");
        testContext.put("serverUUID", "123");

        amazonS3 = mock(AmazonS3.class);

        testPlugin = new S3LogFileStoragePlugin() {
            @Override
            protected AmazonS3 createAmazonS3Client(AWSCredentials awsCredentials) {
                return amazonS3;
            }

            @Override
            protected AmazonS3 createAmazonS3Client() {
                return amazonS3;
            }
        };
        testPlugin.setPath(S3LogFileStoragePlugin.DEFAULT_PATH_FORMAT);
        testPlugin.setRegion(S3LogFileStoragePlugin.DEFAULT_REGION);
        testPlugin.setBucket("testBucket");
        testPlugin.initialize(testContext);

        testOutputStream = mock(OutputStream.class);
        testInputStream = mock(InputStream.class);
        when(testInputStream.read(any(byte[].class), anyInt(), anyInt())).thenReturn(1, -1);
    }

    @Test
    public void expandPathLeadingSlashIsRemoved() {
        assertEquals("monkey", S3LogFileStoragePlugin.expandPath("/monkey", testContext));
    }

    @Test
    public void expandPathMultiSlashRemoved() {
        assertEquals("monkey/test/blah", S3LogFileStoragePlugin.expandPath("/monkey//test///blah", testContext));
    }

    @Test
    public void expandExecId() {
        assertEquals("monkey/testexecid/blah", S3LogFileStoragePlugin.expandPath("monkey/${job.execid}/blah", testContext));
    }

    @Test
    public void expandProject() {
        assertEquals("monkey/testproject/blah", S3LogFileStoragePlugin.expandPath("monkey/${job.project}/blah", testContext));
    }

    @Test
    public void missingKey() {
        assertEquals("monkey/blah", S3LogFileStoragePlugin.expandPath("monkey/${job.id}/blah", testContext));
    }

    @Test
    public void expandJobId() {
        // setup
        testContext.put("id", "testjobid");
        // expect
        assertEquals("monkey/testjobid/blah", S3LogFileStoragePlugin.expandPath("monkey/${job.id}/blah", testContext));
    }

    @Test
    public void initializeUnsetCredentialsAccessKey() {

        testPlugin.setAWSSecretKey("blah");
        try {
            testPlugin.initialize(testContext);
            fail("Should throw an exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("must both be configured"));
            assertTrue(e.getMessage().contains("AWSAccessKeyId"));
            assertTrue(e.getMessage().contains("AWSSecretKey"));
        }
    }

    @Test
    public void initializeUnsetCredentialsSecretKey() {

        testPlugin.setAWSAccessKeyId("blah");
        try {
            testPlugin.initialize(testContext);
            fail("Should thrown exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("must both be configured"));
            assertTrue(e.getMessage().contains("AWSAccessKeyId"));
            assertTrue(e.getMessage().contains("AWSSecretKey"));
        }
    }

    @Test
    public void initializeCredentialsFileDoesNotExist() {

        testPlugin.setAWSCredentialsFile("/blah/file/does/not/exist");
        try {
            testPlugin.initialize(testContext);
            fail("Should thrown exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Credentials file does not exist or cannot be read"));
        }
    }

    @Test
    public void initializeCredentialsFileMissingCredentials() throws IOException {
        Properties p = new Properties();
        p.setProperty("a", "b");
        File t = File.createTempFile("test-credentials", ".properties");
        t.deleteOnExit();
        p.store(new FileOutputStream(t), "test");
        testPlugin.setAWSCredentialsFile(t.getAbsolutePath());
        try {
            testPlugin.initialize(testContext);
            fail("Should throw an exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("doesn't contain the expected properties 'accessKey' and 'secretKey'."));
        }
    }

    @Test
    public void initializeCredentialsFileMissingSecretKey() throws IOException {

        Properties p = new Properties();
        p.setProperty("a", "b");
        p.setProperty("accessKey", "c");
        File t = File.createTempFile("test-credentials", ".properties");
        t.deleteOnExit();
        p.store(new FileOutputStream(t), "test");
        testPlugin.setAWSCredentialsFile(t.getAbsolutePath());
        try {
            testPlugin.initialize(testContext);
            fail("Should thrown exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("doesn't contain the expected properties 'accessKey' and 'secretKey'."));
        }
    }

    @Test
    public void initializeCredentialsFileMissingAccessKey() throws IOException {

        Properties p = new Properties();
        p.setProperty("a", "b");
        p.setProperty("secretKey", "c");
        File t = File.createTempFile("test-credentials", ".properties");
        t.deleteOnExit();
        p.store(new FileOutputStream(t), "test");
        testPlugin.setAWSCredentialsFile(t.getAbsolutePath());
        try {
            testPlugin.initialize(testContext);
            fail("Should thrown exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("doesn't contain the expected properties 'accessKey' and 'secretKey'."));
        }
    }

    @Test
    public void initializeValidCredentials() {

        testPlugin.setBucket("blah");
        testPlugin.setAWSAccessKeyId("blah");
        testPlugin.setAWSSecretKey("blah");
        testPlugin.initialize(testContext);
        verify(amazonS3, atLeastOnce()).setRegion(any(Region.class));
    }

    @Test
    public void initializeValidCredentialsDefault() {

        testPlugin.setBucket("blah");
        testPlugin.initialize(testContext);
        verify(amazonS3, atLeastOnce()).setRegion(any(Region.class));
    }

    @Test
    public void initializeValidCredentialsFile() throws IOException {

        testPlugin.setBucket("blah");
        Properties p = new Properties();
        p.setProperty("accessKey", "b");
        p.setProperty("secretKey", "c");
        File t = File.createTempFile("test-credentials", ".properties");
        t.deleteOnExit();
        p.store(new FileOutputStream(t), "test");
        testPlugin.setAWSCredentialsFile(t.getAbsolutePath());
        testPlugin.initialize(testContext);
        verify(amazonS3, atLeastOnce()).setRegion(any(Region.class));
    }

    @Test
    public void initializeInvalidRegion() {

        testPlugin.setBucket("blah");
        testPlugin.setAWSAccessKeyId("blah");
        testPlugin.setAWSSecretKey("blah");
        testPlugin.setRegion("mulklahoma");
        try {
            testPlugin.initialize(testContext);
            fail("Expected failure");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Region was not found"));
        }
    }

    @Test
    public void initializeInvalidBucket() {

        testPlugin.setBucket("");
        testPlugin.setAWSAccessKeyId("blah");
        testPlugin.setAWSSecretKey("blah");
        try {
            testPlugin.initialize(testContext);
            fail("Expected failure");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("bucket was not set"));
        }
    }

    @Test
    public void initializeNullBucket() {

        testPlugin.setBucket(null);
        testPlugin.setAWSAccessKeyId("blah");
        testPlugin.setAWSSecretKey("blah");
        try {
            testPlugin.initialize(testContext);
            fail("Expected failure");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("bucket was not set"));
        }
    }

    @Test
    public void initializeNullPath() {

        testPlugin.setBucket("asdf");
        testPlugin.setPath(null);
        testPlugin.setAWSAccessKeyId("blah");
        testPlugin.setAWSSecretKey("blah");
        try {
            testPlugin.initialize(testContext);
            fail("Expected failure");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("path was not set"));
        }
    }

    @Test
    public void initializeInvalidPath() {

        testPlugin.setBucket("basdf");
        testPlugin.setPath("");
        testPlugin.setAWSAccessKeyId("blah");
        testPlugin.setAWSSecretKey("blah");
        try {
            testPlugin.initialize(testContext);
            fail("Expected failure");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("path was not set"));
        }
    }

    @Test
    public void initializeInvalidPathNoExecID() {

        testPlugin.setBucket("basdf");
        testPlugin.setPath("blah/logs");
        testPlugin.setAWSAccessKeyId("blah");
        testPlugin.setAWSSecretKey("blah");
        try {
            testPlugin.initialize(testContext);
            fail("Expected failure");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("path must contain ${job.execid} or end with /"));
        }
    }

    @Test
    public void initializePathNoExecIDWithSlash() {

        testPlugin.setBucket("basdf");
        testPlugin.setPath("blah/logs/");
        testPlugin.setAWSAccessKeyId("blah");
        testPlugin.setAWSSecretKey("blah");
        testPlugin.initialize(testContext);
    }

    @Test
    public void initializePathWithExecIDEndsWithSlash() {

        testPlugin.setBucket("basdf");
        testPlugin.setPath("blah/${job.execid}/");
        testPlugin.setAWSAccessKeyId("blah");
        testPlugin.setAWSSecretKey("blah");
        try {
            testPlugin.initialize(testContext);
            fail("Expected failure");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("path must not end with /"));
        }
    }

    @Test
    public void initializePathWithExecIDValid() {

        testPlugin.setBucket("basdf");
        testPlugin.setPath("blah/${job.execid}.blah");
        testPlugin.setAWSAccessKeyId("blah");
        testPlugin.setAWSSecretKey("blah");
        testPlugin.initialize(testContext);
    }

    @Test
    public void isAvailable404() throws ExecutionFileStorageException {
        // setup
        AmazonS3Exception ase = new AmazonS3Exception("test NOT Found");
        ase.setStatusCode(404);
        ase.setRequestId("requestId");
        ase.setExtendedRequestId("extendedRequestId");
        when(amazonS3.getObjectMetadata(any(GetObjectMetadataRequest.class))).thenThrow(ase);
        // expect
        assertFalse(testPlugin.isAvailable(DEFAULT_FILETYPE));
    }

    @Test
    public void isAvailableOk() throws ExecutionFileStorageException {
        // setup
        when(amazonS3.getObjectMetadata(any(GetObjectMetadataRequest.class))).thenReturn(new ObjectMetadata());
        assertTrue(testPlugin.isAvailable(DEFAULT_FILETYPE));
    }

    @Test
    public void isAvailableS3Exception() {
        // setup
        AmazonS3Exception ase = new AmazonS3Exception("blah");
        ase.setRequestId("requestId");
        ase.setExtendedRequestId("extendedRequestId");
        when(amazonS3.getObjectMetadata(any(GetObjectMetadataRequest.class))).thenThrow(ase);
        // expect
        try {
            testPlugin.isAvailable(DEFAULT_FILETYPE);
            fail("Should throw an exception");
        } catch (ExecutionFileStorageException e) {
            assertTrue(e.getMessage().contains("blah"));
        }
    }

    @Test
    public void isAvailableClientException() {
        // setup
        AmazonClientException ase = new AmazonClientException("blah");
        when(amazonS3.getObjectMetadata(any(GetObjectMetadataRequest.class))).thenThrow(ase);
        // expect
        try {
            testPlugin.isAvailable(DEFAULT_FILETYPE);
            fail("Should throw an exception");
        } catch (ExecutionFileStorageException e) {
            assertEquals("blah", e.getMessage());
        }
    }

    @Test
    public void storeClientException() throws IOException {
        // setup
        AmazonClientException ase = new AmazonClientException("putObject");
        when(amazonS3.putObject(any(PutObjectRequest.class))).thenThrow(ase);
        // expect
        boolean result = false;
        try {
            result = testPlugin.store(DEFAULT_FILETYPE, null, 0, null);
            fail("should throw an exception");
        } catch (ExecutionFileStorageException e) {
            assertEquals("putObject", e.getMessage());
        }
    }

    @Test
    public void storeS3Exception() throws IOException {
        // setup
        AmazonClientException ase = new AmazonS3Exception("putObject");
        when(amazonS3.putObject(any(PutObjectRequest.class))).thenThrow(ase);
        // expect
        boolean result = false;
        try {
            result = testPlugin.store(DEFAULT_FILETYPE, null, 0, null);
            fail("should throw an exception");
        } catch (ExecutionFileStorageException e) {
            assertTrue(e.getMessage().contains("putObject"));
        }
    }

    @Test
    public void storeMetadata() throws IOException, ExecutionFileStorageException {
        // setup
        ArgumentCaptor<PutObjectRequest> putObjectRequestArgumentCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        when(amazonS3.putObject(putObjectRequestArgumentCaptor.capture())).thenReturn(new PutObjectResult());
        Date lastModified = new Date();
        int length = 123;
        boolean result = false;
        // when
        result = testPlugin.store(DEFAULT_FILETYPE, null, length, lastModified);
        // then
        assertTrue(result);
        PutObjectRequest putObjectRequest = putObjectRequestArgumentCaptor.getValue();
        assertEquals(length, putObjectRequest.getMetadata().getContentLength());
        assertEquals(lastModified, putObjectRequest.getMetadata().getLastModified());
        Map<String, String> userMetadata = putObjectRequest.getMetadata().getUserMetadata();
        assertEquals(5, userMetadata.size());
        assertEquals(testContext.get("execid"), userMetadata.get("rundeck.execid"));
        assertEquals(testContext.get("project"), userMetadata.get("rundeck.project"));
        assertEquals(testContext.get("url"), userMetadata.get("rundeck.url"));
        assertEquals(testContext.get("serverUrl"), userMetadata.get("rundeck.serverUrl"));
        assertEquals(testContext.get("serverUUID"), userMetadata.get("rundeck.serverUUID"));
    }

    @Test
    public void retrieve() throws IOException, ExecutionFileStorageException {
        // setup
        S3Object s3Object = new S3Object();
        s3Object.setObjectContent(testInputStream);
        ArgumentCaptor<String> bucketNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        when(amazonS3.getObject(bucketNameCaptor.capture(), keyCaptor.capture())).thenReturn(s3Object);

        // when
        boolean result = testPlugin.retrieve(DEFAULT_FILETYPE, testOutputStream);

        // then
        assertTrue(result);
        assertEquals("testBucket", bucketNameCaptor.getValue());
        assertEquals("project/testproject/testexecid.rdlog", keyCaptor.getValue());
        verify(testInputStream, atLeastOnce()).read(any(byte[].class), anyInt(), anyInt());
        verify(testInputStream, times(1)).close();
        verify(testOutputStream, atLeastOnce()).write(any(byte[].class), anyInt(), anyInt());
        verify(testOutputStream, never()).close();
    }

    @Test
    public void retrieveClientException() throws IOException {
        // setup
        doThrow(new IOException("testOutputStream.writeIOException")).when(testOutputStream).write(any(byte[].class), anyInt(), anyInt());
        ArgumentCaptor<String> bucketNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        when(amazonS3.getObject(bucketNameCaptor.capture(), keyCaptor.capture())).thenThrow(new AmazonClientException("getObject"));

        boolean result = false;
        try {
            result = testPlugin.retrieve(DEFAULT_FILETYPE, testOutputStream);
            fail("should throw");
        } catch (ExecutionFileStorageException e) {
            assertEquals("getObject", e.getMessage());
        }
        assertEquals("testBucket", bucketNameCaptor.getValue());
        assertEquals("project/testproject/testexecid.rdlog", keyCaptor.getValue());
        verify(testOutputStream, never()).write(any(byte[].class), anyInt(), anyInt());
        verify(testOutputStream, never()).close();
    }

    @Test
    public void retrieveS3Exception() throws IOException {
        ArgumentCaptor<String> bucketNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        when(amazonS3.getObject(bucketNameCaptor.capture(), keyCaptor.capture())).thenThrow(new AmazonS3Exception("getObject"));

        boolean result = false;
        try {
            result = testPlugin.retrieve(DEFAULT_FILETYPE, testOutputStream);
            fail("should throw");
        } catch (ExecutionFileStorageException e) {
            assertTrue(e.getMessage().contains("getObject"));
        }
        assertEquals("testBucket", bucketNameCaptor.getValue());
        assertEquals("project/testproject/testexecid.rdlog", keyCaptor.getValue());
        verify(testOutputStream, never()).write(any(byte[].class), anyInt(), anyInt());
        verify(testOutputStream, never()).close();
    }

    @Test
    public void retrieveInputIOException() throws IOException, ExecutionFileStorageException {
        S3Object s3Object = new S3Object();
        s3Object.setObjectContent(testInputStream);
        doThrow(new IOException("testInputStream.readIOException")).when(testInputStream).read(any(byte[].class), anyInt(), anyInt());
        ArgumentCaptor<String> bucketNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        when(amazonS3.getObject(bucketNameCaptor.capture(), keyCaptor.capture())).thenReturn(s3Object);

        boolean result = false;
        try {
            result = testPlugin.retrieve(DEFAULT_FILETYPE, testOutputStream);
            fail("should throw exception");
        } catch (IOException e) {
            assertEquals("testInputStream.readIOException", e.getMessage());
        }
        assertEquals("testBucket", bucketNameCaptor.getValue());
        assertEquals("project/testproject/testexecid.rdlog", keyCaptor.getValue());
        verify(testInputStream, atLeastOnce()).read(any(byte[].class), anyInt(), anyInt());
        verify(testInputStream, times(1)).close();
        verify(testOutputStream, never()).write(any(byte[].class), anyInt(), anyInt());
        verify(testOutputStream, never()).close();
    }

    @Test
    public void retrieveOutputIOException() throws IOException, ExecutionFileStorageException {
        S3Object s3Object = new S3Object();
        s3Object.setObjectContent(testInputStream);
        when(testInputStream.read()).thenReturn(-1);
        doThrow(new IOException("testOutputStream.writeIOException")).when(testOutputStream).write(any(byte[].class), anyInt(), anyInt());
        ArgumentCaptor<String> bucketNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        when(amazonS3.getObject(bucketNameCaptor.capture(), keyCaptor.capture())).thenReturn(s3Object);

        boolean result = false;
        try {
            result = testPlugin.retrieve(DEFAULT_FILETYPE, testOutputStream);
            fail("should throw exception");
        } catch (IOException e) {
            assertEquals("testOutputStream.writeIOException", e.getMessage());
        }
        assertEquals("testBucket", bucketNameCaptor.getValue());
        assertEquals("project/testproject/testexecid.rdlog", keyCaptor.getValue());
        verify(testInputStream, atLeastOnce()).read(any(byte[].class), anyInt(), anyInt());
        verify(testInputStream, times(1)).close();
        verify(testOutputStream, atLeastOnce()).write(any(byte[].class), anyInt(), anyInt());
        verify(testOutputStream, never()).close();
    }

}
