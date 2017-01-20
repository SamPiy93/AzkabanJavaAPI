package services;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import errors.AzkabanApiException;
import locks.ReadWriteLockProvider;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.ssl.SSLContexts;

import javax.net.ssl.SSLContext;
import java.io.*;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of azkaban API
 */
@Singleton
public class AzkabanApiImplimentation implements AzkabanApi {
    /**
     * URL should be changed to the azkaban web server's url
     */
    private static final String URL = "https://10.101.16.24:8043/";
    private static final String URL_MANAGER = URL+"manager";
    private static final String URL_SCHEDULE = URL+"schedule";
    private static final String URL_EXECUTOR = URL+"executor";
    private static final String HTTP_HEADER_XML_HTTP_REQUEST_HEADER_NAME = "X-Requested-With";
    private static final String HTTP_HEADER_XML_HTTP_REQUEST_HEADER_VALUE = "XMLHttpRequest";
    private static final String HTTP_HEADER_CONTENT_TYPE_HEADER_NAME = "Content-Type";
    private static final String HTTP_HEADER_CONTENT_TYPE_HEADER_VALUE = "application/x-www-form-urlencoded";
    private static final String AUTHENTICATION_ERROR_MESSAGE = "Azkaban authentication error occurred!";
    private static final String AZKABAN_PROJECT_CREATION_ERROR_MESSAGE = "Error occurred while creating Azkaban project on server side!";
    private static final String SSL_CONNECTION_FAILURE_ERROR_MESSAGE = "SSL connection aborted!";
    private static final String PROJECT_UPLOAD_ERROR_MESSAGE = "Error occurred while uploading the project to Azkaban server";
    private static final String FLOW_SCHEDULE_ERROR_MESSAGE = "Error occurred while scheduling the flow/s of the project";
    private static final String FLOW_CANCELLATION_ERROR_MESSAGE = "Error occurred while cancelling the project flow execution";
    private static final String EXECUTION_FETCHING_ERROR_MESSAGE = "Error occurred while fetching executions for a particular flow";
    private static final String FLOW_FETCHING_ERROR_MESSAGE = "Error occurred while fetching flows for a particular job";
    private static final String UNSCHEDULING_ERROR_MESSAGE = "Error occurred while trying to unschedule a flow";
    private static final String EXECUTE_FLOW_ERROR_MESSAGE = "Error occurred while trying to execute a flow";
    private static final String SSL_ERROR_MESSAGE = "Error occurred while creating SSL context!";
    private static final String ENCODE_NOT_SUPPORT_ERROR_MESSAGE = "Default encoding not supported!";
    private static final String READ_ERROR_MESSAGE = "I/O error occurred while reading from stream";
    private String sessionId;
    private HttpPost httpPost;
    private HttpGet httpGet;
    private HttpResponse httpResponse;
    private List<NameValuePair> urlParams;
    @Inject
    ReadWriteLockProvider readWriteLockProvider;
    @Override
    public void authenticate(String username, String password) throws AzkabanApiException {
        httpPost = new HttpPost(URL);
        httpPost.setHeader(HTTP_HEADER_XML_HTTP_REQUEST_HEADER_NAME, HTTP_HEADER_XML_HTTP_REQUEST_HEADER_VALUE);
        httpPost.setHeader(HTTP_HEADER_CONTENT_TYPE_HEADER_NAME, HTTP_HEADER_CONTENT_TYPE_HEADER_VALUE);
        urlParams = new ArrayList<>();
        urlParams.add(new BasicNameValuePair("action", "login"));
        urlParams.add(new BasicNameValuePair("username", username));
        urlParams.add(new BasicNameValuePair("password", password));
        httpResponse = postContent(httpPost, urlParams);
        String result = getResponse(httpResponse);
        if(checkError(result).has("error")) {
            throw new AzkabanApiException(AUTHENTICATION_ERROR_MESSAGE);
        }
        try {
            readWriteLockProvider.acquireWriteLock();
            this.sessionId = new JsonParser().parse(result).getAsJsonObject().get("session.id").getAsString();
        } finally {
            readWriteLockProvider.releaseWriteLock();
        }
    }

    @Override
    public void createProject(String name, String description) throws AzkabanApiException {
        httpPost = new HttpPost(URL_MANAGER);
        httpPost.setHeader(HTTP_HEADER_XML_HTTP_REQUEST_HEADER_NAME, HTTP_HEADER_XML_HTTP_REQUEST_HEADER_VALUE);
        httpPost.setHeader(HTTP_HEADER_CONTENT_TYPE_HEADER_NAME, HTTP_HEADER_CONTENT_TYPE_HEADER_VALUE);
        urlParams = new ArrayList<>();
        urlParams.add(new BasicNameValuePair("action", "create"));
        try {
            readWriteLockProvider.acquireReadLock();
            urlParams.add(new BasicNameValuePair("session.id", sessionId));
        } finally {
            readWriteLockProvider.releaseReadLock();
        }
        urlParams.add(new BasicNameValuePair("name", name));
        urlParams.add(new BasicNameValuePair("description", description));
        httpResponse = postContent(httpPost, urlParams);
        String result = getResponse(httpResponse);
        if(!parseCreateResponseJson(result).get("status").getAsString().equalsIgnoreCase("success")){
            throw new AzkabanApiException(AZKABAN_PROJECT_CREATION_ERROR_MESSAGE);
        }
    }

    @Override
    public void uploadProject(String name, String projectPath, String zipFileName) throws AzkabanApiException {
        httpPost = new HttpPost(URL_MANAGER);
        String filePath = projectPath.concat(zipFileName.concat(".zip"));
        File file = new File(filePath);
        try {
            readWriteLockProvider.acquireReadLock();
            HttpEntity httpEntity = MultipartEntityBuilder.create()
                    .addTextBody("session.id", this.sessionId)
                    .addTextBody("ajax","upload")
                    .addBinaryBody("file", file, ContentType.create("application/zip"), file.getName())
                    .addTextBody("project",name)
                    .build();
            httpPost.setEntity(httpEntity);
        } finally {
            readWriteLockProvider.releaseReadLock();
        }
        httpResponse = null;
        try {
            httpResponse = sslauthenticatedclient().execute(httpPost);
        } catch (IOException e) {
            throw new AzkabanApiException(SSL_CONNECTION_FAILURE_ERROR_MESSAGE, e);
        }
        String result = getResponse(httpResponse);
        if(httpResponse.getStatusLine().getStatusCode() == 200 && parseCreateResponseJson(result).get("error") != null){
            throw new AzkabanApiException(PROJECT_UPLOAD_ERROR_MESSAGE);
        }
    }

    @Override
    public void scheduleFlow(String name, String projectId, String flowName, String scheduleTime, String scheduleDate, boolean isrecuring, String period) throws AzkabanApiException {
        httpPost = new HttpPost(URL_SCHEDULE);
        httpPost.setHeader(HTTP_HEADER_XML_HTTP_REQUEST_HEADER_NAME, HTTP_HEADER_XML_HTTP_REQUEST_HEADER_VALUE);
        httpPost.setHeader(HTTP_HEADER_CONTENT_TYPE_HEADER_NAME, HTTP_HEADER_CONTENT_TYPE_HEADER_VALUE);
        urlParams = new ArrayList<>();
        urlParams.add(new BasicNameValuePair("ajax", "scheduleFlow"));
        try {
            readWriteLockProvider.acquireReadLock();
            urlParams.add(new BasicNameValuePair("session.id", this.sessionId));
        } finally {
            readWriteLockProvider.releaseReadLock();
        }
        urlParams.add(new BasicNameValuePair("projectName", name));
        urlParams.add(new BasicNameValuePair("projectId", projectId));
        urlParams.add(new BasicNameValuePair("flow", flowName));
        urlParams.add(new BasicNameValuePair("scheduleTime", scheduleTime));
        urlParams.add(new BasicNameValuePair("scheduleDate", scheduleDate));
        if (isrecuring){
            urlParams.add(new BasicNameValuePair("is_recurring", "on"));
            urlParams.add(new BasicNameValuePair("period", period));
        }
        httpResponse = postContent(httpPost, urlParams);
        String result = getResponse(httpResponse);
        if(!parseCreateResponseJson(result).get("status").getAsString().equalsIgnoreCase("success")){
            throw new AzkabanApiException(FLOW_SCHEDULE_ERROR_MESSAGE);
        }
    }

    @Override
    public void deleteProject(String name) throws AzkabanApiException {
        try {
            readWriteLockProvider.acquireReadLock();
            String url = URL_MANAGER+"?session.id="+ this.sessionId +"&delete=true&project="+name;
            httpGet = new HttpGet(url);
        } finally {
            readWriteLockProvider.releaseReadLock();
        }
        httpGet.setHeader(HTTP_HEADER_XML_HTTP_REQUEST_HEADER_NAME, HTTP_HEADER_XML_HTTP_REQUEST_HEADER_VALUE);
        httpGet.setHeader(HTTP_HEADER_CONTENT_TYPE_HEADER_NAME, HTTP_HEADER_CONTENT_TYPE_HEADER_VALUE);
        httpResponse = null;
        try {
            httpResponse = sslauthenticatedclient().execute(httpGet);
        } catch (IOException e) {
            throw new AzkabanApiException(SSL_CONNECTION_FAILURE_ERROR_MESSAGE, e);
        }
    }

    @Override
    public void cancelFlow(String execId) throws AzkabanApiException {
        try {
            readWriteLockProvider.acquireReadLock();
            String url = URL_EXECUTOR+"?ajax=cancelFlow&session.id="+this.sessionId +"&execid="+execId;
            httpGet = new HttpGet(url);
        } finally {
            readWriteLockProvider.releaseReadLock();
        }
        httpGet.setHeader(HTTP_HEADER_XML_HTTP_REQUEST_HEADER_NAME, HTTP_HEADER_XML_HTTP_REQUEST_HEADER_VALUE);
        httpGet.setHeader(HTTP_HEADER_CONTENT_TYPE_HEADER_NAME, HTTP_HEADER_CONTENT_TYPE_HEADER_VALUE);
        httpResponse = null;
        try {
            httpResponse = sslauthenticatedclient().execute(httpGet);
        } catch (IOException e) {
            throw new AzkabanApiException(SSL_CONNECTION_FAILURE_ERROR_MESSAGE, e);
        }
        if (httpResponse.getStatusLine().getStatusCode() != 200){
            throw new AzkabanApiException(FLOW_CANCELLATION_ERROR_MESSAGE);
        }
    }

    @Override
    public String fetchFlows(String name) throws AzkabanApiException {
        try {
            readWriteLockProvider.acquireReadLock();
            String url = URL_MANAGER+"?session.id="+ sessionId +"&ajax=fetchprojectflows&project="+name;
            httpGet = new HttpGet(url);
        } finally {
            readWriteLockProvider.releaseReadLock();
        }
        httpGet.setHeader(HTTP_HEADER_XML_HTTP_REQUEST_HEADER_NAME, HTTP_HEADER_XML_HTTP_REQUEST_HEADER_VALUE);
        httpGet.setHeader(HTTP_HEADER_CONTENT_TYPE_HEADER_NAME, HTTP_HEADER_CONTENT_TYPE_HEADER_VALUE);
        httpResponse = null;
        try {
            httpResponse = sslauthenticatedclient().execute(httpGet);
        } catch (IOException e) {
            throw new AzkabanApiException(SSL_CONNECTION_FAILURE_ERROR_MESSAGE, e);
        }
        if (httpResponse.getStatusLine().getStatusCode() == 200){
            return getResponse(httpResponse);
        } else {
            throw new AzkabanApiException(FLOW_FETCHING_ERROR_MESSAGE);
        }
    }

    @Override
    public String fetchExecutions(String name, String flowId, int startIndex, int endIndex) throws AzkabanApiException {
        try {
            readWriteLockProvider.acquireReadLock();
            String url = URL_MANAGER+"?session.id="+ sessionId +"&ajax=fetchFlowExecutions&project="+name+"&flow="+flowId+"&start="+startIndex+"&length="+endIndex;
            httpGet = new HttpGet(url);
        } finally {
            readWriteLockProvider.releaseReadLock();
        }
        httpGet.setHeader(HTTP_HEADER_XML_HTTP_REQUEST_HEADER_NAME, HTTP_HEADER_XML_HTTP_REQUEST_HEADER_VALUE);
        httpGet.setHeader(HTTP_HEADER_CONTENT_TYPE_HEADER_NAME, HTTP_HEADER_CONTENT_TYPE_HEADER_VALUE);
        httpResponse = null;
        try {
            httpResponse = sslauthenticatedclient().execute(httpGet);
        } catch (IOException e) {
            throw new AzkabanApiException(SSL_CONNECTION_FAILURE_ERROR_MESSAGE, e);
        }
        if (httpResponse.getStatusLine().getStatusCode() == 200){
            return getResponse(httpResponse);
        } else {
            throw new AzkabanApiException(EXECUTION_FETCHING_ERROR_MESSAGE);
        }
    }

    @Override
    public void unscheduleFlow(String scheduleId) throws AzkabanApiException {
        httpPost = new HttpPost(URL_SCHEDULE);
        httpPost.setHeader(HTTP_HEADER_XML_HTTP_REQUEST_HEADER_NAME, HTTP_HEADER_XML_HTTP_REQUEST_HEADER_VALUE);
        httpPost.setHeader(HTTP_HEADER_CONTENT_TYPE_HEADER_NAME, HTTP_HEADER_CONTENT_TYPE_HEADER_VALUE);
        urlParams = new ArrayList<>();
        urlParams.add(new BasicNameValuePair("action", "removeSched"));
        try {
            readWriteLockProvider.acquireReadLock();
            urlParams.add(new BasicNameValuePair("session.id", this.sessionId));
        } finally {
            readWriteLockProvider.releaseReadLock();
        }        urlParams.add(new BasicNameValuePair("scheduleId", scheduleId));
        httpResponse = postContent(httpPost, urlParams);
        String result = getResponse(httpResponse);
        System.out.println(result);
        if(!parseCreateResponseJson(result).get("status").getAsString().equalsIgnoreCase("success")){
            throw new AzkabanApiException(UNSCHEDULING_ERROR_MESSAGE);
        }
    }

    @Override
    public void executeFlow(String name, String flowId) throws AzkabanApiException {
        try {
            readWriteLockProvider.acquireReadLock();
            String url = URL_EXECUTOR+"?session.id="+this.sessionId +"&ajax=executeFlow&project="+name+"&flow="+flowId;
            httpGet = new HttpGet(url);
        } finally {
            readWriteLockProvider.releaseReadLock();
        }
        httpGet.setHeader(HTTP_HEADER_XML_HTTP_REQUEST_HEADER_NAME, HTTP_HEADER_XML_HTTP_REQUEST_HEADER_VALUE);
        httpGet.setHeader(HTTP_HEADER_CONTENT_TYPE_HEADER_NAME, HTTP_HEADER_CONTENT_TYPE_HEADER_VALUE);
        httpResponse = null;
        try {
            httpResponse = sslauthenticatedclient().execute(httpGet);
        } catch (IOException e) {
            throw new AzkabanApiException(SSL_CONNECTION_FAILURE_ERROR_MESSAGE, e);
        }
        if (httpResponse.getStatusLine().getStatusCode() != 200){
            throw new AzkabanApiException(EXECUTE_FLOW_ERROR_MESSAGE);
        }
    }

    private JsonObject checkError(String result){
        JsonParser parser = new JsonParser();

        return parser.parse(result).getAsJsonObject();
    }

    private JsonObject parseCreateResponseJson(String result){
        JsonParser parser = new JsonParser();
        return parser.parse(result).getAsJsonObject();
    }

    private HttpClient sslauthenticatedclient() throws AzkabanApiException {
        SSLContext sslcontext = null;
        try {
            sslcontext = SSLContexts.custom()
                    .loadTrustMaterial(
                            new File("/home/sampiy/ZoneGIT/Azkaban/RTIS_Utils/DAS_BYOA_Azkaban_Project_Creator/DAS-Scheduler/src/main/java/SSL/cacerts"),
                            "changeit".toCharArray(),
                            new TrustSelfSignedStrategy())
                    .build();
        } catch (NoSuchAlgorithmException e) {
            throw new AzkabanApiException(SSL_ERROR_MESSAGE, e);
        } catch (KeyManagementException e) {
            throw new AzkabanApiException(SSL_ERROR_MESSAGE, e);
        } catch (KeyStoreException e) {
            throw new AzkabanApiException(SSL_ERROR_MESSAGE, e);
        } catch (CertificateException e) {
            throw new AzkabanApiException(SSL_ERROR_MESSAGE, e);
        } catch (IOException e) {
            throw new AzkabanApiException(SSL_ERROR_MESSAGE, e);
        }
        SSLConnectionSocketFactory sslConnectionSocketFactory = new SSLConnectionSocketFactory(
                sslcontext,
                new String[]{"TLSv1"},
                null,
                SSLConnectionSocketFactory.getDefaultHostnameVerifier());
        return HttpClients.custom()
                .setSSLSocketFactory(sslConnectionSocketFactory)
                .build();

    }

    private HttpResponse postContent(HttpPost httpPost,List<NameValuePair> urlParams) throws AzkabanApiException {
        try {
            httpPost.setEntity(new UrlEncodedFormEntity(urlParams));
            return  sslauthenticatedclient().execute(httpPost);
        } catch (IOException e) {
            throw new AzkabanApiException(SSL_CONNECTION_FAILURE_ERROR_MESSAGE, e);
        }
    }

    private String getResponse(HttpResponse httpResponse) throws AzkabanApiException {
        StringBuffer result = new StringBuffer();
        String line;
        try {
            while ((line = new BufferedReader(new InputStreamReader(httpResponse.getEntity().getContent())).readLine()) != null) {
                result.append(line);
            }
        } catch (IOException e) {
            throw new AzkabanApiException(READ_ERROR_MESSAGE, e);
        }
        return result.toString();
    }
}
