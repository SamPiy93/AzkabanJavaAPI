package services;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import errors.AzkabanApiException;
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
    private static final String PROJECT_UPLOAD_ERROR_MESSAGE = "Error occurred while uploading the project to Azkaban";
    private static final String PROJECT_DELETION_ERROR_MESSAGE = "Error occurred while deleting the project from Azkaban";
    private static final String PROJECT_FLOW_CANCELLATION_ERROR_MESSAGE = "Error occurred while cancelling the flow of the project";
    private static final String FLOW_FETCHING_ERROR_MESSAGE = "Error occurred while fetching the flows of the project";
    private static final String EXECUTION_FETCHING_ERROR_MESSAGE = "Error occurred while fetching executions of the flow";
    private static final String AZKABAN_RECORD_FETCH_ERROR_MESSAGE = "Error occurred while fetching records of the project";
    private static final String FLOW_EXECUTION_ERROR_MESSAGE = "Error occurred while executing flows of the project";
    private static final String SSL_ERROR_MESSAGE = "SSL authentication error";
    private static final String PROJECT_ID_FETCH_ERROR_MESSAGE = "Error occurred while fetching the project id";
    private String sessionId;
    private String projectId = null;
    private List<String> projects = new ArrayList<>();
    @Override
    public boolean authenticate(String username, String password) throws AzkabanApiException {
        HttpPost httpPost = new HttpPost(URL);
        httpPost.setHeader(HTTP_HEADER_XML_HTTP_REQUEST_HEADER_NAME, HTTP_HEADER_XML_HTTP_REQUEST_HEADER_VALUE);
        httpPost.setHeader(HTTP_HEADER_CONTENT_TYPE_HEADER_NAME, HTTP_HEADER_CONTENT_TYPE_HEADER_VALUE);
        List<NameValuePair> urlParams = new ArrayList<>();
        urlParams.add(new BasicNameValuePair("action", "login"));
        urlParams.add(new BasicNameValuePair("username", username));
        urlParams.add(new BasicNameValuePair("password", password));
        HttpResponse httpResponse = postContent(httpPost, urlParams);
        String result = getResponse(httpResponse);
        if(!checkError(result).has("error"))
            parseAuthenticationResponseJson(result);
        return !checkError(result).has("error");
    }

    @Override
    public boolean createProject(String name, String description) throws AzkabanApiException {
        HttpPost post = new HttpPost(URL_MANAGER);
        post.setHeader(HTTP_HEADER_XML_HTTP_REQUEST_HEADER_NAME, HTTP_HEADER_XML_HTTP_REQUEST_HEADER_VALUE);
        post.setHeader(HTTP_HEADER_CONTENT_TYPE_HEADER_NAME, HTTP_HEADER_CONTENT_TYPE_HEADER_VALUE);
        List<NameValuePair> urlParameters = new ArrayList<>();
        urlParameters.add(new BasicNameValuePair("action", "create"));
        urlParameters.add(new BasicNameValuePair("session.id", sessionId));
        urlParameters.add(new BasicNameValuePair("name", name));
        urlParameters.add(new BasicNameValuePair("description", description));
        HttpResponse response = postContent(post, urlParameters);
        String result = getResponse(response);
        if(parseCreateResponseJson(result).get("status").getAsString().equalsIgnoreCase("success")){
            projects.add(name);
        }
        return response.getStatusLine().getStatusCode() == 200 && parseCreateResponseJson(result).get("status").getAsString().equalsIgnoreCase("success");
    }

    @Override
    public boolean uploadProject(String name, String projectPath, String zipFileName) throws AzkabanApiException {
        HttpPost post = new HttpPost(URL_MANAGER);
        String filePath = projectPath.concat(zipFileName.concat(".zip"));
        File file = new File(filePath);
        HttpEntity httpEntity = MultipartEntityBuilder.create()
                .addTextBody("session.id", sessionId)
                .addTextBody("ajax","upload")
                .addBinaryBody("file", file, ContentType.create("application/zip"), file.getName())
                .addTextBody("project",name)
                .build();
        post.setEntity(httpEntity);
        HttpResponse response = null;
        try {
            response = sslauthenticatedclient().execute(post);
        } catch (IOException e) {
            throw new AzkabanApiException(PROJECT_UPLOAD_ERROR_MESSAGE, e);
        }
        String result = getResponse(response);
        if(response.getStatusLine().getStatusCode() == 200 && parseCreateResponseJson(result).get("error") != null){
            System.out.println(parseCreateResponseJson(result).get("error").getAsString());
            return false;
        }
        projectId = parseCreateResponseJson(result).get("projectId").getAsString().trim();
        return true;
    }

    @Override
    public boolean scheduleFlow(String name, String projectId, String flowName, String scheduleTime, String scheduleDate, boolean isrecuring, String period) throws AzkabanApiException {
        HttpPost httpPost = new HttpPost(URL_SCHEDULE);
        httpPost.setHeader(HTTP_HEADER_XML_HTTP_REQUEST_HEADER_NAME, HTTP_HEADER_XML_HTTP_REQUEST_HEADER_VALUE);
        httpPost.setHeader(HTTP_HEADER_CONTENT_TYPE_HEADER_NAME, HTTP_HEADER_CONTENT_TYPE_HEADER_VALUE);
        List<NameValuePair> urlParams = new ArrayList<>();
        urlParams.add(new BasicNameValuePair("ajax", "scheduleFlow"));
        urlParams.add(new BasicNameValuePair("session.id", sessionId));
        urlParams.add(new BasicNameValuePair("projectName", name));
        urlParams.add(new BasicNameValuePair("projectId", projectId));
        urlParams.add(new BasicNameValuePair("flow", flowName));
        urlParams.add(new BasicNameValuePair("scheduleTime", scheduleTime));
        urlParams.add(new BasicNameValuePair("scheduleDate", scheduleDate));
        if (isrecuring){
            urlParams.add(new BasicNameValuePair("is_recurring", "on"));
            urlParams.add(new BasicNameValuePair("period", period));
        }
        HttpResponse httpResponse = postContent(httpPost, urlParams);
        String result = getResponse(httpResponse);
        if(parseCreateResponseJson(result).get("status").getAsString().equalsIgnoreCase("success")){
            projects.add(name);
        }
        return httpResponse.getStatusLine().getStatusCode() == 200 && parseCreateResponseJson(result).get("status").getAsString().equalsIgnoreCase("success");
    }

    @Override
    public boolean deleteProject(String name) throws AzkabanApiException {
        String url = URL_MANAGER+"?session.id="+ sessionId +"&delete=true&project="+name;
        HttpGet get = new HttpGet(url);
        get.setHeader(HTTP_HEADER_XML_HTTP_REQUEST_HEADER_NAME, HTTP_HEADER_XML_HTTP_REQUEST_HEADER_VALUE);
        get.setHeader(HTTP_HEADER_CONTENT_TYPE_HEADER_NAME, HTTP_HEADER_CONTENT_TYPE_HEADER_VALUE);
        HttpResponse httpResponse = null;
        try {
            httpResponse = sslauthenticatedclient().execute(get);
        } catch (IOException e) {
            throw new AzkabanApiException(PROJECT_DELETION_ERROR_MESSAGE, e);
        }
        return httpResponse.getStatusLine().getStatusCode() == 200;
    }

    @Override
    public boolean cancelFlow(String execId) throws AzkabanApiException {
        String url = URL_EXECUTOR+"?ajax=cancelFlow&session.id="+this.sessionId +"&execid="+execId;
        HttpGet get = new HttpGet(url);
        get.setHeader(HTTP_HEADER_XML_HTTP_REQUEST_HEADER_NAME, HTTP_HEADER_XML_HTTP_REQUEST_HEADER_VALUE);
        get.setHeader(HTTP_HEADER_CONTENT_TYPE_HEADER_NAME, HTTP_HEADER_CONTENT_TYPE_HEADER_VALUE);
        HttpResponse response = null;
        try {
            response = sslauthenticatedclient().execute(get);
        } catch (IOException e) {
            throw new AzkabanApiException(PROJECT_FLOW_CANCELLATION_ERROR_MESSAGE, e);
        }
        return response.getStatusLine().getStatusCode() == 200;
    }

    @Override
    public String fetchFlows(String name) throws AzkabanApiException {
        String url = URL_MANAGER+"?session.id="+ sessionId +"&ajax=fetchprojectflows&project="+name;
        HttpGet get = new HttpGet(url);
        get.setHeader(HTTP_HEADER_XML_HTTP_REQUEST_HEADER_NAME, HTTP_HEADER_XML_HTTP_REQUEST_HEADER_VALUE);
        get.setHeader(HTTP_HEADER_CONTENT_TYPE_HEADER_NAME, HTTP_HEADER_CONTENT_TYPE_HEADER_VALUE);
        HttpResponse response = null;
        try {
            response = sslauthenticatedclient().execute(get);
        } catch (IOException e) {
            throw new AzkabanApiException(FLOW_FETCHING_ERROR_MESSAGE, e);
        }
        return getResponse(response);
    }

    @Override
    public String fetchExecutions(String name, String flowId, int startIndex, int endIndex) throws AzkabanApiException {
        String url = URL_MANAGER+"?session.id="+ sessionId +"&ajax=fetchFlowExecutions&project="+name+"&flow="+flowId+"&start="+startIndex+"&length="+endIndex;
        HttpGet get = new HttpGet(url);
        get.setHeader(HTTP_HEADER_XML_HTTP_REQUEST_HEADER_NAME, HTTP_HEADER_XML_HTTP_REQUEST_HEADER_VALUE);
        get.setHeader(HTTP_HEADER_CONTENT_TYPE_HEADER_NAME, HTTP_HEADER_CONTENT_TYPE_HEADER_VALUE);
        HttpResponse response = null;
        try {
            response = sslauthenticatedclient().execute(get);
        } catch (IOException e) {
            throw new AzkabanApiException(EXECUTION_FETCHING_ERROR_MESSAGE, e);
        }
        if (response.getStatusLine().getStatusCode() == 200){
            return getResponse(response);
        } else {
            throw new AzkabanApiException(FLOW_EXECUTION_ERROR_MESSAGE);
        }
    }

    @Override
    public boolean fetch(String name, String flowId, String operation, String ajaxParam) throws AzkabanApiException {
        String url = URL + operation + "?session.id=" + this.sessionId + "&ajax=" + ajaxParam + "&project=" + name + "&flow=" + flowId;
        HttpGet get = new HttpGet(url);
        get.setHeader(HTTP_HEADER_XML_HTTP_REQUEST_HEADER_NAME, HTTP_HEADER_XML_HTTP_REQUEST_HEADER_VALUE);
        get.setHeader(HTTP_HEADER_CONTENT_TYPE_HEADER_NAME, HTTP_HEADER_CONTENT_TYPE_HEADER_VALUE);
        HttpResponse response = null;
        try {
            response = sslauthenticatedclient().execute(get);
        } catch (IOException e) {
            throw new AzkabanApiException(AZKABAN_RECORD_FETCH_ERROR_MESSAGE, e);
        }
        if (ajaxParam.equalsIgnoreCase("fetchflowgraph")) {
            System.out.println(getResponse(response));
        }
        return response.getStatusLine().getStatusCode() == 200;
    }
    @Override
    public boolean unscheduleFlow(String scheduleId) throws AzkabanApiException {
        HttpPost post = new HttpPost(URL_SCHEDULE);
        post.setHeader(HTTP_HEADER_XML_HTTP_REQUEST_HEADER_NAME, HTTP_HEADER_XML_HTTP_REQUEST_HEADER_VALUE);
        post.setHeader(HTTP_HEADER_CONTENT_TYPE_HEADER_NAME, HTTP_HEADER_CONTENT_TYPE_HEADER_VALUE);
        List<NameValuePair> urlParameters = new ArrayList<>();
        urlParameters.add(new BasicNameValuePair("action", "removeSched"));
        urlParameters.add(new BasicNameValuePair("session.id", sessionId));
        urlParameters.add(new BasicNameValuePair("scheduleId", scheduleId));
        HttpResponse response = postContent(post, urlParameters);
        String result = getResponse(response);
        System.out.println(result);
        if(parseCreateResponseJson(result).get("status").getAsString().equalsIgnoreCase("success")){
            System.out.println("success");
        }
        return response.getStatusLine().getStatusCode() == 200 && parseCreateResponseJson(result).get("status").getAsString().equalsIgnoreCase("success");
    }

    @Override
    public boolean executeFlow(String name, String flowId) throws AzkabanApiException {
        String url = URL_EXECUTOR+"?session.id="+this.sessionId +"&ajax=executeFlow&project="+name+"&flow="+flowId;
        HttpGet get = new HttpGet(url);
        get.setHeader(HTTP_HEADER_XML_HTTP_REQUEST_HEADER_NAME, HTTP_HEADER_XML_HTTP_REQUEST_HEADER_VALUE);
        get.setHeader(HTTP_HEADER_CONTENT_TYPE_HEADER_NAME, HTTP_HEADER_CONTENT_TYPE_HEADER_VALUE);
        HttpResponse response = null;
        try {
            response = sslauthenticatedclient().execute(get);
        } catch (IOException e) {
            throw new AzkabanApiException(FLOW_EXECUTION_ERROR_MESSAGE, e);
        }
        return response.getStatusLine().getStatusCode() == 200;
    }

    @Override
    public boolean isAuthenticated() {
        return (this.sessionId != null);
    }

    private JsonObject checkError(String result){
        JsonParser parser = new JsonParser();

        return parser.parse(result).getAsJsonObject();
    }

    private JsonObject parseCreateResponseJson(String result){
        JsonParser parser = new JsonParser();
        return parser.parse(result).getAsJsonObject();
    }

    public JsonObject parseAuthenticationResponseJson(String result){
        JsonParser parser = new JsonParser();
        JsonObject object = parser.parse(result).getAsJsonObject();
        this.sessionId = object.get("session.id").getAsString();
        return object;
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

    private HttpResponse postContent(HttpPost post,List<NameValuePair> urlParameters) throws AzkabanApiException {
        try {
            post.setEntity(new UrlEncodedFormEntity(urlParameters));
            return  sslauthenticatedclient().execute(post);
        } catch (UnsupportedEncodingException e) {
            throw new AzkabanApiException(SSL_ERROR_MESSAGE, e);
        } catch (ClientProtocolException e) {
            throw new AzkabanApiException(SSL_ERROR_MESSAGE, e);
        } catch (IOException e) {
            throw new AzkabanApiException(SSL_ERROR_MESSAGE, e);
        }
    }

    private String getResponse(HttpResponse response) throws AzkabanApiException {
        BufferedReader rd = null;
        try {
            rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
        } catch (IOException e) {
            throw new AzkabanApiException(SSL_ERROR_MESSAGE, e);
        }

        StringBuffer result = new StringBuffer();
        String line = "";
        try {
            while ((line = rd.readLine()) != null) {
                result.append(line);
            }
        } catch (IOException e) {
            throw new AzkabanApiException(SSL_ERROR_MESSAGE, e);
        }

        return result.toString();
    }
}
