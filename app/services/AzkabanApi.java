package services;

import errors.AzkabanApiException;

/**
 * Azkaban Api interface
 */
public interface AzkabanApi {

    boolean authenticate(String username, String password) throws AzkabanApiException;

    boolean createProject(String name, String description) throws AzkabanApiException;

    boolean uploadProject(String name, String projectPath, String zipFileName) throws AzkabanApiException;

    boolean scheduleFlow(String name, String projectId, String flowName, String scheduleTime, String scheduleDate, boolean isrecuring, String period) throws AzkabanApiException;

    boolean deleteProject(String name) throws AzkabanApiException;

    boolean cancelFlow(String execId) throws AzkabanApiException;

    String fetchFlows(String name) throws AzkabanApiException;

    String fetchExecutions(String name, String flowId, int startIndex, int endIndex) throws AzkabanApiException;

    boolean fetch(String name, String flowId, String operation, String ajaxParam) throws AzkabanApiException;

    boolean unscheduleFlow(String scheduleId) throws AzkabanApiException;

    boolean executeFlow(String name, String flowId) throws AzkabanApiException;

    boolean isAuthenticated();
}
