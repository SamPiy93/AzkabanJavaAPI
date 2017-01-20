package services;

import errors.AzkabanApiException;

/**
 * Azkaban Api interface
 */
public interface AzkabanApi {

    void authenticate(String username, String password) throws AzkabanApiException;

    void createProject(String name, String description) throws AzkabanApiException;

    void uploadProject(String name, String projectPath, String zipFileName) throws AzkabanApiException;

    void scheduleFlow(String name, String projectId, String flowName, String scheduleTime, String scheduleDate, boolean isrecuring, String period) throws AzkabanApiException;

    void deleteProject(String name) throws AzkabanApiException;

    void cancelFlow(String execId) throws AzkabanApiException;

    String fetchFlows(String name) throws AzkabanApiException;

    String fetchExecutions(String name, String flowId, int startIndex, int endIndex) throws AzkabanApiException;

    void unscheduleFlow(String scheduleId) throws AzkabanApiException;

    void executeFlow(String name, String flowId) throws AzkabanApiException;
}
