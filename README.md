# AzkabanJavaAPI
Java API for Azkaban job scheduler.
Implementation for the following methods are included in this project. The default AJAX API methods has been replaced in this.

authenticate(String username, String password)

void createProject(String name, String description)

void uploadProject(String name, String projectPath, String zipFileName)

void scheduleFlow(String name, String projectId, String flowName, String scheduleTime, String scheduleDate, boolean isrecuring, String period)

void deleteProject(String name)

void cancelFlow(String execId)

String fetchFlows(String name)

String fetchExecutions(String name, String flowId, int startIndex, int endIndex)

void unscheduleFlow(String scheduleId)

void executeFlow(String name, String flowId)
