package controllers;

import com.google.inject.Inject;
import dto.AuthenticateDto;
import errors.AzkabanApiException;
import errors.PojoToJsonMappingException;
import mapper.JsonMapper;
import play.libs.Json;
import play.mvc.BodyParser;
import play.mvc.Result;
import services.AzkabanApi;

import static play.mvc.Controller.request;
import static play.mvc.Results.ok;

/**
 * Controller for azkaban scheduler
 */
public class AzkabanSchedulerController {

}
