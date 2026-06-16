package com.csg.airtel.aaa4j.application.resources;

import com.csg.airtel.aaa4j.domain.model.AccountingRequestDto;
import com.csg.airtel.aaa4j.domain.model.session.UserSessionData;
import com.csg.airtel.aaa4j.domain.service.AccountingHandlerFactory;
import com.csg.airtel.aaa4j.domain.service.NotificationTrackingService;
import com.csg.airtel.aaa4j.external.clients.CacheClient;
import com.csg.airtel.aaa4j.external.repository.UserBucketRepository;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;


import java.util.HashMap;


import java.util.Map;



@Path("/debug")
@ApplicationScoped
public class RedisResource {
    private static final Logger log = Logger.getLogger(RedisResource.class);
    final UserBucketRepository userRepository;

    final CacheClient cacheClient;

    final AccountingHandlerFactory accountingHandlerFactory;

    final NotificationTrackingService notificationTrackingService;

    /**
     * These /debug endpoints expose and mutate cache/accounting state with no authentication.
     * They are disabled by default; set debug.endpoints.enabled=true (non-production only) to use them.
     */
    @ConfigProperty(name = "debug.endpoints.enabled", defaultValue = "false")
    boolean debugEnabled;

    public RedisResource(UserBucketRepository userRepository, CacheClient cacheClient, AccountingHandlerFactory accountingHandlerFactory, NotificationTrackingService notificationTrackingService) {
        this.userRepository = userRepository;
        this.cacheClient = cacheClient;
        this.accountingHandlerFactory = accountingHandlerFactory;
        this.notificationTrackingService = notificationTrackingService;
    }

    /** Reject access to debug endpoints unless explicitly enabled via config. Returns 404 to avoid disclosing existence. */
    private void assertDebugEnabled() {
        if (!debugEnabled) {
            throw new NotFoundException();
        }
    }

    private static String requireNonBlank(String value, String paramName) {
        if (value == null || value.isBlank()) {
            throw new BadRequestException(paramName + " is required");
        }
        return value;
    }

    @GET
    @Path("/redis-ping")
    @Produces(MediaType.TEXT_PLAIN)
    public Uni<Map<String, Object>> testConnection() {
        assertDebugEnabled();
        return userRepository.getServiceBucketsByUserName("100001")
                .onItem().transform(buckets -> {
                    Map<String, Object> results = new HashMap<>();
                    results.put("buckets", buckets);
                    return results;
                });
    }


    @POST
    @Path("/redis-ping")
    @Produces(MediaType.TEXT_PLAIN)
    @Consumes(MediaType.APPLICATION_JSON)
    public Uni<Map<String, Object>> interimUpdate(AccountingRequestDto request) {
        assertDebugEnabled();
        return accountingHandlerFactory
                .getHandler(request,null)
                .onItem().transform(result -> {
                    Map<String, Object> res = new HashMap<>();
                    res.put("accounting_result", result);
                    return res;
                }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }


    @DELETE
    @Path("/redis/delete")
    @Produces(MediaType.APPLICATION_JSON)
    public Uni<Map<String, Object>> deleteKeyCache(@QueryParam("username") String key) {
        assertDebugEnabled();
        requireNonBlank(key, "username");
        return cacheClient
                .deleteKey(key)
                .onItem().transform(result -> {
                    Map<String, Object> res = new HashMap<>();
                    res.put("accounting_result", result);
                    return res;
                });
    }

    @GET
    @Path("/redis/get")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Uni <UserSessionData> getKeyCache(@QueryParam("username") String key) {
        assertDebugEnabled();
        requireNonBlank(key, "username");
        log.infof("cache check request initiated key %s",key);
        return cacheClient
                .getUserData(key)
                .onItem().transform(result ->
                     result
                );
    }

    @GET
    @Path("/redis/notification-clear")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Uni<Map<String, Object>> deleteNotification(@QueryParam("username") String username,@QueryParam("templateId") long templateId,
                                                   @QueryParam("bucketId") String bucketId, @QueryParam("thresholdLevel") long thresholdLevel) {
        assertDebugEnabled();
        requireNonBlank(username, "username");
        return notificationTrackingService
                .clearNotificationTracking(username,templateId,bucketId,thresholdLevel)
                .onItem().transform(result -> {
                    Map<String, Object> res = new HashMap<>();
                    res.put("Notification-clear", result);
                    return res;
                });
    }
}