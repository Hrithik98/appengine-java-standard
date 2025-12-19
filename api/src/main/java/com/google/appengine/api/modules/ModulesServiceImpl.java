/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.appengine.api.modules;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.appengine.v1.Appengine;
import com.google.api.services.appengine.v1.model.Application;
import com.google.api.services.appengine.v1.model.ManualScaling;
import com.google.api.services.appengine.v1.model.Service;
import com.google.api.services.appengine.v1.model.TrafficSplit;
import com.google.api.services.appengine.v1.model.Version;
import com.google.appengine.api.modules.ModulesServicePb.GetDefaultVersionRequest;
import com.google.appengine.api.modules.ModulesServicePb.GetDefaultVersionResponse;
import com.google.appengine.api.modules.ModulesServicePb.GetHostnameRequest;
import com.google.appengine.api.modules.ModulesServicePb.GetHostnameResponse;
import com.google.appengine.api.modules.ModulesServicePb.GetModulesRequest;
import com.google.appengine.api.modules.ModulesServicePb.GetModulesResponse;
import com.google.appengine.api.modules.ModulesServicePb.GetNumInstancesRequest;
import com.google.appengine.api.modules.ModulesServicePb.GetNumInstancesResponse;
import com.google.appengine.api.modules.ModulesServicePb.GetVersionsRequest;
import com.google.appengine.api.modules.ModulesServicePb.GetVersionsResponse;
import com.google.appengine.api.modules.ModulesServicePb.ModulesServiceError.ErrorCode;
import com.google.appengine.api.modules.ModulesServicePb.SetNumInstancesRequest;
import com.google.appengine.api.modules.ModulesServicePb.SetNumInstancesResponse;
import com.google.appengine.api.modules.ModulesServicePb.StartModuleRequest;
import com.google.appengine.api.modules.ModulesServicePb.StartModuleResponse;
import com.google.appengine.api.modules.ModulesServicePb.StopModuleRequest;
import com.google.appengine.api.modules.ModulesServicePb.StopModuleResponse;
import com.google.appengine.api.utils.FutureWrapper;
import com.google.apphosting.api.ApiProxy;
import com.google.apphosting.api.ApiProxy.Environment;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ForwardingFuture;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Implementation of {@link ModulesService}.
 */
class ModulesServiceImpl implements ModulesService {
  private static final Logger logger = Logger.getLogger(ModulesServiceImpl.class.getName());
  private static final String STARTING_STARTED_MESSAGE =
      "Attempted to start an already started module version, continuing";
  private static final String STOPPING_STOPPED_MESSAGE =
      "Attempted to stop an already stopped module version, continuing";

  @VisibleForTesting
  static final String PACKAGE = "modules";
  private static final String GOOGLE_CLOUD_PROJECT_ENV = "GOOGLE_CLOUD_PROJECT";
  private static final String GAE_APPLICATION_ENV = "GAE_APPLICATION";

  private static final String INSTANCE_ID_ENV_ATTRIBUTE = "com.google.appengine.instance.id";
  private final String projectId;
  private final ExecutorService executor;

  // --- Constructors ---

  ModulesServiceImpl() {
    this(getProjectId());
  }

  @VisibleForTesting
  ModulesServiceImpl(String projectId) {
    this.projectId = projectId;
    this.executor = Executors.newCachedThreadPool();
  }

  private static String getProjectId() {
    String projectId = System.getenv(GOOGLE_CLOUD_PROJECT_ENV);
    if (projectId == null) {
      String gaeApplication = System.getenv(GAE_APPLICATION_ENV);
      if (gaeApplication != null && gaeApplication.startsWith("s~")) {
        projectId = gaeApplication.substring(2);
      }
    }
    if (projectId == null) {
      throw new IllegalStateException("Could not determine project ID.");
    }
    return projectId;
  }

  // --- Public Methods ---

  @Override
  public String getCurrentModule() {
    return getCurrentEnvironmentOrThrow().getModuleId();
  }

  @Override
  public String getCurrentVersion() {
    Environment env = getCurrentEnvironmentOrThrow();
    return Splitter.on('.').split(env.getVersionId()).iterator().next();
  }

  private static Map<String, Object> getThreadLocalAttributes() {
    return getCurrentEnvironmentOrThrow().getAttributes();
  }

  @Override
  public String getCurrentInstanceId() {
    Map<String, Object> env = getThreadLocalAttributes();
    if (!env.containsKey(INSTANCE_ID_ENV_ATTRIBUTE)) {
      throw new ModulesException("Instance id unavailable");
    }
    String instanceId = (String) getThreadLocalAttributes().get(INSTANCE_ID_ENV_ATTRIBUTE);
    if (instanceId == null) {
      throw new ModulesException("Instance id unavailable");
    }
    return instanceId;
  }

  @Override
  public Set<String> getModules() {
    return getAsyncResult(getModulesAsync());
  }

  @Override
  public Set<String> getVersions(String module) {
    return getAsyncResult(getVersionsAsync(module));
  }

  @Override
  public String getDefaultVersion(String module) {
    return getAsyncResult(getDefaultVersionAsync(module));
  }

  @Override
  public int getNumInstances(String module, String version) {
    return getAsyncResult(getNumInstancesAsync(module, version));
  }

  @Override
  public void setNumInstances(String module, String version, long instances) {
    getAsyncResult(setNumInstancesAsync(module, version, instances));
  }

  @Override
  public void startVersion(String module, String version) {
    getAsyncResult(startVersionAsync(module, version));
  }

  @Override
  public void stopVersion(String module, String version) {
    getAsyncResult(stopVersionAsync(module, version));
  }

  @Override
  public String getVersionHostname(String module, String version) {
    return getAsyncResult(getVersionHostnameAsync(module, version));
  }

  @Override
  public String getInstanceHostname(String module, String version, String instance) {
    return getAsyncResult(getInstanceHostnameAsync(module, version, instance));
  }

  // --- Asynchronous Implementations ---

  private Future<Set<String>> getModulesAsync() {
    if (!hasOptedIn()) {
      return getModulesAsyncLegacy();
    }
    return submit(() -> {
      Appengine client = getAdminAPIClientWithUseragent("get_modules");
      List<Service> services = client.apps().services().list(this.projectId).execute().getServices();
      if (services == null) {
        return Collections.emptySet();
      }
      return services.stream().map(Service::getId).collect(Collectors.toSet());
    });
  }

  private Future<Set<String>> getModulesAsyncLegacy() {
    GetModulesRequest.Builder requestBuilder = GetModulesRequest.newBuilder();
    Future<byte[]> rawFuture = makeApiCall(PACKAGE, "GetModules", requestBuilder.build().toByteArray());
    return new ModulesServiceFutureWrapper<>(rawFuture, "GetModules") {
      @Override
      protected Set<String> wrap(byte[] key) throws InvalidProtocolBufferException {
        GetModulesResponse.Builder responseBuilder = GetModulesResponse.newBuilder();
        responseBuilder.mergeFrom(key);
        return Sets.newHashSet(responseBuilder.getModuleList());
      }
    };
  }

  private Future<Set<String>> getVersionsAsync(String module) {
    if (!hasOptedIn()) {
      return getVersionsAsyncLegacy(module);
    }
    String targetModule = (module != null) ? module : getCurrentModule();
    return submit(() -> {
      Appengine client = getAdminAPIClientWithUseragent("get_versions");
      List<Version> versions =
          client.apps().services().versions().list(this.projectId, targetModule).execute().getVersions();
      if (versions == null) {
        return Collections.emptySet();
      }
      return versions.stream().map(Version::getId).collect(Collectors.toSet());
    });
  }

  private Future<Set<String>> getVersionsAsyncLegacy(String module) {
    GetVersionsRequest.Builder requestBuilder = GetVersionsRequest.newBuilder();
    if (module != null) {
      requestBuilder.setModule(module);
    }
    Future<byte[]> rawFuture = makeApiCall(PACKAGE, "GetVersions", requestBuilder.build().toByteArray());
    return new ModulesServiceFutureWrapper<>(rawFuture, "GetVersions") {
      @Override
      protected Set<String> wrap(byte[] key) throws InvalidProtocolBufferException {
        GetVersionsResponse.Builder responseBuilder = GetVersionsResponse.newBuilder();
        responseBuilder.mergeFrom(key);
        return Sets.newHashSet(responseBuilder.getVersionList());
      }
    };
  }

  private Future<String> getDefaultVersionAsync(String module) {
    if (!hasOptedIn()) {
      return getDefaultVersionAsyncLegacy(module);
    }
    String targetModule = (module != null) ? module : getCurrentModule();
    return submit(() -> {
      Appengine client = getAdminAPIClientWithUseragent("get_default_version");
      Service service = client.apps().services().get(this.projectId, targetModule).execute();
      TrafficSplit split = service.getSplit();
      Map<String, Double> allocations = (split != null) ? split.getAllocations() : null;

      String retVersion = findDefaultVersionFromAllocations(allocations);

      if (retVersion == null) {
        throw new IOException("Could not determine default version for module '" + targetModule + "'.");
      }
      return retVersion;
    });
  }

  @VisibleForTesting
  static String findDefaultVersionFromAllocations(Map<String, Double> allocations) {
    String retVersion = null;
    if (allocations != null && !allocations.isEmpty()) {
      double maxAlloc = -1.0;
      for (Map.Entry<String, Double> entry : allocations.entrySet()) {
        String version = entry.getKey();
        double allocation = entry.getValue();

        if (allocation == 1.0) {
          retVersion = version;
          break;
        }

        if (allocation > maxAlloc) {
          retVersion = version;
          maxAlloc = allocation;
        } else if (allocation == maxAlloc) {
          if (retVersion == null || version.compareTo(retVersion) < 0) {
            retVersion = version;
          }
        }
      }
    }
    return retVersion;
  }

  private Future<String> getDefaultVersionAsyncLegacy(String module) {
    GetDefaultVersionRequest.Builder requestBuilder = GetDefaultVersionRequest.newBuilder();
    if (module != null) {
      requestBuilder.setModule(module);
    }
    Future<byte[]> rawFuture = makeApiCall(PACKAGE, "GetDefaultVersion", requestBuilder.build().toByteArray());
    return new ModulesServiceFutureWrapper<>(rawFuture, "GetDefaultVersion") {
      @Override
      protected String wrap(byte[] key) throws InvalidProtocolBufferException {
        GetDefaultVersionResponse.Builder responseBuilder = GetDefaultVersionResponse.newBuilder();
        responseBuilder.mergeFrom(key);
        return responseBuilder.getVersion();
      }
    };
  }

  private Future<Integer> getNumInstancesAsync(String module, String version) {
    if (!hasOptedIn()) {
      return getNumInstancesAsyncLegacy(module, version);
    }
    String targetModule = (module != null) ? module : getCurrentModule();
    String targetVersion = (version != null) ? version : getCurrentVersion();
    return submit(() -> {
      Appengine client = getAdminAPIClientWithUseragent("get_num_instances");
      Version ver = client.apps().services().versions().get(this.projectId, targetModule, targetVersion).execute();
      if (ver.getManualScaling() != null && ver.getManualScaling().getInstances() != null) {
        return ver.getManualScaling().getInstances();
      }
      return 0;
    });
  }

  private Future<Integer> getNumInstancesAsyncLegacy(String module, String version) {
    GetNumInstancesRequest.Builder requestBuilder = GetNumInstancesRequest.newBuilder();
    if (module != null) {
      requestBuilder.setModule(module);
    }
    if (version != null) {
      requestBuilder.setVersion(version);
    }
    Future<byte[]> rawFuture = makeApiCall(PACKAGE, "GetNumInstances", requestBuilder.build().toByteArray());
    return new ModulesServiceFutureWrapper<>(rawFuture, "GetNumInstances") {
      @Override
      protected Integer wrap(byte[] key) throws InvalidProtocolBufferException {
        GetNumInstancesResponse.Builder responseBuilder = GetNumInstancesResponse.newBuilder();
        responseBuilder.mergeFrom(key);
        long instances = responseBuilder.getInstances();
        if (instances < 0 || instances > Integer.MAX_VALUE) {
          throw new IllegalStateException("Invalid instances value: " + instances);
        }
        return (int) instances;
      }
    };
  }

  @Override
  public Future<Void> setNumInstancesAsync(String module, String version, long instances) {
    if (!hasOptedIn()) {
      return setNumInstancesAsyncLegacy(module, version, instances);
    }
    String targetModule = (module != null) ? module : getCurrentModule();
    String targetVersion = (version != null) ? version : getCurrentVersion();
    Version body = new Version().setManualScaling(new ManualScaling().setInstances((int) instances));
    return patchVersion(targetModule, targetVersion, body, "manualScaling.instances");
  }

  public Future<Void> setNumInstancesAsyncLegacy(String module, String version, long instances) {
    SetNumInstancesRequest.Builder requestBuilder = SetNumInstancesRequest.newBuilder();
    if (module != null) {
      requestBuilder.setModule(module);
    }
    if (version != null) {
      requestBuilder.setVersion(version);
    }
    requestBuilder.setInstances(instances);
    Future<byte[]> rawFuture = makeApiCall(PACKAGE, "SetNumInstances", requestBuilder.build().toByteArray());
    return new ModulesServiceFutureWrapper<>(rawFuture, "SetNumInstances") {
      @Override
      protected Void wrap(byte[] key) throws InvalidProtocolBufferException {
        SetNumInstancesResponse.Builder responseBuilder = SetNumInstancesResponse.newBuilder();
        responseBuilder.mergeFrom(key);
        return null;
      }
    };
  }

  @Override
  public Future<Void> startVersionAsync(String module, String version) {
    if (!hasOptedIn()) {
      return startVersionAsyncLegacy(module, version);
    }
    String targetModule = (module != null) ? module : getCurrentModule();
    String targetVersion = (version != null) ? version : getCurrentVersion();
    Version body = new Version().setServingStatus("SERVING");
    return patchVersion(targetModule, targetVersion, body, "servingStatus");
  }

  public Future<Void> startVersionAsyncLegacy(String module, String version) {
    StartModuleRequest.Builder requestBuilder = StartModuleRequest.newBuilder();
    requestBuilder.setModule(module);
    requestBuilder.setVersion(version);
    Future<byte[]> rawFuture = makeApiCall(PACKAGE, "StartModule", requestBuilder.build().toByteArray());
    Future<Void> modulesServiceFuture =
        new ModulesServiceFutureWrapper<>(rawFuture, "StartModule") {
      @Override
      protected Void wrap(byte[] key) throws InvalidProtocolBufferException {
        StartModuleResponse.Builder responseBuilder = StartModuleResponse.newBuilder();
        responseBuilder.mergeFrom(key);
        return null;
      }
    };
    return new IgnoreUnexpectedStateExceptionFuture(modulesServiceFuture, STARTING_STARTED_MESSAGE);
  }

  @Override
  public Future<Void> stopVersionAsync(String module, String version) {
    if (!hasOptedIn()) {
      return stopVersionAsyncLegacy(module, version);
    }
    String targetModule = (module != null) ? module : getCurrentModule();
    String targetVersion = (version != null) ? version : getCurrentVersion();
    Version body = new Version().setServingStatus("STOPPED");
    return patchVersion(targetModule, targetVersion, body, "servingStatus");
  }

  public Future<Void> stopVersionAsyncLegacy(String module, String version) {
    StopModuleRequest.Builder requestBuilder = StopModuleRequest.newBuilder();
    if (module != null) {
      requestBuilder.setModule(module);
    }
    if (version != null) {
      requestBuilder.setVersion(version);
    }
    Future<byte[]> rawFuture = makeApiCall(PACKAGE, "StopModule", requestBuilder.build().toByteArray());
    Future<Void> modulesServiceFuture =
        new ModulesServiceFutureWrapper<>(rawFuture, "StopModule") {
      @Override
      protected Void wrap(byte[] key) throws InvalidProtocolBufferException {
        StopModuleResponse.Builder responseBuilder = StopModuleResponse.newBuilder();
        responseBuilder.mergeFrom(key);
        return null;
      }
    };
    return new IgnoreUnexpectedStateExceptionFuture(modulesServiceFuture, STOPPING_STOPPED_MESSAGE);
  }

  private Future<String> getVersionHostnameAsync(String module, String version) {
    if (!hasOptedIn()) {
      return getVersionHostnameAsyncLegacy(module, version);
    }
    String targetModule = (module != null) ? module : getCurrentModule();
    String targetVersion = (version != null) ? version : getCurrentVersion();
    return submit(() -> {
      Appengine client = getAdminAPIClientWithUseragent("get_version_hostname");
      Application appResponse = client.apps().get(this.projectId).execute();
      String defaultHostname = appResponse.getDefaultHostname();

      Set<String> services = getModules();

      if (services.size() == 1 && services.contains("default")) {
        if (!"default".equals(targetModule)) {
          throw new ModulesException("Module '" + targetModule + "' not found.");
        }
        return constructHostname(targetVersion, defaultHostname);
      }
      return constructHostname(targetVersion, targetModule, defaultHostname);
    });
  }

  private Future<String> getVersionHostnameAsyncLegacy(String module, String version) {
    GetHostnameRequest.Builder requestBuilder = newGetHostnameRequestBuilder(module, version);
    Future<byte[]> rawFuture = makeApiCall(PACKAGE, "GetHostname", requestBuilder.build().toByteArray());
    return new ModulesServiceFutureWrapper<>(rawFuture, "GetHostname") {
      @Override
      protected String wrap(byte[] key) throws InvalidProtocolBufferException {
        GetHostnameResponse.Builder responseBuilder = GetHostnameResponse.newBuilder();
        responseBuilder.mergeFrom(key);
        return responseBuilder.getHostname();
      }
    };
  }

  private Future<String> getInstanceHostnameAsync(String module, String version, String instance) {
    if (!hasOptedIn()) {
      return getInstanceHostnameAsyncLegacy(module, version, instance);
    }
    if (instance == null || instance.isEmpty()) {
      throw new IllegalArgumentException("Instance string cannot be null or empty.");
    }
    int instanceNum;
    try {
      instanceNum = Integer.parseInt(instance);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("The specified instance ID must be an integer.", e);
    }
    if (instanceNum < 0) {
      throw new IllegalArgumentException("The specified instance must be an integer greater than or equal to 0.");
    }

    String targetModule = (module != null) ? module : getCurrentModule();
    String targetVersion = (version != null) ? version : getCurrentVersion();
    return submit(() -> {
      Appengine client = getAdminAPIClientWithUseragent("get_instance_hostname");
      Application appResponse = client.apps().get(this.projectId).execute();
      String defaultHostname = appResponse.getDefaultHostname();

      Set<String> services = getModules();

      if (services.size() == 1 && services.contains("default")) {
        if (!"default".equals(targetModule)) {
           throw new ModulesException("Module '" + targetModule + "' not found.");
        }
        return constructHostname(instance, targetVersion, defaultHostname);
      }

      Version versionDetails = client.apps().services().versions()
          .get(this.projectId, targetModule, targetVersion)
          .setView("FULL")
          .execute();

      ManualScaling manualScaling = versionDetails.getManualScaling();
      if (manualScaling == null || manualScaling.getInstances() == null) {
        throw new ModulesException("Instance-specific hostnames are only available for manually scaled services.");
      }
      Integer manualScaling_numsInstances = manualScaling.getInstances();
      if (instanceNum >= manualScaling_numsInstances) {
        throw new ModulesException("The specified instance does not exist for this module/version.");
      }

      return constructHostname(instance, targetVersion, targetModule, defaultHostname);
    });
  }

  private Future<String> getInstanceHostnameAsyncLegacy(String module, String version, String instance) {
    GetHostnameRequest.Builder requestBuilder = newGetHostnameRequestBuilder(module, version);
    requestBuilder.setInstance(instance);
    Future<byte[]> rawFuture = makeApiCall(PACKAGE, "GetHostname", requestBuilder.build().toByteArray());
    return new ModulesServiceFutureWrapper<>(rawFuture, "GetHostname") {
      @Override
      protected String wrap(byte[] key) throws InvalidProtocolBufferException {
        GetHostnameResponse.Builder responseBuilder = GetHostnameResponse.newBuilder();
        responseBuilder.mergeFrom(key);
        return responseBuilder.getHostname();
      }
    };
  }

  // --- Helper Methods ---

  // CHANGED TO PROTECTED for safer overriding
  @VisibleForTesting
  protected Appengine getAdminAPIClientWithUseragent(String methodName) {
    try {
      GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
      if (credentials.createScopedRequired()) {
        credentials = credentials.createScoped(Arrays.asList("https://www.googleapis.com/auth/cloud-platform"));
      }
      HttpRequestInitializer requestInitializer = new HttpCredentialsAdapter(credentials);
      String userAgent = "appengine-modules-api-java-client/" + methodName;

      return new Appengine.Builder(
              GoogleNetHttpTransport.newTrustedTransport(),
              JacksonFactory.getDefaultInstance(),
              requestInitializer)
          .setApplicationName(userAgent)
          .build();
    } catch (IOException | GeneralSecurityException e) {
      throw new RuntimeException("Failed to build App Engine Admin API client", e);
    }
  }

  @VisibleForTesting
  protected Future<byte[]> makeApiCall(String packageName, String methodName, byte[] request) {
    return ApiProxy.makeAsyncCall(packageName, methodName, request);
  }

  private GetHostnameRequest.Builder newGetHostnameRequestBuilder(String module, String version) {
    GetHostnameRequest.Builder builder = GetHostnameRequest.newBuilder();
    if (module != null) {
      builder.setModule(module);
    }
    if (version != null) {
      builder.setVersion(version);
    }
    return builder;
  }

  private static String constructHostname(String... parts) {
    return Arrays.stream(parts)
        .filter(p -> p != null && !p.isEmpty())
        .collect(Collectors.joining("."));
  }

  private static ApiProxy.Environment getCurrentEnvironmentOrThrow() {
    ApiProxy.Environment environment = ApiProxy.getCurrentEnvironment();
    if (environment == null) {
      throw new IllegalStateException(
          "Operation not allowed in a thread that is neither the original request thread "
              + "nor a thread created by ThreadManager");
    }
    return environment;
  }

  // CHANGED TO PROTECTED for safer overriding
  @VisibleForTesting
  protected boolean hasOptedIn() {
    String optInValue = System.getenv("MODULES_USE_ADMIN_API");
    return "true".equalsIgnoreCase(optInValue);
  }

  private <V> Future<V> submit(Callable<V> task) {
    return executor.submit(task);
  }

  private <V> V getAsyncResult(Future<V> asyncResult) {
    try {
      return asyncResult.get();
    } catch (InterruptedException ie) {
      throw new ModulesException("Unexpected failure", ie);
    } catch (ExecutionException ee) {
      if (ee.getCause() instanceof RuntimeException) {
        throw (RuntimeException) ee.getCause();
      } else if (ee.getCause() instanceof Error) {
        throw (Error) ee.getCause();
      } else {
        throw new UndeclaredThrowableException(ee.getCause());
      }
    }
  }

  @VisibleForTesting
  protected Future<Void> patchVersion(String module, String version, Version body, String updateMask) {
    return submit(() -> {
      String methodName = "";
      if (body.getManualScaling() != null) {
        methodName = "set_num_instances";
      } else if (body.getServingStatus() != null) {
        if ("SERVING".equals(body.getServingStatus())) {
          methodName = "start_version";
        } else if ("STOPPED".equals(body.getServingStatus())) {
          methodName = "stop_version";
        }
      }
      Appengine client = getAdminAPIClientWithUseragent(methodName);
      client.apps().services().versions().patch(this.projectId, module, version, body)
          .setUpdateMask(updateMask)
          .execute();
      return null;
    });
  }

  // --- Inner Classes ---

  private abstract static class ModulesServiceFutureWrapper<V> extends FutureWrapper<byte[], V> {
    private final String method;

    public ModulesServiceFutureWrapper(Future<byte[]> delegate, String method) {
      super(delegate);
      this.method = method;
    }

    @VisibleForTesting
    protected Future<byte[]> makeApiCall(String packageName, String methodName, byte[] request) {
      return ApiProxy.makeAsyncCall(packageName, methodName, request);
    }

    @Override
    protected Throwable convertException(Throwable cause) {
      if (cause instanceof ApiProxy.ApplicationException) {
        return convertApplicationException(method, (ApiProxy.ApplicationException) cause);
      } else if (cause instanceof InvalidProtocolBufferException) {
        return new ModulesException("Unexpected failure", cause);
      } else {
        return cause;
      }
    }

    private RuntimeException convertApplicationException(String method,
        ApiProxy.ApplicationException e) {
      switch (ErrorCode.forNumber(e.getApplicationError())) {
        case INVALID_MODULE:
          return new ModulesException("Unknown module");
        case INVALID_VERSION:
          return new ModulesException("Unknown module version");
        case INVALID_INSTANCES:
          return new ModulesException("Invalid instance");
        case UNEXPECTED_STATE:
          if (method.equals("StartModule") || method.equals("StopModule")) {
            return new UnexpectedStateException("Unexpected state for method " + method);
          } else {
            return new ModulesException("Unexpected state with method '" + method + "'");
          }
        default:
          return new ModulesException("Unknown error: '" + e.getApplicationError() + "'");
      }
    }
  }

  private static class IgnoreUnexpectedStateExceptionFuture extends ForwardingFuture<Void> {
    private final Future<Void> delegate;
    private final String logMessage;

    IgnoreUnexpectedStateExceptionFuture(Future<Void> delegate, String logMessage) {
      this.delegate = delegate;
      this.logMessage = logMessage;
    }

    @Override
    protected Future<Void> delegate() {
      return delegate;
    }

    @Override
    public Void get() throws InterruptedException, ExecutionException {
      try {
        return delegate.get();
      } catch (ExecutionException ee) {
        return throwOriginalUnlessUnexpectedState(ee);
      }
    }

    @Override
    public Void get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      try {
        return delegate.get(timeout, unit);
      } catch (ExecutionException ee) {
        return throwOriginalUnlessUnexpectedState(ee);
      }
    }

    private Void throwOriginalUnlessUnexpectedState(ExecutionException original)
        throws ExecutionException {
      Throwable cause = original.getCause();
      if (cause instanceof UnexpectedStateException) {
        logger.info(logMessage);
        return null;
      } else {
        throw original;
      }
    }
  }
}
