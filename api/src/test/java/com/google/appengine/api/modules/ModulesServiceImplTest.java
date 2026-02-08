package com.google.appengine.api.modules;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.services.appengine.v1.Appengine;
import com.google.api.services.appengine.v1.model.Application;
import com.google.api.services.appengine.v1.model.ListServicesResponse;
import com.google.api.services.appengine.v1.model.ListVersionsResponse;
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
import com.google.appengine.api.modules.ModulesServicePb.ModulesServiceError;
import com.google.appengine.api.modules.ModulesServicePb.SetNumInstancesRequest;
import com.google.appengine.api.modules.ModulesServicePb.SetNumInstancesResponse;
import com.google.appengine.api.modules.ModulesServicePb.StartModuleRequest;
import com.google.appengine.api.modules.ModulesServicePb.StartModuleResponse;
import com.google.appengine.api.modules.ModulesServicePb.StopModuleRequest;
import com.google.appengine.api.modules.ModulesServicePb.StopModuleResponse;
import com.google.apphosting.api.ApiProxy;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class ModulesServiceImplTest {
  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  private static final String FAKE_MODULE = "fake-module";
  private static final String FAKE_VERSION = "fake-version";
  private static final String FAKE_INSTANCE_ID = "fake-instance-id";
  private static final String FAKE_HOSTNAME = "fake-hostname";
  private static final String INSTANCE_ID_ENV_ATTRIBUTE = "com.google.appengine.instance.id";

  @Mock private ApiProxy.Environment environment;
  @Mock private Future<byte[]> mockFuture;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private Appengine mockAppengine;

  private ModulesServiceImpl service;
  private TestModulesServiceImpl testService;

  @Before
  public void setUp() throws Exception {
    ApiProxy.setEnvironmentForCurrentThread(environment);
    when(environment.getModuleId()).thenReturn(FAKE_MODULE);
    when(environment.getVersionId()).thenReturn(FAKE_VERSION + ".123");
    when(environment.getAttributes())
        .thenReturn(ImmutableMap.of(INSTANCE_ID_ENV_ATTRIBUTE, FAKE_INSTANCE_ID));
    testService = new TestModulesServiceImpl();
    service = testService;
  }

  private class TestModulesServiceImpl extends ModulesServiceImpl {
    private boolean useAdminApi = false;
    private Future<byte[]> apiCallFuture = mockFuture;

    TestModulesServiceImpl() {
      super("test-project");
    }

    void setUseAdminApi(boolean useAdminApi) {
      this.useAdminApi = useAdminApi;
    }

    void setApiCallFuture(Future<byte[]> apiCallFuture) {
      this.apiCallFuture = apiCallFuture;
    }

    @Override
    protected boolean hasOptedIn() {
      return useAdminApi;
    }

    @Override
    protected Future<byte[]> makeApiCall(String packageName, String methodName, byte[] request) {
      return apiCallFuture;
    }

    @Override
    protected Appengine getAdminAPIClientWithUseragent(String methodName) {
      return mockAppengine;
    }
  }

  @Test
  public void getCurrentModule() {
    assertThat(service.getCurrentModule()).isEqualTo(FAKE_MODULE);
  }

  @Test
  public void getCurrentVersion() {
    assertThat(service.getCurrentVersion()).isEqualTo(FAKE_VERSION);
  }

  @Test
  public void getCurrentInstanceId() {
    assertThat(service.getCurrentInstanceId()).isEqualTo(FAKE_INSTANCE_ID);
  }

  @Test
  public void getCurrentInstanceId_missing() {
    when(environment.getAttributes()).thenReturn(ImmutableMap.of());
    assertThrows(ModulesException.class, () -> service.getCurrentInstanceId());
  }

  @Test
  public void getModules_legacy() throws Exception {
    GetModulesResponse response =
        GetModulesResponse.newBuilder().addModule("default").addModule("module1").build();
    when(mockFuture.get()).thenReturn(response.toByteArray());

    Set<String> modules = service.getModules();
    assertThat(modules).containsExactly("default", "module1");
  }

  @Test
  public void getVersions_legacy() throws Exception {
    GetVersionsResponse response =
        GetVersionsResponse.newBuilder().addVersion("v1").addVersion("v2").build();
    when(mockFuture.get()).thenReturn(response.toByteArray());

    Set<String> versions = service.getVersions(FAKE_MODULE);
    assertThat(versions).containsExactly("v1", "v2");
  }

  @Test
  public void getDefaultVersion_legacy() throws Exception {
    GetDefaultVersionResponse response =
        GetDefaultVersionResponse.newBuilder().setVersion("v1").build();
    when(mockFuture.get()).thenReturn(response.toByteArray());

    String defaultVersion = service.getDefaultVersion(FAKE_MODULE);
    assertThat(defaultVersion).isEqualTo("v1");
  }

  @Test
  public void getNumInstances_legacy() throws Exception {
    GetNumInstancesResponse response = GetNumInstancesResponse.newBuilder().setInstances(5).build();
    when(mockFuture.get()).thenReturn(response.toByteArray());

    int numInstances = service.getNumInstances(FAKE_MODULE, FAKE_VERSION);
    assertThat(numInstances).isEqualTo(5);
  }

  @Test
  public void setNumInstances_legacy() throws Exception {
    SetNumInstancesResponse response = SetNumInstancesResponse.getDefaultInstance();
    when(mockFuture.get()).thenReturn(response.toByteArray());

    service.setNumInstances(FAKE_MODULE, FAKE_VERSION, 10);
    // No exception thrown means success
  }

  @Test
  public void startVersion_legacy() throws Exception {
    StartModuleResponse response = StartModuleResponse.getDefaultInstance();
    when(mockFuture.get()).thenReturn(response.toByteArray());

    service.startVersion(FAKE_MODULE, FAKE_VERSION);
    // No exception thrown means success
  }

  @Test
  public void stopVersion_legacy() throws Exception {
    StopModuleResponse response = StopModuleResponse.getDefaultInstance();
    when(mockFuture.get()).thenReturn(response.toByteArray());

    service.stopVersion(FAKE_MODULE, FAKE_VERSION);
    // No exception thrown means success
  }

  @Test
  public void getVersionHostname_legacy() throws Exception {
    GetHostnameResponse response =
        GetHostnameResponse.newBuilder().setHostname(FAKE_HOSTNAME).build();
    when(mockFuture.get()).thenReturn(response.toByteArray());

    String hostname = service.getVersionHostname(FAKE_MODULE, FAKE_VERSION);
    assertThat(hostname).isEqualTo(FAKE_HOSTNAME);
  }

  @Test
  public void getInstanceHostname_legacy() throws Exception {
    GetHostnameResponse response =
        GetHostnameResponse.newBuilder().setHostname(FAKE_HOSTNAME).build();
    when(mockFuture.get()).thenReturn(response.toByteArray());

    String hostname = service.getInstanceHostname(FAKE_MODULE, FAKE_VERSION, "1");
    assertThat(hostname).isEqualTo(FAKE_HOSTNAME);
  }

  @Test
  public void invalidModule_legacy() throws Exception {
    when(mockFuture.get())
        .thenThrow(
            new ExecutionException(
                new ApiProxy.ApplicationException(
                    ModulesServiceError.ErrorCode.INVALID_MODULE.getNumber(), "Error")));
    assertThrows(ModulesException.class, () -> service.getVersions("invalid-module"));
  }

  @Test
  public void invalidVersion_legacy() throws Exception {
    when(mockFuture.get())
        .thenThrow(
            new ExecutionException(
                new ApiProxy.ApplicationException(
                    ModulesServiceError.ErrorCode.INVALID_VERSION.getNumber(), "Error")));
    assertThrows(
        ModulesException.class, () -> service.getNumInstances("invalid-module", "invalid-version"));
  }

  @Test
  public void unexpectedState_startModule_legacy() throws Exception {
    when(mockFuture.get())
        .thenThrow(
            new ExecutionException(
                new ApiProxy.ApplicationException(
                    ModulesServiceError.ErrorCode.UNEXPECTED_STATE.getNumber(), "Error")));
    // Should not throw exception.
    service.startVersion(FAKE_MODULE, FAKE_VERSION);
  }

  @Test
  public void unexpectedState_stopModule_legacy() throws Exception {
    when(mockFuture.get())
        .thenThrow(
            new ExecutionException(
                new ApiProxy.ApplicationException(
                    ModulesServiceError.ErrorCode.UNEXPECTED_STATE.getNumber(), "Error")));
    // Should not throw exception.
    service.stopVersion(FAKE_MODULE, FAKE_VERSION);
  }

  @Test
  public void getModules_adminApi() throws Exception {
    testService.setUseAdminApi(true);
    Service service1 = new Service().setId("default");
    Service service2 = new Service().setId("module1");
    ListServicesResponse listServicesResponse =
        new ListServicesResponse().setServices(Arrays.asList(service1, service2));
    when(mockAppengine.apps().services().list("test-project").execute())
        .thenReturn(listServicesResponse);

    Set<String> modules = service.getModules();
    assertThat(modules).containsExactly("default", "module1");
  }

  @Test
  public void getVersions_adminApi() throws Exception {
    testService.setUseAdminApi(true);
    Version version1 = new Version().setId("v1");
    Version version2 = new Version().setId("v2");
    ListVersionsResponse listVersionsResponse =
        new ListVersionsResponse().setVersions(Arrays.asList(version1, version2));
    when(mockAppengine.apps().services().versions().list("test-project", FAKE_MODULE).execute())
        .thenReturn(listVersionsResponse);

    Set<String> versions = service.getVersions(FAKE_MODULE);
    assertThat(versions).containsExactly("v1", "v2");
  }

  @Test
  public void getDefaultVersion_adminApi() throws Exception {
    testService.setUseAdminApi(true);
    Map<String, Double> allocations = new HashMap<>();
    allocations.put("v1", 1.0);
    allocations.put("v2", 0.0);
    Service serviceResponse =
        new Service().setSplit(new TrafficSplit().setAllocations(allocations));
    when(mockAppengine.apps().services().get("test-project", FAKE_MODULE).execute())
        .thenReturn(serviceResponse);

    String defaultVersion = service.getDefaultVersion(FAKE_MODULE);
    assertThat(defaultVersion).isEqualTo("v1");
  }

  @Test
  public void getNumInstances_adminApi() throws Exception {
    testService.setUseAdminApi(true);
    Version versionResponse = new Version().setManualScaling(new ManualScaling().setInstances(5));
    when(mockAppengine
            .apps()
            .services()
            .versions()
            .get("test-project", FAKE_MODULE, FAKE_VERSION)
            .execute())
        .thenReturn(versionResponse);

    int numInstances = service.getNumInstances(FAKE_MODULE, FAKE_VERSION);
    assertThat(numInstances).isEqualTo(5);
  }

  @Test
  public void setNumInstances_adminApi() throws Exception {
    testService.setUseAdminApi(true);
    service.setNumInstances(FAKE_MODULE, FAKE_VERSION, 10);
    verify(
            mockAppengine
                .apps()
                .services()
                .versions()
                .patch(
                    "test-project",
                    FAKE_MODULE,
                    FAKE_VERSION,
                    new Version().setManualScaling(new ManualScaling().setInstances(10))))
        .setUpdateMask("manualScaling.instances");
  }

  @Test
  public void startVersion_adminApi() throws Exception {
    testService.setUseAdminApi(true);
    service.startVersion(FAKE_MODULE, FAKE_VERSION);
    verify(
            mockAppengine
                .apps()
                .services()
                .versions()
                .patch(
                    "test-project",
                    FAKE_MODULE,
                    FAKE_VERSION,
                    new Version().setServingStatus("SERVING")))
        .setUpdateMask("servingStatus");
  }

  @Test
  public void stopVersion_adminApi() throws Exception {
    testService.setUseAdminApi(true);
    service.stopVersion(FAKE_MODULE, FAKE_VERSION);
    verify(
            mockAppengine
                .apps()
                .services()
                .versions()
                .patch(
                    "test-project",
                    FAKE_MODULE,
                    FAKE_VERSION,
                    new Version().setServingStatus("STOPPED")))
        .setUpdateMask("servingStatus");
  }

  @Test
  public void getVersionHostname_adminApi() throws Exception {
    testService.setUseAdminApi(true);
    when(mockAppengine.apps().get("test-project").execute())
        .thenReturn(new Application().setDefaultHostname("app.appspot.com"));
    Service service1 = new Service().setId("default");
    Service service2 = new Service().setId(FAKE_MODULE);
    ListServicesResponse listServicesResponse =
        new ListServicesResponse().setServices(Arrays.asList(service1, service2));
    when(mockAppengine.apps().services().list("test-project").execute())
        .thenReturn(listServicesResponse);
    String hostname = service.getVersionHostname(FAKE_MODULE, FAKE_VERSION);
    assertThat(hostname).isEqualTo(FAKE_VERSION + "." + FAKE_MODULE + ".app.appspot.com");
  }

  @Test
  public void getInstanceHostname_adminApi() throws Exception {
    testService.setUseAdminApi(true);
    when(mockAppengine.apps().get("test-project").execute())
        .thenReturn(new Application().setDefaultHostname("app.appspot.com"));
    Service service1 = new Service().setId("default");
    Service service2 = new Service().setId(FAKE_MODULE);
    ListServicesResponse listServicesResponse =
        new ListServicesResponse().setServices(Arrays.asList(service1, service2));
    when(mockAppengine.apps().services().list("test-project").execute())
        .thenReturn(listServicesResponse);
    Version versionResponse = new Version().setManualScaling(new ManualScaling().setInstances(5));
    when(mockAppengine
            .apps()
            .services()
            .versions()
            .get("test-project", FAKE_MODULE, FAKE_VERSION)
            .setView("FULL")
            .execute())
        .thenReturn(versionResponse);

    String hostname = service.getInstanceHostname(FAKE_MODULE, FAKE_VERSION, "1");
    assertThat(hostname).isEqualTo("1." + FAKE_VERSION + "." + FAKE_MODULE + ".app.appspot.com");
  }

  @Test
  public void getInstanceHostname_adminApi_notManualScaling() throws Exception {
    testService.setUseAdminApi(true);
    when(mockAppengine.apps().get("test-project").execute())
        .thenReturn(new Application().setDefaultHostname("app.appspot.com"));
    Service service1 = new Service().setId("default");
    Service service2 = new Service().setId(FAKE_MODULE);
    ListServicesResponse listServicesResponse =
        new ListServicesResponse().setServices(Arrays.asList(service1, service2));
    when(mockAppengine.apps().services().list("test-project").execute())
        .thenReturn(listServicesResponse);
    Version versionResponse = new Version(); // No manual scaling
    when(mockAppengine
            .apps()
            .services()
            .versions()
            .get("test-project", FAKE_MODULE, FAKE_VERSION)
            .setView("FULL")
            .execute())
        .thenReturn(versionResponse);

    assertThrows(
        ModulesException.class, () -> service.getInstanceHostname(FAKE_MODULE, FAKE_VERSION, "1"));
  }
}
