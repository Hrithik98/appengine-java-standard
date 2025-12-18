package com.google.appengine.api.modules;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import com.google.api.services.appengine.v1.Appengine;
import com.google.api.services.appengine.v1.model.*;
import com.google.apphosting.api.ApiProxy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Future;

@ExtendWith(MockitoExtension.class)
class ModulesServiceImplTest {

    @Mock
    private ApiProxy.Delegate<ApiProxy.Environment> mockDelegate;

    @Mock
    private ApiProxy.Environment mockEnvironment;

    @Mock(answer = org.mockito.Answers.RETURNS_DEEP_STUBS)
    private Appengine mockAppengineClient;

    private ModulesServiceImpl modulesService;
    
    // Toggle this boolean to switch paths in tests
    private boolean useModernPath = false; 

    @BeforeEach
    void setUp() {
        // 1. Setup the Internal ApiProxy Environment
        ApiProxy.setDelegate(mockDelegate);
        ApiProxy.setEnvironmentForCurrentThread(mockEnvironment);

        lenient().when(mockEnvironment.getModuleId()).thenReturn("default");
        lenient().when(mockEnvironment.getVersionId()).thenReturn("testVersion.12345");
        lenient().when(mockEnvironment.getAttributes()).thenReturn(new HashMap<>());

        // 2. Initialize the service using the NEW @VisibleForTesting constructor
        modulesService = new ModulesServiceImpl("test-project") {
            
            @Override
            protected Appengine getAdminAPIClientWithUseragent(String methodName) {
                return mockAppengineClient;
            }

            @Override
            protected boolean hasOptedIn() {
                return useModernPath;
            }
        };
    }

    @AfterEach
    void tearDown() {
        ApiProxy.setDelegate(null);
        ApiProxy.clearEnvironmentForCurrentThread();
    }

    // =========================================================================
    // SECTION 1: Base Functionality
    // =========================================================================

    @Nested
    class BaseFunctionalityTests {
        @Test
        void testGetCurrentModule() {
            assertEquals("default", modulesService.getCurrentModule());
        }

        @Test
        void testGetCurrentVersion() {
            assertEquals("testVersion", modulesService.getCurrentVersion());
        }

        @Test
        void testGetCurrentInstanceId_Missing() {
            assertThrows(ModulesException.class, () -> modulesService.getCurrentInstanceId());
        }

        @Test
        void testFindDefaultVersion_Logic() {
            Map<String, Double> allocations = new HashMap<>();
            allocations.put("v1", 0.0);
            allocations.put("v2", 1.0);
            assertEquals("v2", ModulesServiceImpl.findDefaultVersionFromAllocations(allocations));
        }
    }

    // =========================================================================
    // SECTION 2: Legacy Path
    // =========================================================================

    @Nested
    class LegacyImplementationTests {

        @BeforeEach
        void setLegacyMode() {
            useModernPath = false;
        }

        @Test
        void testGetModules() throws Exception {
            byte[] fakeResponse = com.google.appengine.api.modules.ModulesServicePb.GetModulesResponse.newBuilder()
                    .addModule("default")
                    .addModule("backend")
                    .build()
                    .toByteArray();

            Future<byte[]> mockFuture = mock(Future.class);
            when(mockFuture.get()).thenReturn(fakeResponse);

            when(mockDelegate.makeAsyncCall(
                    any(ApiProxy.Environment.class), 
                    eq("modules"),                   
                    eq("GetModules"),                
                    any(byte[].class),               
                    any(ApiProxy.ApiConfig.class)    
            )).thenReturn(mockFuture);

            Set<String> modules = modulesService.getModules();
            assertTrue(modules.contains("default"));
            assertTrue(modules.contains("backend"));
        }

        @Test
        void testGetVersions() throws Exception {
            byte[] fakeResponse = com.google.appengine.api.modules.ModulesServicePb.GetVersionsResponse.newBuilder()
                    .addVersion("v1")
                    .build()
                    .toByteArray();

            Future<byte[]> mockFuture = mock(Future.class);
            when(mockFuture.get()).thenReturn(fakeResponse);

            when(mockDelegate.makeAsyncCall(
                    any(ApiProxy.Environment.class), 
                    eq("modules"),                   
                    eq("GetVersions"),               
                    any(byte[].class),               
                    any(ApiProxy.ApiConfig.class)    
            )).thenReturn(mockFuture);

            Set<String> versions = modulesService.getVersions("default");
            assertTrue(versions.contains("v1"));
        }
    }

    // =========================================================================
    // SECTION 3: Modern Path
    // =========================================================================

    @Nested
    class ModernImplementationTests {

        @BeforeEach
        void setOptInMode() {
            useModernPath = true;
        }

        @Test
        void testGetModules() throws IOException {
            ListServicesResponse response = new ListServicesResponse();
            Service s1 = new Service().setId("default");
            response.setServices(Collections.singletonList(s1));

            when(mockAppengineClient.apps().services().list("test-project").execute())
                    .thenReturn(response);

            Set<String> modules = modulesService.getModules();
            assertTrue(modules.contains("default"));
        }

        @Test
        void testStartVersion() throws IOException {
            modulesService.startVersion("default", "v1");
            verify(mockAppengineClient.apps().services().versions())
                    .patch(eq("test-project"), eq("default"), eq("v1"), 
                           argThat(v -> "SERVING".equals(v.getServingStatus())));
        }
    }
}
