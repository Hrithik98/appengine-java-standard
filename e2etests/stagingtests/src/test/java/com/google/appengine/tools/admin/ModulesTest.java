package com.google.appengine.tools.admin;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.TruthJUnit.assume;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * End-to-end tests for Modules API.
 *
 * <p>To run these tests, navigate to
 * experimental/users/hrithikgajera/modules/appengine-java-standard first.
 *
 * <p>1. Deploy the modules-test-app application to your App Engine project: ./mvnw -pl
 * e2etests/testlocalapps/modules-test-app package appengine:deploy -DprojectId=<your-project-id>
 *
 * <p>2. Run the test and set the deployed application URL in deployed.app.url system property:
 * ./mvnw -pl e2etests/stagingtests test -Dtest=ModulesTest
 * -Ddeployed.app.url=https://modules-test-app-dot-<your-project-id>.appspot.com
 */
@RunWith(JUnit4.class)
public class ModulesTest {

  @Test
  public void testModulesApi() throws Exception {
    String appUrl = System.getProperty("deployed.app.url");
    assume().that(appUrl).isNotNull();
    URL url = new URL(appUrl + "/modules");
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod("GET");
    int responseCode = connection.getResponseCode();
    assertThat(responseCode).isEqualTo(200);
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
      String line;
      StringBuilder response = new StringBuilder();
      while ((line = reader.readLine()) != null) {
        response.append(line);
      }
      assertThat(response.toString()).contains("Current module: modules-test-app");
    }
  }
}
