package com.google.appengine.testlocalapps;

import com.google.appengine.api.modules.ModulesService;
import com.google.appengine.api.modules.ModulesServiceFactory;
import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class ModulesTestServlet extends HttpServlet {
  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    ModulesService modulesService = ModulesServiceFactory.getModulesService();
    resp.setContentType("text/plain");
    resp.getWriter().println("Current module: " + modulesService.getCurrentModule());
    resp.getWriter().println("Current version: " + modulesService.getCurrentVersion());
    resp.getWriter().println("Current instance id: " + modulesService.getCurrentInstanceId());
    resp.getWriter().println("Modules: " + modulesService.getModules());
    resp.getWriter()
        .println(
            "Versions for "
                + modulesService.getCurrentModule()
                + ": "
                + modulesService.getVersions(modulesService.getCurrentModule()));
    resp.getWriter()
        .println(
            "Default version for "
                + modulesService.getCurrentModule()
                + ": "
                + modulesService.getDefaultVersion(modulesService.getCurrentModule()));
  }
}
