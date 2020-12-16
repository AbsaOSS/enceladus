/*
 * Copyright 2018 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.enceladus.menas.web.controllers

import javax.servlet.http.HttpServletRequest
import org.apache.commons.io.IOUtils
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.core.io.ClassPathResource
import org.springframework.http.MediaType.TEXT_HTML_VALUE
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.{RequestMapping, ResponseBody}
import org.webjars.WebJarAssetLocator
import za.co.absa.enceladus.menas.web.LineageConfig

@Controller
class LineageController @Autowired()(webJarAssetLocator: WebJarAssetLocator) {

  @RequestMapping(path = Array(LineageConfig.mappingPathForController), produces = Array(TEXT_HTML_VALUE))
  @ResponseBody
  def index(httpRequest: HttpServletRequest): String = {
    val resourceName = webJarAssetLocator.getFullPath(LineageConfig.jarName, "index.html")

    val resource = new ClassPathResource(resourceName)

    val baseUrl = LineageConfig.baseUrl(httpRequest.getContextPath)
    // TODO: Don't use string replaces #1116
   IOUtils.toString(resource.getInputStream, "UTF-8")
      .replaceAllLiterally(
        "/*[[${embeddedMode}]]*/",
        "true; //")
      .replaceAllLiterally(
        "/*[[${apiUrl}]]*/",
        s"'${LineageConfig.apiUrl.getOrElse("")}'; //")
      .replaceAllLiterally(
        """<base href="/" th:href="@{/}">""",
        s"""<base href="$baseUrl">""")
      .replaceAllLiterally("/*[+", "")
      .replaceAllLiterally("+]*/", "")
  }

}
