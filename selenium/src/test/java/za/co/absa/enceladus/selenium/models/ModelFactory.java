/*
 * Copyright 2018-2019 ABSA Group Limited
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

package za.co.absa.enceladus.selenium.models;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.junit.rules.ExpectedException;
import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

public class ModelFactory {
  private static final DateTimeFormatter timeStampFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy, HH:mm");
  private static final String schemaNavigation = "__navigation0---schemaMainView--";
  private static final String datasetNavigation = "__navigation0---datasetMainView--";
  private static final int defaultWaitTime = 10;

  public static Dataset getDataset(WebDriver driver, String nameCheck) {
    WebDriverWait wait = new WebDriverWait(driver, defaultWaitTime);
    wait.until(d -> d.findElement(By.xpath("//span[@id='" + datasetNavigation + "currentDatasetName' and text()='" + nameCheck + "']")));

    By nameSelector = By.id(datasetNavigation + "currentDatasetName");
    String name = getText(driver, nameSelector);
    String description = getText(driver, By.id(datasetNavigation + "currentDatasetDescription"));
    int version = Integer.parseInt(getText(driver, By.id(datasetNavigation + "currentDatasetVersion")));
    String rawPath = getText(driver, By.id(datasetNavigation + "currentDatasetRawPath"));
    String publishPath = getText(driver, By.id(datasetNavigation + "currentDatasetPublishedPath"));
    String schema = getText(driver, By.id(datasetNavigation + "currentDatasetSchmea"));
    String createdBy = getText(driver, By.id(datasetNavigation + "currentDatasetUserUpdated"));
    String updatedBy = getText(driver, By.id(datasetNavigation + "currentDatasetUserCreated"));
    LocalDateTime creationDate = LocalDateTime.parse(getText(driver, By.id(datasetNavigation + "currentDatasetCreated")), timeStampFormatter);
    LocalDateTime updateDate = LocalDateTime.parse(getText(driver, By.id(datasetNavigation + "currentDatasetLastUpdate")), timeStampFormatter);

    return new Dataset(name, description, version, createdBy, updatedBy, creationDate, updateDate, rawPath, publishPath, schema);
  }

  public static Schema getSchema(WebDriver driver, String nameCheck) {
    WebDriverWait wait = new WebDriverWait(driver, defaultWaitTime);
    wait.until(d -> d.findElement(By.xpath("//span[@id='" + schemaNavigation + "currentShemaName' and text()='" + nameCheck + "']")));

    By nameSelector = By.id(schemaNavigation + "currentShemaName");
    String name = getText(driver, nameSelector);
    String description = getText(driver, By.id(schemaNavigation + "currentShemaDescription"));
    int version = Integer.parseInt(getText(driver, By.id(schemaNavigation + "currentShemaVersion")));
    String createdBy = getText(driver, By.id(schemaNavigation + "currentShemaUserUpdated"));
    String updatedBy = getText(driver, By.id(schemaNavigation + "currentShemaUserCreated"));
    LocalDateTime creationDate = LocalDateTime.parse(getText(driver, By.id(schemaNavigation + "currentShemaDateCreated")), timeStampFormatter);
    LocalDateTime updateDate = LocalDateTime.parse(getText(driver, By.id(schemaNavigation + "currentShemaLastUpdated")), timeStampFormatter);

    return new Schema(name, description, version, createdBy, updatedBy, creationDate, updateDate);
  }

  /**
   * Find an element and get its text.
   * @param by By locator to the element of choice
   * @return Returns inner text from an element
   */
  private static String getText(WebDriver driver, By by) {
    return driver.findElement(by).getText();
  }
}
