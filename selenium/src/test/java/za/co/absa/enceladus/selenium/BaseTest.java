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

package za.co.absa.enceladus.selenium;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

import org.openqa.selenium.*;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.interactions.Actions;
import org.openqa.selenium.support.ui.ExpectedCondition;
import org.openqa.selenium.support.ui.WebDriverWait;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

public class BaseTest {
  String postfix = dtf.format(LocalDateTime.now());
  String username = "user";
  By schemasView = By.id("__navigation0---schemaDetailView");
  By datasetsView = By.id("__navigation0---datasetDetailView");
  By mappingTablesView = By.id("__navigation0---mappingTableDetailView");
  WebDriver driver;
  WebDriverWait wait;
  private By nameField = By.name("username");
  private By passwordField = By.name("password");
  private String password = "changeme";
  private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy_MM_dd-HH_mm_ss");

  @BeforeClass
  public void setUpSuite() {
    ChromeOptions driverOptions = new ChromeOptions();
    // OHEADLESS option
    // driverOptions.setHeadless(true);
    Dimension dimensions = new Dimension(1920, 1080);

    driver = new ChromeDriver(driverOptions);
    driver.manage().window().setSize(dimensions);
    driver.navigate().to("http://127.0.0.1:8080/menas");
    wait = new WebDriverWait(driver, 5);
  }

  @BeforeMethod
  public void setUpMethod() throws InterruptedException {
    login(username, password);
    TimeUnit.SECONDS.sleep(1);
  }

  @AfterMethod
  public void tearDownMethod() throws InterruptedException {
    TimeUnit.SECONDS.sleep(1);
    hoverClick(By.xpath("//button[contains(@id, '--logout')  and " +
                        "not(ancestor::div[contains(@class,'sapMNavItemHidden')])]"));
    TimeUnit.SECONDS.sleep(1);
  }

  @AfterClass
  public void tearDownSuite() throws InterruptedException {
    TimeUnit.SECONDS.sleep(5);
    driver.close();
    driver.quit();
  }

  /**
   * Lets user choose which tab in the side navigation menu he would like
   * to go to. Takes By locator of that tab as an input.
   *
   * @param  by  By locator to the element of choice in the left navigation
   *              panel
   */
  void chooseTab(By by)  throws InterruptedException {
    hoverClick(By.id("__xmlview0--menasApp-MasterBtn"));
    TimeUnit.SECONDS.sleep(1);
    hoverClick(by);
    hoverClick(By.id("__xmlview0--Navigation"));
    TimeUnit.SECONDS.sleep(1);
  }

  /**
   * Helps with a standard login usecase so the test are not dependant
   * on this proces.
   *
   * @param  username  Username of the test user
   * @param  password  Password of the test user
   */
  void login(String username, String password) {
    WebElement usernameElement = driver.findElement(nameField);
    WebElement passwordElement = driver.findElement(passwordField);
    WebElement submitElement = driver.findElement(By.name("submit"));

    usernameElement.sendKeys(username);
    passwordElement.sendKeys(password);
    submitElement.click();
  }

  /**
   * Checks if the element is present anywhere in the DOM. This is level
   * above the isDisplayed method. If the element is present then we can
   * check if it is displayed. Element isDisplayed on an unexisting element
   * throws an error.
   *
   * @param  by  By locator to the element of choice
   * @return Returns true or false, depending on the presenence of the
   *          element
   */
  Boolean isPresent(By by) {
    try {
      driver.findElement(by);
      return true;
    } catch (org.openqa.selenium.NoSuchElementException e) {
      return false;
    }
  }

  /**
   * Finds the element then simulates mouse hovering over it and clicks it.
   * This method is created for those instances when multiple element overlap
   * and there is an error (or the possibility) which says "Another element
   * would recieve the click"
   *
   * @param  by  By locator to the element of choice
   */
  void hoverClick(By by) {
    WebElement element = driver.findElement(by);
    hoverClick(element);
  }

  /**
   * Simulates mouse hovering over element and clicks it.
   * This method is created for those instances when multiple element overlap
   * and there is an error (or the possibility) which says "Another element
   * would recieve the click"
   *
   * @param  element  By locator to the element of choice
   */
  void hoverClick(WebElement element) {
    Actions builder = new Actions(driver);
    int xOffset = element.getLocation().getX() < 0 ? Math.abs(element.getLocation().getX()) + 2 : 0;

    builder.moveToElement(element, xOffset, 2).click().build().perform();
  }

  /**
   * Checks if the element has a class of choice defined on it. This checks
   * the class for the whole name, not part of it.
   *
   * @param  by  By locator to the element of choice
   * @param  clazz  Class of choice
   * @return Returns true or false. Depends if the element has the class
   *          defined or not
   */
  boolean hasClass(By by, String clazz) {
    WebElement element = driver.findElement(by);
    String clazzes = element.getAttribute("class");

    for (String k : clazzes.split(" ")) {
        if (k.equals(clazz)) {
            return true;
        }
    }

    return false;
  }

  /**
   * Checks if the element is displayed or not. If element is not even in DOM
   * still returns boolean.
   *
   * @param  by  By locator to the element of choice
   * @return Returns true or false. Depending if the element is present and
   *          displayed.
   */
  boolean isElementVisible(By by) {
    try {
      return driver.findElement(by).isDisplayed();
    } catch (org.openqa.selenium.NoSuchElementException e) {
      return false;
    }
  }

  void waitForPageToLoad() {
    new WebDriverWait(driver, 30).until((ExpectedCondition<Boolean>) wd ->
      ((JavascriptExecutor) wd).executeScript("document.body.addEventListener('DOMSubtreeModified', function () {\n"
                                              + "  return true;\n"
                                              + "}, false);").equals(true));
  }
}
