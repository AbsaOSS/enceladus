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

package za.co.absa.enceladus.selenium;

import org.openqa.selenium.WebDriver;
import org.openqa.selenium.support.ui.ExpectedCondition;
import org.testng.annotations.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import za.co.absa.enceladus.selenium.models.ModelFactory;
import za.co.absa.enceladus.selenium.models.Schema;

import static org.testng.AssertJUnit.*;

public class SchemaTest extends BaseTest {
  private By schemasTab = By.id("__xmlview0--Schemas");
  private String schemaNavigation = "__navigation0---schemaMainView--";

  @Test
  public void addSchema() throws InterruptedException {
    String name = "Schema " + postfix;
    String description = "Alfa, Beta, Gama, Delta";

    By addSchemaButton = By.id(schemaNavigation + "NewSchema");
    By newSchemaName = By.id("newSchemaName-inner");
    By newSchemaDescription = By.id("newSchemaDescription-inner");
    By newSchemaAddButton = By.id("newSchemaAddButton");
    By newSchemaCancelButton = By.id("newSchemaCancelButton");

    chooseTab(schemasTab);
    hoverClick(addSchemaButton);
    wait.until(d -> d.findElement(By.xpath("//span[@id='addSchemaDialog-title-inner' and text()='New Schema']")));
    WebElement elementName = driver.findElement(newSchemaName);
    WebElement elementDescription = driver.findElement(newSchemaDescription);

    assertTrue(isPresent(newSchemaAddButton));
    assertTrue(isPresent(newSchemaCancelButton));

    elementName.sendKeys(name);
    elementDescription.sendKeys(description);
    driver.findElement(newSchemaAddButton).click();

    Schema currentSchema = ModelFactory.getSchema(driver, name);

    assertEquals(name, currentSchema.getName());
    assertEquals(description, currentSchema.getDescription());
    assertEquals(0, currentSchema.getVersion());
    assertEquals(username, currentSchema.getCreatedBy());
    assertEquals(username, currentSchema.getUpdatedBy());
  }

  @Test
  public void removeSchema() {
    By itemToDeleteSelector = By.xpath("//div[contains(text(), 'Schema')]/ancestor::li");
    By deleteButtonSelector = By.id(schemaNavigation + "Delete");
    By okButtonSelector = By.xpath("//bdi[text()='Yes']/ancestor::button");

    WebElement element = driver.findElements(itemToDeleteSelector).get(0);
    String name = element.findElement(By.xpath(".//div[contains(text(),'Schema')]")).getText();
    hoverClick(element);

    Schema toBeDeleted = ModelFactory.getSchema(driver, name);
    By deletedSchema = By.xpath("//li//div[text()='" + toBeDeleted.getName() + "']");

    hoverClick(deleteButtonSelector);
    hoverClick(okButtonSelector);

    wait.until((ExpectedCondition<Boolean>) d -> !d.findElement(By.id("sap-ui-blocklayer-popup")).isDisplayed());
    boolean present = isPresent(deletedSchema);
    assertFalse(present);
  }

  @Test
  public void editSchema() {
    By schemaToUpdate = By.xpath("//li//div[text()='UpdateMe']");
    By editButton = By.id(schemaNavigation + "Edit");
    By descriptionField = By.id("editSchemaDescription-inner");
    By saveButton = By.id("editSchemaSaveButton-inner");
    hoverClick(schemaToUpdate);

    Schema currentSchema = ModelFactory.getSchema(driver, "UpdateMe");

    String changeInDescription = "To Be Updated - " + postfix;
    hoverClick(editButton);
    WebElement descField = driver.findElement(descriptionField);
    descField.clear();
    descField.sendKeys(changeInDescription);
    hoverClick(saveButton);

    Schema newSchema = ModelFactory.getSchema(driver, "UpdateMe");

    assertEquals(currentSchema.getName(), newSchema.getName());
    assertEquals(changeInDescription, newSchema.getDescription());
    assertEquals(currentSchema.getVersion() + 1, newSchema.getVersion());
    assertEquals(username, newSchema.getUpdatedBy());
    assertEquals(currentSchema.getCreatedBy(), newSchema.getCreatedBy());
    assertEquals(currentSchema.getCreationDate(), newSchema.getCreationDate());
    assertTrue(0 < newSchema.compareUpdateTimes(currentSchema));
  }
}
