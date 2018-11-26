/*
 * <!--
 *   ~ Copyright 2018 ABSA Group Limited
 *   ~
 *   ~ Licensed under the Apache License, Version 2.0 (the "License");
 *   ~ you may not use this file except in compliance with the License.
 *   ~ You may obtain a copy of the License at
 *   ~     http://www.apache.org/licenses/LICENSE-2.0
 *   ~
 *   ~ Unless required by applicable law or agreed to in writing, software
 *   ~ distributed under the License is distributed on an "AS IS" BASIS,
 *   ~ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   ~ See the License for the specific language governing permissions and
 *   ~ limitations under the License.
 *   -->
 */

package za.co.absa;

import org.testng.annotations.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import za.co.absa.models.ModelFactory;
import za.co.absa.models.Schema;

import static org.testng.AssertJUnit.*;

import java.util.concurrent.TimeUnit;

public class SchemaTest extends BaseTest {
  private By schemasTab = By.id("__xmlview0--Schemas");
  private String schemaNavigation = "__navigation0---schemaMainView--";

  @Test
  public void addSchema() throws InterruptedException {
    String name = "Omega " + postfix;
    String description = "Alfa, Beta, Gama, Delta";

    By addSchemaButton = By.id(schemaNavigation + "NewSchema");
    By newSchemaName = By.id("newSchemaName-inner");
    By newSchemaDescription = By.id("newSchemaDescription-inner");
    By newSchemaAddButton = By.id("newSchemaAddButton");
    By newSchemaCancelButton = By.id("newSchemaCancelButton");

    chooseTab(schemasTab);
    hoverClick(addSchemaButton);
    TimeUnit.SECONDS.sleep(1);
    WebElement elementName = driver.findElement(newSchemaName);
    WebElement elementDescription = driver.findElement(newSchemaDescription);

    assertTrue(isPresent(newSchemaAddButton));
    assertTrue(isPresent(newSchemaCancelButton));

    elementName.sendKeys(name);
    elementDescription.sendKeys(description);
    driver.findElement(newSchemaAddButton).click();

    Schema currentSchema = ModelFactory.getSchema(driver);

    assertEquals(name, currentSchema.getName());
    assertEquals(description, currentSchema.getDescription());
    assertEquals(0, currentSchema.getVersion());
    assertEquals(username, currentSchema.getCreatedBy());
    assertEquals(username, currentSchema.getUpdatedBy());
  }

  @Test
  public void removeSchema() {
    By itemToDeleteSelector = By.xpath("//div[contains(text(), 'Omega')]/ancestor::li");
    By deleteButtonSelector = By.id(schemaNavigation + "Delete");
    By okButtonSelector = By.xpath("//bdi[text()='Yes']/ancestor::button");

    WebElement element = driver.findElements(itemToDeleteSelector).get(0);
    hoverClick(element);

    Schema toBeDeleted = ModelFactory.getSchema(driver);
    By deletedSchema = By.xpath("//li//div[text()='"+ toBeDeleted.getName() +"']");

    hoverClick(deleteButtonSelector);
    hoverClick(okButtonSelector);
    assertFalse(isPresent(deletedSchema));
  }

  @Test
  public void editSchema() {
    By schemaToUpdate = By.xpath("//li//div[text()='UpdateMe']");
    By editButton = By.id(schemaNavigation + "Edit");
    By descriptionField = By.id("editSchemaDescription-inner");
    By saveButton = By.id("editSchemaSaveButton-inner");
    hoverClick(schemaToUpdate);

    Schema currentSchema = ModelFactory.getSchema(driver);

    String changeInDescription = "To Be Updated - " + postfix;
    hoverClick(editButton);
    WebElement descField = driver.findElement(descriptionField);
    descField.clear();
    descField.sendKeys(changeInDescription);
    hoverClick(saveButton);

    Schema newSchema = ModelFactory.getSchema(driver);

    assertEquals(currentSchema.getName(), newSchema.getName());
    assertEquals(changeInDescription, newSchema.getDescription());
    assertEquals(currentSchema.getVersion() + 1, newSchema.getVersion());
    assertEquals(username, newSchema.getUpdatedBy());
    assertEquals(currentSchema.getCreatedBy(), newSchema.getCreatedBy());
    assertEquals(currentSchema.getCreationDate(), newSchema.getCreationDate());
    assertTrue(0 < newSchema.compareUpdateTimes(currentSchema));
  }
}
