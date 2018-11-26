package za.co.absa;

import org.testng.annotations.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
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
    WebElement eName = driver.findElement(newSchemaName);
    WebElement eDescription = driver.findElement(newSchemaDescription);

    assertTrue(isPresent(newSchemaAddButton));
    assertTrue(isPresent(newSchemaCancelButton));

    eName.sendKeys(name);
    eDescription.sendKeys(description);
    driver.findElement(newSchemaAddButton).click();

    Schema currentSchema = new Schema(driver);

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

    Schema toBeDeleted = new Schema(driver);
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

//    TimeUnit.SECONDS.sleep(1);
    Schema currentSchema = new Schema(driver);

    String changeInDescription = "To Be Updated - " + postfix;
    hoverClick(editButton);
    WebElement descField = driver.findElement(descriptionField);
    descField.clear();
    descField.sendKeys(changeInDescription);
    hoverClick(saveButton);

    // TimeUnit.SECONDS.sleep(1);
    Schema newSchema = new Schema(driver);

    assertEquals(currentSchema.getName(), newSchema.getName());
    assertEquals(changeInDescription, newSchema.getDescription());
    assertEquals(currentSchema.getVersion() + 1, newSchema.getVersion());
    assertEquals(username, newSchema.getUpdatedBy());
    assertEquals(currentSchema.getCreatedBy(), newSchema.getCreatedBy());
    assertEquals(currentSchema.getCreationDate(), newSchema.getCreationDate());
    assertTrue(0 < newSchema.compareUpdateTimes(currentSchema));
  }
}
