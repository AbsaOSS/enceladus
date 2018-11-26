package za.co.absa.models;

import org.openqa.selenium.WebDriver;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.By;
import org.openqa.selenium.support.ui.WebDriverWait;

import java.time.LocalDateTime;

public class Schema extends Base{

  public Schema(WebDriver webdriver) {
    super(webdriver);
    WebDriverWait wait = new WebDriverWait(driver, 10);
    String schemaNavigation = "__navigation0---schemaMainView--";
    By nameSelector = By.id(schemaNavigation + "currentShemaName");
    wait.until(ExpectedConditions.visibilityOfElementLocated(nameSelector));
    this.name = getText(nameSelector);
    this.description = getText(By.id(schemaNavigation + "currentShemaDescription"));
    this.version = Integer.parseInt(getText(By.id(schemaNavigation + "currentShemaVersion")));
    this.createdBy = getText(By.id(schemaNavigation + "currentShemaUserUpdated"));
    this.updatedBy = getText(By.id(schemaNavigation + "currentShemaUserCreated"));
    this.creationDate = LocalDateTime.parse(getText(By.id(schemaNavigation + "currentShemaDateCreated")), timeStampFormatter);
    this.updateDate = LocalDateTime.parse(getText(By.id(schemaNavigation + "currentShemaLastUpdated")), timeStampFormatter);
  }

}
