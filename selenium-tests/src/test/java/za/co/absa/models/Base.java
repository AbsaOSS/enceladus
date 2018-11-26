package za.co.absa.models;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.openqa.selenium.support.ui.WebDriverWait;

class Base {
  DateTimeFormatter timeStampFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy, HH:mm");

  String name;
  String description;
  int version;
  String createdBy;
  String updatedBy;
  LocalDateTime creationDate;
  LocalDateTime updateDate;
  WebDriver driver;

  Base(WebDriver webDriver) {
    this.driver = webDriver;
  }

  /**
   * Find an element and get its text
   * @param by By locator to the element of choice
   * @return Returns inner text from an element
   */
  String getText(By by) {
    return driver.findElement(by).getText();
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  public int getVersion() {
    return version;
  }

  public String getCreatedBy() {
    return createdBy;
  }

  public String getUpdatedBy() {
    return updatedBy;
  }

  public LocalDateTime getCreationDate() {
    return creationDate;
  }

  public LocalDateTime getUpdateDate() {
    return updateDate;
  }

  public int compareUpdateTimes(Base second) {
    return updateDate.compareTo(second.getUpdateDate());
  }
}
