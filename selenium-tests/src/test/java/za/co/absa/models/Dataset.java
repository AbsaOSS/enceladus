package za.co.absa.models;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

import java.time.LocalDateTime;

public class Dataset extends Base {
  private String rawPath;
  private String publishPath;
  private String schema;

  public Dataset(WebDriver webDriver) {
    super(webDriver);
    WebDriverWait wait = new WebDriverWait(driver, 10);
    String datasetNavigation = "__navigation0---datasetMainView--";
    By nameSelector = By.id(datasetNavigation + "currentDatasetName");
    wait.until(ExpectedConditions.visibilityOfElementLocated(nameSelector));
    this.name = getText(nameSelector);
    this.description = getText(By.id(datasetNavigation + "currentDatasetDescription"));
    this.version = Integer.parseInt(getText(By.id(datasetNavigation + "currentDatasetVersion")));
    this.rawPath = getText(By.id(datasetNavigation + "currentDatasetRawPath"));
    this.publishPath = getText(By.id(datasetNavigation + "currentDatasetPublishedPath"));
    this.schema = getText(By.id(datasetNavigation + "currentDatasetSchmea"));
    this.createdBy = getText(By.id(datasetNavigation + "currentShemaUserUpdated"));
    this.updatedBy = getText(By.id(datasetNavigation + "currentShemaUserCreated"));
    this.creationDate = LocalDateTime.parse(getText(By.id(datasetNavigation + "currentShemaDateCreated")), timeStampFormatter);
    this.updateDate = LocalDateTime.parse(getText(By.id(datasetNavigation + "currentShemaLastUpdated")), timeStampFormatter);
  }

  public String getRawPath() {
    return rawPath;
  }

  public String getPublishPath() {
    return publishPath;
  }

  public String getSchema() {
    return schema;
  }
}
