package za.co.absa;

import org.testng.annotations.Test;
import org.openqa.selenium.By;

import static org.testng.AssertJUnit.*;

public class AppTest extends BaseTest {

  private void checkLayout(By[] elements) {
    for (By var : elements) {
      assertTrue(isElementVisible(var));
    }
  }

  @Test
  public void schemaPageLayout() throws InterruptedException {
    By schemasTab = By.id("__xmlview0--Schemas");

    By[] elements = {
      By.id("__navigation0---schemaMainView--BasicInfo"),
      By.id("__navigation0---schemaMainView--Fields"),
      By.id("__navigation0---schemaMainView--UploadNew"),
      By.id("__navigation0---schemaMainView--UsedIn"),
      By.id("__navigation0---schemaMainView--Delete"),
      By.id("__navigation0---schemaMainView--Edit")
    };

    chooseTab(schemasTab);
    assertFalse(hasClass(schemasView, "sapMNavItemHidden"));
    assertFalse(isPresent(datasetsView));
    assertFalse(isPresent(mappingTablesView));
    checkLayout(elements);
  }

  @Test
  public void datasetsPageLayout() throws InterruptedException {
    By datasetsTab = By.id("__xmlview0--Datasets");

    By[] elements = {
      By.id("__navigation0---datasetMainView--BasicInfo"),
      By.id("__navigation0---datasetMainView--Schema"),
      By.id("__navigation0---datasetMainView--Delete"),
      By.id("__navigation0---datasetMainView--Edit")
    };

    chooseTab(datasetsTab);
    assertTrue(hasClass(schemasView, "sapMNavItemHidden"));
    assertFalse(hasClass(datasetsView, "sapMNavItemHidden"));
    assertFalse(isPresent(mappingTablesView));
    checkLayout(elements);
  }

  @Test
  public void mappingTablesPageLayout() throws InterruptedException {
    By mappingTablesTab = By.id("__xmlview0--MappingTables");

    By[] elements = {
      By.id("__navigation0---mappingTableMainView--BasicInfo"),
      By.id("__navigation0---mappingTableMainView--Schema"),
      By.id("__navigation0---mappingTableMainView--UsedIn"),
      By.id("__navigation0---mappingTableMainView--Defaults"),
      By.id("__navigation0---mappingTableMainView--Delete"),
      By.id("__navigation0---mappingTableMainView--AddDefaultValue"),
      By.id("__navigation0---mappingTableMainView--Edit")
    };

    chooseTab(mappingTablesTab);
    assertTrue(hasClass(schemasView, "sapMNavItemHidden"));
    assertFalse(isPresent(datasetsView));
    assertFalse(hasClass(mappingTablesView, "sapMNavItemHidden"));
    checkLayout(elements);
  }
}
