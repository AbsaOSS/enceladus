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

import org.testng.annotations.Test;
import org.openqa.selenium.By;

import static org.testng.AssertJUnit.*;

public class AppTest extends BaseTest {

  private void checkLayout(By[] elements) {
    for (By element : elements) {
      assertTrue(isElementVisible(element));
    }
  }

  @Test
  public void schemaPageLayout() throws InterruptedException {
    By schemasTab = By.id("__xmlview0--Schemas");

    By[] elements = {
      By.id("__navigation0---schemaDetailView--BasicInfo"),
      By.id("__navigation0---schemaDetailView--Fields"),
      By.id("__navigation0---schemaDetailView--UploadNew"),
      By.id("__navigation0---schemaDetailView--UsedIn"),
      By.id("__navigation0---schemaDetailView--Delete"),
      By.id("__navigation0---schemaDetailView--Edit")
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
      By.id("__navigation0---datasetDetailView--BasicInfo"),
      By.id("__navigation0---datasetDetailView--Schema"),
      By.id("__navigation0---datasetDetailView--Delete"),
      By.id("__navigation0---datasetDetailView--Edit")
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
      By.id("__navigation0---mappingTableDetailView--BasicInfo"),
      By.id("__navigation0---mappingTableDetailView--Schema"),
      By.id("__navigation0---mappingTableDetailView--UsedIn"),
      By.id("__navigation0---mappingTableDetailView--Defaults"),
      By.id("__navigation0---mappingTableDetailView--Delete"),
      By.id("__navigation0---mappingTableDetailView--AddDefaultValue"),
      By.id("__navigation0---mappingTableDetailView--Edit")
    };

    chooseTab(mappingTablesTab);
    assertTrue(hasClass(schemasView, "sapMNavItemHidden"));
    assertFalse(isPresent(datasetsView));
    assertFalse(hasClass(mappingTablesView, "sapMNavItemHidden"));
    checkLayout(elements);
  }
}
