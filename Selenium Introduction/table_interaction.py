import csv
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

class WebDriverContextManager:
    def __init__(self, driver_path=None):
        self.driver_path = driver_path
        self.driver = None

    def __enter__(self):
        # Initialize the Chrome WebDriver
        self.driver = webdriver.Chrome(executable_path=self.driver_path) if self.driver_path else webdriver.Chrome()
        return self.driver

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Quit the WebDriver on exit
        if self.driver:
            self.driver.quit()

def extract_plotly_table_to_csv(file_name="report.html"):
    file_path = f"file://{os.path.abspath(file_name)}"
    with WebDriverContextManager() as driver:
        try:
            driver.get(file_path)

            # 1. Wait for the Plotly table container to appear (By.CLASS_NAME)
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "plotly-graph-div"))
            )

            # 2. Try to find the Plotly div by ID (By.ID)
            try:
                plotly_div = driver.find_element(By.ID, "abc317a1-29ba-4609-87e4-317833df0ae4")
            except NoSuchElementException:
                plotly_div = None  # Not critical, just for demonstration

            # 3. Try to find the Plotly div by XPath (By.XPATH)
            try:
                plotly_div_xpath = driver.find_element(By.XPATH, "//div[contains(@class, 'plotly-graph-div')]")
            except NoSuchElementException:
                plotly_div_xpath = None  # Not critical, just for demonstration

            # 4. Try to find the Plotly div by CSS Selector (By.CSS_SELECTOR)
            try:
                plotly_div_css = driver.find_element(By.CSS_SELECTOR, ".plotly-graph-div")
            except NoSuchElementException:
                plotly_div_css = None  # Not critical, just for demonstration

            # 5. Extract table data from Plotly using JavaScript
            plotly_data = driver.execute_script("""
                let gd = document.querySelector('.plotly-graph-div');
                if (!gd || !gd.data) return null;
                let table = gd.data.find(x => x.type === 'table');
                if (!table) return null;
                let headers = table.header.values;
                let values = table.cells.values;
                let rows = [];
                for (let i = 0; i < values[0].length; i++) {
                    let row = [];
                    for (let j = 0; j < values.length; j++) {
                        row.push(values[j][i]);
                    }

                    rows.push(row);
                }
                return [headers, ...rows];
            """)

            if not plotly_data:
                print("Could not find a Plotly table on the page.")
                return

            # 6. Save the extracted data to table.csv
            with open("table.csv", "w", newline='', encoding="utf-8") as f:
                writer = csv.writer(f)
                for row in plotly_data:
                    writer.writerow(row)
            print("Table successfully saved to table.csv")

        except TimeoutException:
            print("The table did not load in time.")
        except Exception as e:
            print(f"An error occurred: {e}")

if __name__ == "__main__":
    extract_plotly_table_to_csv("report.html")
