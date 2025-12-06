import os
import csv
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException


class WebDriverContextManager:
    def __init__(self, driver_path=None):
        self.driver_path = driver_path
        self.driver = None

    def __enter__(self):
        self.driver = webdriver.Chrome(executable_path=self.driver_path) if self.driver_path else webdriver.Chrome()
        return self.driver

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.driver:
            self.driver.quit()


def save_doughnut_data(driver, index):
    data = driver.execute_script("""
        let gd = document.querySelector('.plotly-graph-div');
        if (!gd || !gd.data) return null;
        let pie = gd.data.find(x => x.type === 'pie');
        if (!pie || !pie.labels || !pie.values || !pie.values._inputArray) return null;
        let labels = pie.labels;
        let inputArray = pie.values._inputArray;
        let result = [["Facility Type", "Min Average Time Spent"]];
        for (let i = 0; i < labels.length; i++) {
            let value = inputArray[i] !== undefined ? inputArray[i] : "";
            result.push([labels[i], value]);
        }
        return result;
    """)
    if not data:
        print(f"Could not extract doughnut chart data at stage {index}.")
        return
    with open(f"doughnut{index}.csv", "w", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(data)
    print(f"Doughnut chart data saved to doughnut{index}.csv")

def doughnut_chart_interaction(file_name="report.html"):
    file_path = f"file://{os.path.abspath(file_name)}"
    with WebDriverContextManager() as driver:
        try:
            driver.get(file_path)
            # Wait for the Plotly chart to load
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "plotly-graph-div"))
            )
            time.sleep(1)  # Additional wait for full rendering

            # Take initial screenshot
            driver.save_screenshot("screenshot0.png")
            print("Initial screenshot saved as screenshot0.png")
            save_doughnut_data(driver, 0)

            # Find legend items (filters) by CSS Selector
            legend_items = driver.find_elements(By.CSS_SELECTOR, ".legendtoggle")
            if not legend_items:
                # Alternative: try to find legend items by XPath
                legend_items = driver.find_elements(By.XPATH, "//g[@class='legend']/g[@class='traces']/g[@class='legendtoggle']")
            if not legend_items:
                print("No legend items (filters) found for doughnut chart.")
                return

            # Iterate through each filter option
            for idx, legend in enumerate(legend_items, start=1):
                try:
                    legend.click()
                    time.sleep(1)  # Wait for chart to update
                    driver.save_screenshot(f"screenshot{idx}.png")
                    print(f"Screenshot saved as screenshot{idx}.png")
                    save_doughnut_data(driver, idx)
                except (ElementClickInterceptedException, NoSuchElementException) as e:
                    print(f"Failed to click legend item {idx}: {e}")
                except Exception as e:
                    print(f"Unexpected error at legend item {idx}: {e}")

            # Edge case: unselect all filters (if possible)
            # Try to click all legend items again to unselect all
            for legend in legend_items:
                try:
                    legend.click()
                    time.sleep(0.5)
                except Exception:
                    pass
            driver.save_screenshot(f"screenshot{len(legend_items)+1}.png")
            print(f"Screenshot saved as screenshot{len(legend_items)+1}.png")
            save_doughnut_data(driver, len(legend_items)+1)

        except TimeoutException:
            print("The doughnut chart did not load in time.")
        except Exception as e:
            print(f"An error occurred: {e}")


if __name__ == "__main__":
    doughnut_chart_interaction("report.html")
