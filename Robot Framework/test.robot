*** Settings ***
Library           SeleniumLibrary
Library           helper.py

Suite Setup       Open Browser To Report
Suite Teardown    Close Browser

*** Variables ***
${REPORT_FILE}      ${CURDIR}/../Selenium Introduction/report.html
${PARQUET_FOLDER}   ${CURDIR}/../parquet_data/facility_name_min_time_spent_per_visit_date
${FILTER_DATE}      2025-12-05
*** Test Cases ***
Compare HTML Table With Parquet Dataset
    Wait Until Element Is Visible    css:.plotly-graph-div    10s
    Sleep    1s
    ${plotly_script}=    Get Plotly Script Text
    ${df_html}=          Parse Plotly Table From Script    ${plotly_script}
    ${df_html}=          Filter DataFrame By Date    ${df_html}    ${FILTER_DATE}
    ${df_parquet}=       Prepare Parquet For Comparison     ${PARQUET_FOLDER}   ${FILTER_DATE}
    ${match}    ${diff}=    Compare Dataframes    ${df_html}    ${df_parquet}
    IF    not ${match}
        Fail    DataFrames do not match!\n${diff}
    END

*** Keywords ***
Open Browser To Report
    Open Browser    ${REPORT_FILE}    Chrome
    Maximize Browser Window

Get Plotly Script Text
    ${plotly_script}=    Execute Javascript
    ...    var scripts = document.getElementsByTagName('script');
    ...    for (var i = 0; i < scripts.length; i++) {
    ...        var txt = scripts[i].textContent;
    ...        if (txt.includes('Plotly.newPlot') && txt.includes('[')) {
    ...            if (txt.trim().startsWith('window.PLOTLYENV')) {
    ...                return txt;
    ...            }
    ...        }
    ...    }
    ...    return null;
    RETURN    ${plotly_script}