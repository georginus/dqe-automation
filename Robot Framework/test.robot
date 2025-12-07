*** Settings ***
Library           SeleniumLibrary
Library           helper.py

Suite Setup       Open Browser To Report
Suite Teardown    Close Browser

*** Variables ***
${REPORT_FILE}      ${CURDIR}/../Selenium Introduction/report.html
${PARQUET_FOLDER}   ${CURDIR}/parquet_data/facility_name_min_time_spent_per_visit_date
${FILTER_DATE}      2025-12-05

*** Test Cases ***
Compare HTML Table With Parquet Dataset
    Wait Until Element Is Visible    css:.plotly-graph-div    10s
    Sleep    1s
    ${plotly_script}=    Get Plotly Script Text
    ${df_html}=    Parse Plotly Table From Script    ${plotly_script}
    ${df_parquet}=    Prepare Parquet For Comparison     ${PARQUET_FOLDER}   ${FILTER_DATE}
    ${match}    ${diff}=    Compare Dataframes    ${df_html}    ${df_parquet}
    Run Keyword If    ${match}    Log    DataFrames match!
    Run Keyword Unless    ${match}    Fail    DataFrames do not match!\n${diff}

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
    [Return]    ${plotly_script}

Get Table Data From Script
    ${table_data}=    Execute Javascript
    ...    var scripts = document.getElementsByTagName('script');
    ...    for (var i = 0; i < scripts.length; i++) {
    ...        var txt = scripts[i].textContent;
    ...        if (txt.includes('Plotly.newPlot')) {
    ...            var start = txt.indexOf('Plotly.newPlot');
    ...            if (start === -1) continue;
    ...            var arrStart = txt.indexOf('[', start);
    ...            var depth = 0, end = -1;
    ...            for (var j = arrStart; j < txt.length; j++) {
    ...                if (txt[j] === '[') depth++;
    ...                if (txt[j] === ']') depth--;
    ...                if (depth === 0) { end = j; break; }
    ...            }
    ...            if (arrStart !== -1 && end !== -1) {
    ...                var arrText = txt.substring(arrStart, end + 1);
    ...                try { var arr = JSON.parse(arrText.replace(/(['"])?([a-zA-Z0-9_]+)(['"])?:/g, '"$2":')); var table = null; for (var t = 0; t < arr.length; t++) { if (arr[t].type && arr[t].type === 'table') { table = arr[t]; break; } } if (!table) return null; var headers = table.header.values; var values = table.cells.values; var rows = []; for (var m =
 0; m < values[0].length; m++) { var row = []; for (var n = 0; n < values.length; n++) { row.push(values[n][m]); } rows.push(row); } return [headers].concat(rows); } catch(e) { return null; }
    ...            }
    ...        }
    ...    }
    ...    return null;
    [Return]    ${table_data}