# **cdsspy** 

<!-- badges: start -->

[![Dependencies](https://img.shields.io/badge/dependencies-12/01-orange?style=flat)](#)
[![License:
MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://choosealicense.com/licenses/mit/)
<!-- badges: end -->

<div align="left">

<p align="left">
<a href="https://dwr.state.co.us/Tools"><strong>« CDSS »</strong></a>
<br /> <a href="https://dwr.state.co.us/Rest/GET/Help">CDSS REST Web
Services</a>
</p>

</div>

<hr>

The goal of `cdsspy` is to provide functions that help Python users to navigate, explore, and make requests to the CDSS REST API web service. 

The Colorado’s Decision Support Systems (CDSS) is a water management system created and developed by the Colorado Water Conservation Board (CWCB) and the Colorado Division of Water Resources (DWR).

Thank you to those at CWCB and DWR for providing an accessible and well documented REST API!

---

[**GitHub**](https://github.com/anguswg-ucsb/cdsspy)

[**PyPI**](https://pypi.org/project/cdsspy/)

[**cdssr (R Package)**](https://github.com/anguswg-ucsb/cdssr)

---

## Installation
Install the latest version of `cdsspy` from PyPI:

``` 
pip install cdsspy
```
<br>

# Example: Telemetry site data
**Identify query inputs using reference tables**

The **get_reference_tbl()** function will return tables that makes it easier to know what information should be supplied to the data retrieval functions in cdsspy. For more information on the exact reference tables click [here](https://dwr.state.co.us/rest/get/help#Datasets&#ReferenceTablesController&#gettingstarted&#jsonxml).

Let’s locate the parameters available at telemetry stations.

```python
# available parameters for telemetry stations
telemetry_params = cdsspy.get_reference_tbl(
    table_name = "telemetryparams"
    )
```
<br>

**Locate stations**

We can use the **get_\<endpoint>\_stations()** functions to identify the stations within a given spatial extent (point/polygon), water district, division, or county. Station data can also be retrieved by providing a specific station abbreviation, GNIS ID, USGS ID, or WDID.

```python
# identify telemetry stations in water district 6
stations  = cdsspy.get_telemetry_stations(
    water_district = "6"
    )
```

**Retrieve Telemetry station timeseries data**

The **get_\<endpoint>\_ts()** functions retrieve timeseries data from the CDSS API.

We can then take a station abbreviations from the **get_telemetry_stations()** call, a parameter from the **get_reference_tbl()** call, and use this information as inputs into the get_telemetry_ts() function.

The function call below with return a daily discharge timeseries for the ANDDITCO site between 2015-2022

```python
# identify telemetry stations in water district 6
# stations  = cdsspy.get_telemetry_ts(
#     abbrev         = stations.abbrev[1],
#     parameter      = telemetry_params.parameter[7],
#     start_date     = "2015-01-01",
#     end_date       = "2022-01-01",
#     timescale      = "day"
#     )
```