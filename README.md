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

## **Available endpoints**

Below is a table of all of the CDSS API endpoints **`cdsspy`** has
functions for.

| **-** | **Function**                    | **Description**                                                    | **Endpoint**                                                                                                                                                                                                                         |
|-------|---------------------------------|--------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1     | **get_admin_calls()**           | Returns list of active/historic administrative calls               | [administrativecalls/active](https://dwr.state.co.us/rest/get/help#Datasets&#AdministrativeCallsController&https://dnrweblink.state.co.us/dwr/ElectronicFile.aspx?docid=3600964&dbid=0&#gettingstarted&#jsonxml)                     |
| 2     | **get_structures()**            | Returns list of administrative structures                          | [structures](https://dwr.state.co.us/rest/get/help#Datasets&#StructuresController&#gettingstarted&#jsonxml)                                                                                                                          |
| 3     | **get_structures_divrec()**     | Returns list of diversion/release/stage records based on WDID      | [structures/divrec/](https://dwr.state.co.us/rest/get/help#Datasets&#DiversionRecordsController&https://dnrweblink.state.co.us/dwr/ElectronicFile.aspx?docid=3600965&dbid=0&#gettingstarted&#jsonxml)                                |
| 4     | **get_climate_stations()**      | Returns Climate Stations                                           | [climatedata/climatestations](https://dwr.state.co.us/rest/get/help#Datasets&#ClimateStationsController&https://www.ncdc.noaa.gov/cdo-web/webservices&https://www.northernwater.org/our-data/weather-data&#gettingstarted&#jsonxml)  |
| 5     | **get_climate_ts()**            | Returns Climate Station Time Series (day, month, year)             | [climatedata/climatestationts](https://dwr.state.co.us/rest/get/help#Datasets&#ClimateStationsController&https://www.ncdc.noaa.gov/cdo-web/webservices&https://www.northernwater.org/our-data/weather-data&#gettingstarted&#jsonxml) |
| 6     | **get_gw_gplogs_wells()**       | Returns Groundwater GeophysicalLogsWell from filters               | [groundwater/geophysicallogs/](https://dwr.state.co.us/rest/get/help#Datasets&#GroundwaterGeophysicalLogsController&#gettingstarted&#jsonxml)                                                                                        |
| 7     | **get_gw_gplogs_geologpicks()** | Returns Groundwater Geophysical Log picks by well ID               | [groundwater/geophysicallogs/](https://dwr.state.co.us/rest/get/help#Datasets&#GroundwaterGeophysicalLogsController&#gettingstarted&#jsonxml)                                                                                        |
| 7     | **get_gw_wl_wells()**           | Returns WaterLevelsWell from filters                               | [groundwater/waterlevels/wells](https://dwr.state.co.us/rest/get/help#Datasets&#GroundwaterLevelsController&#gettingstarted&#jsonxml)                                                                                                |
| 6     | **get_gw_wl_wellmeasures()**    | Returns Groundwater Measurements                                   | [groundwater/waterlevels/wellmeasurements](https://dwr.state.co.us/rest/get/help#Datasets&#GroundwaterLevelsController&#gettingstarted&#jsonxml)                                                                                     |
| 8     | **get_reference_tbl()**         | Returns reference tables list                                      | [referencetables/](https://dwr.state.co.us/rest/get/help#Datasets&#ReferenceTablesController&#gettingstarted&#jsonxml)                                                                                                               |
| 9     | **get_sw_stations()**           | Returns Surface Water Station info                                 | [surfacewater/surfacewaterstations](https://dwr.state.co.us/rest/get/help#Datasets&#SurfaceWaterController&#gettingstarted&#jsonxml)                                                                                                 |
| 10    | **get_sw_ts()**                 | Returns Surface Water Time Series                                  | [surfacewater/surfacewaterts](https://dwr.state.co.us/rest/get/help#Datasets&#SurfaceWaterController&#gettingstarted&#jsonxml)                                                                                                       |
| 11    | **get_telemetry_stations()**    | Returns telemetry stations and their most recent parameter reading | [telemetrystations/telemetrystation](https://dwr.state.co.us/rest/get/help#Datasets&#TelemetryStationsController&#gettingstarted&#jsonxml)                                                                                           |
| 12    | **get_telemetry_ts()**          | Returns telemetry time series data (raw, hour, day)                | [telemetrystations/telemetrytimeseries](https://dwr.state.co.us/rest/get/help#Datasets&#TelemetryStationsController&#gettingstarted&#jsonxml)                                                                                        |

<br>

## **Identify query inputs using reference tables**

The **get_reference_tbl()** function will return tables that makes it easier to know what information should be supplied to the data retrieval functions in cdsspy. For more information on the exact reference tables click [here](https://dwr.state.co.us/rest/get/help#Datasets&#ReferenceTablesController&#gettingstarted&#jsonxml).

<br>
Let’s locate the parameters available at telemetry stations.

```python
# available parameters for telemetry stations
telemetry_params = cdsspy.get_reference_tbl(
    table_name = "telemetryparams"
    )
```

<br>

## **Locate stations**

We can use the **get_\<endpoint>\_stations()** set of functions to identify different types of stations within a given water district, division, or county. Station data can also be retrieved by providing a specific station abbreviation, GNIS ID, USGS ID, or WDID. 

<br>
Here we locate all the telemetry stations within water district 6:

```python
# identify telemetry stations in water district 6
stations  = cdsspy.get_telemetry_stations(
    water_district = "6"
    )
```

<br>

## **Retrieve Telemetry station timeseries data**

The **get_\<endpoint>\_ts()** set of functions retrieve timeseries data from the CDSS API.

We can then take a station abbreviations from the **get_telemetry_stations()** call, a parameter from the **get_reference_tbl()** call, and use this information as inputs into the **get_telemetry_ts()** function.

<br>

The function call below with return a daily discharge timeseries for the ANDDITCO site between 2015-2022:

```python
# identify telemetry stations in water district 6
discharge_ts   = cdsspy.get_telemetry_ts(
    abbrev         = "ANDDITCO",
    parameter      = telemetry_params.parameter[6],
    start_date     = "2015-01-01",
    end_date       = "2022-01-01",
    timescale      = "day"
    )
```

![](https://cdsspy-images.s3.us-west-1.amazonaws.com/daily_dischargge_plot.png)

<br>

## **Retrieve groundwater data**
The **get_gw_()** set of functions lets users make get requests to the various CDSS API groundwater endpoints shown in the table below:

| **-** | **Function**                    | **Endpoint**                                                                                                                                     |
|-------|---------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------|
| 1     | **get_gw_wl_wellmeasures()**    | [api/v2/groundwater/waterlevels/wellmeasurements](https://dwr.state.co.us/Rest/GET/Help/Api/GET-api-v2-groundwater-waterlevels-wellmeasurements) |
| 2     | **get_gw_wl_wells()**           | [api/v2/groundwater/waterlevels/wells](https://dwr.state.co.us/Rest/GET/Help/Api/GET-api-v2-groundwater-waterlevels-wells)                       |
| 3     | **get_gw_gplogs_wells()**       | [api/v2/groundwater/geophysicallogs/wells](https://dwr.state.co.us/Rest/GET/Help/Api/GET-api-v2-groundwater-geophysicallogs-wells)               |
| 4     | **get_gw_gplogs_geologpicks()** | [api/v2/groundwater/geophysicallogs/geoplogpicks](https://dwr.state.co.us/Rest/GET/Help/Api/GET-api-v2-groundwater-geophysicallogs-geoplogpicks) |

<br>

Here we will retrieve groundwater well measurement data for Well ID 1274 between 1990-2022.

```python
# Request wellmeasurements endpoint (api/v2/groundwater/waterlevels/wellmeasurements)
well_measure = cdsspy.get_gw_wl_wellmeasures(
    wellid     = "1274",
    start_date = "1990-01-01",
    end_date   = "2022-01-01"
    )
```