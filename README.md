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

The goal of **`cdsspy`** is to provide functions that help Python users to navigate, explore, and make requests to the CDSS REST API web service. 

The Colorado’s Decision Support Systems (CDSS) is a water management system created and developed by the Colorado Water Conservation Board (CWCB) and the Colorado Division of Water Resources (DWR).

Thank you to those at CWCB and DWR for providing an accessible and well documented REST API!

<style>
div.blue { background-color:#e6f0ff; border-radius: 5px; padding: 10px;}
</style>
<div class = "blue">
If you are looking for the equivalent R package, see 
<a href="https://github.com/anguswg-ucsb/cdssr"><strong>cdssr</strong></a>
</div>

---

- [**cdssr GitHub (R)**](https://github.com/anguswg-ucsb/cdssr)

- [**cdsspy Github (Python)**](https://github.com/anguswg-ucsb/cdsspy)

- [**cdssr documentation**](https://anguswg-ucsb.github.io/cdssr/)

- [**cdsspy PyPI**](https://pypi.org/project/cdsspy/)

---

<br> 

## Installation
Install the latest version of `cdsspy` from PyPI:

``` 
pip install cdsspy
```
<br>

## **Available endpoints**

Below is a table of all of the CDSS API endpoints **`cdsspy`** provides functions for.

|**-** |**Function**                    | **Description**                                                    | **Endpoint**                                 |
|------|--------------------------------| -------------------------------------------------------------------|----------------------------------------------|
|1     | **get_admin_calls()**          | Returns list of active/historic administrative calls               | [administrativecalls/active](https://dwr.state.co.us/rest/get/help#Datasets&#AdministrativeCallsController&https://dnrweblink.state.co.us/dwr/ElectronicFile.aspx?docid=3600964&dbid=0&#gettingstarted&#jsonxml)            |
|2     | **get_structures()**           | Returns list of administrative structures                          | [structures](https://dwr.state.co.us/rest/get/help#Datasets&#StructuresController&#gettingstarted&#jsonxml)                            |
|3     | **get_structures_divrec()**    | Returns list of diversion/release/stage records based on WDID      | [structures/divrec/](https://dwr.state.co.us/rest/get/help#Datasets&#DiversionRecordsController&https://dnrweblink.state.co.us/dwr/ElectronicFile.aspx?docid=3600965&dbid=0&#gettingstarted&#jsonxml)              |
|4     | **get_climate_stations()**     | Returns Climate Stations                                           | [climatedata/climatestations](https://dwr.state.co.us/rest/get/help#Datasets&#ClimateStationsController&https://www.ncdc.noaa.gov/cdo-web/webservices&https://www.northernwater.org/our-data/weather-data&#gettingstarted&#jsonxml)           |
|5     | **get_climate_ts()**           | Returns Climate Station Time Series (day, month, year)             | [climatedata/climatestationts](https://dwr.state.co.us/rest/get/help#Datasets&#ClimateStationsController&https://www.ncdc.noaa.gov/cdo-web/webservices&https://www.northernwater.org/our-data/weather-data&#gettingstarted&#jsonxml)       |
|6     | **get_climate_frostdates()**           | Returns Climate Station Frost Dates             | [climatedata/climatestationfrostdates](https://dwr.state.co.us/rest/get/help#Datasets&#ClimateStationsController&https://www.ncdc.noaa.gov/cdo-web/webservices&https://www.northernwater.org/our-data/weather-data&#gettingstarted&#jsonxml)       |
|7     | **get_gw_gplogs_wells()**          | Returns Groundwater GeophysicalLogsWell from filters           | [groundwater/geophysicallogs/](https://dwr.state.co.us/rest/get/help#Datasets&#GroundwaterGeophysicalLogsController&#gettingstarted&#jsonxml)                          |
|8     | **get_gw_gplogs_geologpicks()**          | Returns Groundwater Geophysical Log picks by well ID           | [groundwater/geophysicallogs/](https://dwr.state.co.us/rest/get/help#Datasets&#GroundwaterGeophysicalLogsController&#gettingstarted&#jsonxml)                          |
|9     | **get_gw_wl_wells()**          | Returns WaterLevelsWell from filters           | [groundwater/waterlevels/wells](https://dwr.state.co.us/rest/get/help#Datasets&#GroundwaterLevelsController&#gettingstarted&#jsonxml)                          |
|10     | **get_gw_wl_wellmeasures()**          | Returns  Groundwater Measurements | [groundwater/waterlevels/wellmeasurements](https://dwr.state.co.us/rest/get/help#Datasets&#GroundwaterLevelsController&#gettingstarted&#jsonxml)                          |
|11     | **get_reference_tbl()**        | Returns reference tables list                                      | [referencetables/](https://dwr.state.co.us/rest/get/help#Datasets&#ReferenceTablesController&#gettingstarted&#jsonxml)                      |
|12     | **get_sw_stations()**          | Returns Surface Water Station info                                 | [surfacewater/surfacewaterstations](https://dwr.state.co.us/rest/get/help#Datasets&#SurfaceWaterController&#gettingstarted&#jsonxml)     |
|13    | **get_sw_ts()**                | Returns Surface Water Time Series                                  | [surfacewater/surfacewaterts](https://dwr.state.co.us/rest/get/help#Datasets&#SurfaceWaterController&#gettingstarted&#jsonxml)        |
|14    | **get_telemetry_stations()**   | Returns telemetry stations and their most recent parameter reading | [telemetrystations/telemetrystation](https://dwr.state.co.us/rest/get/help#Datasets&#TelemetryStationsController&#gettingstarted&#jsonxml)    |
|15    | **get_telemetry_ts()**         | Returns telemetry time series data (raw, hour, day) | [telemetrystations/telemetrytimeseries](https://dwr.state.co.us/rest/get/help#Datasets&#TelemetryStationsController&#gettingstarted&#jsonxml)|
|16    | **get_water_rights_netamount()**   | Returns current status of a water right based on all of its court decreed actions | [waterrights/netamount](https://dwr.state.co.us/rest/get/help#Datasets&#WaterRightsController&#gettingstarted&#jsonxml)    |
|17    | **get_water_rights_trans()**         | Returns court decreed actions that affect amount and use(s) that can be used by each water right | [waterrights/transaction](https://dwr.state.co.us/rest/get/help#Datasets&#WaterRightsController&#gettingstarted&#jsonxml)|
|18    | **get_call_analysis_wdid()**   | 	Performs a call analysis that returns a time series showing the percentage of each day that the specified WDID and priority was out of priority and the downstream call in priority | [analysisservices/callanalysisbywdid](https://dwr.state.co.us/rest/get/help#Datasets&#AnalysisServicesController&#gettingstarted&#jsonxml)    |
|19    | **get_source_route_framework()**         | Returns the DWR source route framework reference table for the criteria specified | [analysisservices/watersourcerouteframework](https://dwr.state.co.us/rest/get/help#Datasets&#AnalysisServicesController&#gettingstarted&#jsonxml)|
                                                                                

<br>
<br>
<br>

## **Identify query inputs using reference tables**

The **`get_reference_tbl()`** function will return tables that makes it easier to know what information should be supplied to the data retrieval functions in **`cdsspy`**. For more information on the source reference tables click [here](https://dwr.state.co.us/rest/get/help#Datasets&#ReferenceTablesController&#gettingstarted&#jsonxml).

<br>

```python
# available parameters for telemetry stations
telemetry_params = cdsspy.get_reference_tbl(
    table_name = "telemetryparams"
    )
```

|    | Parameter   |
|---:|:------------|
|  0 | AIRTEMP     |
|  1 | BAR_P       |
|  2 | BATTERY     |
|  3 | COND        |
|  4 | D1          |
|  5 | D2          |
|  6 | DISCHRG     |
|  7 | DISCHRG1    |
|  8 | DISCHRG2    |
|  9 | DISCHRG3    |
| 10 | DISCHRG4    |
| 11 | ELEV        |

<br>
<br>
<br>

## **Locate structures**

**`cdsspy`** provides functions for locating structures/stations/wells/sites by providing a spatial extent, water district, division, county, designated basin, or management district to the functions in the table below. Site data can also be retrieved by providing the site specific abbreviations, GNIS IDs, USGS IDs, WDIDs, or Well IDs.
|**-** |**Function**                    | **Description**                                                    | **Endpoint**                                 |
|------|--------------------------------| -------------------------------------------------------------------|----------------------------------------------|
|1     | **get_structures()**           | Returns list of administrative structures                          | [structures](https://dwr.state.co.us/rest/get/help#Datasets&#StructuresController&#gettingstarted&#jsonxml)                            |          |
|2     | **get_climate_stations()**     | Returns Climate Stations                                           | [climatedata/climatestations](https://dwr.state.co.us/rest/get/help#Datasets&#ClimateStationsController&https://www.ncdc.noaa.gov/cdo-web/webservices&https://www.northernwater.org/our-data/weather-data&#gettingstarted&#jsonxml)           |
|3     | **get_gw_gplogs_wells()**          | Returns Groundwater GeophysicalLogsWell from filters           | [groundwater/geophysicallogs/](https://dwr.state.co.us/rest/get/help#Datasets&#GroundwaterGeophysicalLogsController&#gettingstarted&#jsonxml)                          |
|4     | **get_gw_wl_wells()**          | Returns WaterLevelsWell from filters           | [groundwater/waterlevels/wells](https://dwr.state.co.us/rest/get/help#Datasets&#GroundwaterLevelsController&#gettingstarted&#jsonxml)                          |
|5     | **get_sw_stations()**          | Returns Surface Water Station info                                 | [surfacewater/surfacewaterstations](https://dwr.state.co.us/rest/get/help#Datasets&#SurfaceWaterController&#gettingstarted&#jsonxml)     |
|6    | **get_telemetry_stations()**   | Returns telemetry stations and their most recent parameter reading | [telemetrystations/telemetrystation](https://dwr.state.co.us/rest/get/help#Datasets&#TelemetryStationsController&#gettingstarted&#jsonxml)    |                     

<br>

#### **Example: Locating telemetry stations by county:**

```python
# identify telemetry stations in Boulder County
stations  = cdsspy.get_telemetry_stations(
    county = "Boulder"
    )
```
<br>

|    |   division |   waterDistrict | county   | stationName                                                    | waterSource         |   streamMile | abbrev   | stationType         | structureType   | parameter   |   longitude |   latitude |
|---:|-----------:|----------------:|:---------|:---------------------------------------------------------------|:--------------------|-------------:|:---------|:--------------------|:----------------|:------------|------------:|-----------:|
|  0 |          1 |               5 | BOULDER  | CLOUGH PRIVATE DITCH                                           | SAINT VRAIN CREEK   |       nan    | 0500536A | Diversion Structure | Ditch           | DISCHRG     |    -105.217 |    40.1941 |
|  1 |          1 |               6 | BOULDER  | ANDERSON DITCH                                                 | BOULDER CREEK       |        23.58 | ANDDITCO | Diversion Structure | Ditch           | DISCHRG     |    -105.301 |    40.0132 |
|  2 |          1 |               6 | BOULDER  | ANDREWS FARWELL DITCH                                          | SOUTH BOULDER CREEK |         1.5  | ANFDITCO | Diversion Structure | Ditch           | DISCHRG     |    -105.169 |    40.0178 |
|  3 |          1 |               6 | BOULDER  | BASELINE RESERVOIR OUTLET                                      | BOULDER CREEK       |        19.18 | BASOUTCO | Stream Gage         | Reservoir       | DISCHRG     |    -105.198 |    39.9986 |
|  4 |          1 |               6 | BOULDER  | BOULDER CREEK SUPPLY CANAL TO BOULDER CREEK ABOVE ERIE TURNOUT | BOULDER CREEK       |        15.5  | BCSCBCCO | Diversion Structure | Other           | DISCHRG     |    -105.193 |    40.053  |
|  5 |          1 |               5 | BOULDER  | BOULDER RESERVOIR INLET                                        | LEFT HAND CREEK     |         8.66 | BFCINFCO | Stream Gage         | Reservoir       | DISCHRG     |    -105.218 |    40.0863 |

![](https://cdsspy-images.s3.us-west-1.amazonaws.com/county_telem_stations2.png)

<br>

#### **Example: Locating telemetry stations around a point**

```python
# identify telemetry stations within a 15 mile radius of the center point of Boulder County
stations  = cdsspy.get_telemetry_stations(
    aoi    = [-105.358164, 40.092608],
    radius = 15
    )
```

![](https://cdsspy-images.s3.us-west-1.amazonaws.com/poi_telem_stations.png)
<br>

#### **Example: Locating telemetry stations within a spatial extent**
If a polygon is given as the **aoi** input, a masking operation is done to ensure the returned points only include those that are *within* the given polygon. 

```python
# identify telemetry stations 15 miles around the centroid of a polygon
stations  = cdsspy.get_telemetry_stations(
    aoi    = geopandas.read_file("https://cdsspy-images.s3.us-west-1.amazonaws.com/boulder_county.gpkg")
    radius = 15
    )
```

The gif below highlights the masking process that is done when a polygon is used as the input for conducting a location search. 

![](https://cdsspy-images.s3.us-west-1.amazonaws.com/boulder_telem_stations_poly.gif)

<br>
<br>
<br>

## **Retrieve timeseries data** 

The functions in the table below retrieve timeseries data from the various timeseries related CDSS API endpoints.  

|**-** |**Function**                    | **Description**                                                    | **Endpoint**                                 |
|------|--------------------------------| -------------------------------------------------------------------|----------------------------------------------|
|1     | **get_structures_divrec()**    | Returns list of diversion/release/stage records based on WDID      | [structures/divrec/](https://dwr.state.co.us/rest/get/help#Datasets&#DiversionRecordsController&https://dnrweblink.state.co.us/dwr/ElectronicFile.aspx?docid=3600965&dbid=0&#gettingstarted&#jsonxml)              |
|2     | **get_climate_ts()**           | Returns Climate Station Time Series (day, month, year)             | [climatedata/climatestationts](https://dwr.state.co.us/rest/get/help#Datasets&#ClimateStationsController&https://www.ncdc.noaa.gov/cdo-web/webservices&https://www.northernwater.org/our-data/weather-data&#gettingstarted&#jsonxml)       |
|3     | **get_gw_wl_wellmeasures()**          | Returns  Groundwater Measurements | [groundwater/waterlevels/wellmeasurements](https://dwr.state.co.us/rest/get/help#Datasets&#GroundwaterLevelsController&#gettingstarted&#jsonxml)                          |
|4    | **get_sw_ts()**                | Returns Surface Water Time Series                                  | [surfacewater/surfacewaterts](https://dwr.state.co.us/rest/get/help#Datasets&#SurfaceWaterController&#gettingstarted&#jsonxml)        |
|5    | **get_telemetry_ts()**         | Returns telemetry time series data (raw, hour, day) | [telemetrystations/telemetrytimeseries](https://dwr.state.co.us/rest/get/help#Datasets&#TelemetryStationsController&#gettingstarted&#jsonxml)|

<br>

#### **Example: Daily discharge at a telemetry station**
We can then take a station abbreviations from the **`get_telemetry_stations()`** call, a parameter from the **`get_reference_tbl()`** call, and use this information as inputs into the **`get_telemetry_ts()`** function. 

<br>

The function below returns a dataframe of daily discharge for the "ANDDITCO" site between 2015-2022.

```python
# Daily discharge at "ANDDITCO" telemetry station
discharge_ts   = cdsspy.get_telemetry_ts(
    abbrev         = "ANDDITCO",     # Site abbreviation from the outputs of get_telemetry_stations()
    parameter      = "DISCHRG",      # Desired parameter, identified by the get_reference_tbl()
    start_date     = "2015-01-01",   # Starting date
    end_date       = "2022-01-01",   # Ending date
    timescale      = "day"           # select daily timescale
    )
```

![](https://cdsspy-images.s3.us-west-1.amazonaws.com/discharge_timeseries_plot2.png)

<br>

#### **Example: Retrieve Diversion records for multiple structures**
Some of the CDSS API endpoints allow users to request data from multiple structures. To do this, we can get a list of relevent WDIDs by:

1. Executing a spatial search
2. Selecting the WDID's of interest from our search results
3. Providing the WDID's as a vector to **`get_structures_divrec()`** 


```python 
# 1. Executing a spatial search
structures = cdsspy.get_structures(
    aoi    = [-105.3578, 40.09244],
    radius = 5
    )

# 2. Selecting the WDID's of interest from search results and putting WDIDs into a list
ditch_wdids = structures[(structures["ciuCode"] == "A") & (structures["structureType"] == "DITCH")]
ditch_wdids = list(ditch_wdids['wdid'])

# 3. Providing the WDID's as a list to **`get_structures_divrec()`** 
diversion_rec  = cdsspy.get_structures_divrec(
    wdid           = ditch_wdids,
    wc_identifier  = "diversion",
    type           = "month"
    )
```
**Note:** Data availability can vary between structures (i.e. Missing data, not all structures have every data type/temporal resolution available, etc.) 

![](https://cdsspy-images.s3.us-west-1.amazonaws.com/divrec_facet_plot.png)

<br>
<br>
<br>

## **Retrieve groundwater data**
The **`get_gw_()`** set of functions lets users retrieve data from the various groundwater related API endpoints shown in the table below:

| **-** | **Function**                    | **Endpoint**                                                                                                                                     |
|-------|---------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------|
| 1     | **get_gw_wl_wellmeasures()**    | [api/v2/groundwater/waterlevels/wellmeasurements](https://dwr.state.co.us/Rest/GET/Help/Api/GET-api-v2-groundwater-waterlevels-wellmeasurements) |
| 2     | **get_gw_wl_wells()**           | [api/v2/groundwater/waterlevels/wells](https://dwr.state.co.us/Rest/GET/Help/Api/GET-api-v2-groundwater-waterlevels-wells)                       |
| 3     | **get_gw_gplogs_wells()**       | [api/v2/groundwater/geophysicallogs/wells](https://dwr.state.co.us/Rest/GET/Help/Api/GET-api-v2-groundwater-geophysicallogs-wells)               |
| 4     | **get_gw_gplogs_geologpicks()** | [api/v2/groundwater/geophysicallogs/geoplogpicks](https://dwr.state.co.us/Rest/GET/Help/Api/GET-api-v2-groundwater-geophysicallogs-geoplogpicks) |

<br>

Here we will retrieve groundwater well measurement data for Well ID 1274 between 2021-2022.

```python
# Request wellmeasurements endpoint (api/v2/groundwater/waterlevels/wellmeasurements)
well_measure = cdsspy.get_gw_wl_wellmeasures(
    wellid     = "1274",
    start_date = "1990-01-01",
    end_date   = "2022-01-01"
    )
```
![](https://cdsspy-images.s3.us-west-1.amazonaws.com/gw_depth_to_water_plot.png)
