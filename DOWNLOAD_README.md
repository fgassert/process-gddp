# PREP Downscaled Climate Indicators
Summary climate indicators derived from the downscaled CMIP5 datasets [NEX-GDDP](https://www.nccs.nasa.gov/services/data-collections/land-based-products/nex-gddp) (global) and [LOCA](https://gdo-dcp.ucllnl.org/downscaled_cmip_projections/) (continental US).

**Citation**: Gassert, Francis, Enrique Cornejo, and Emily Nilson. 2021. "Making Climate Data Accessible: Methods for Producing NEX-GDDP and LOCA Downscaled Climate Indicators." Technical Note. World Resources Institute. Washington DC. Available at https://www.wri.org/research/making-climate-data-accessible 

## Background

This repository contains ensemble indicators derived from the downscaled CMIP5 datasets: NASA Earth Exchange Global Daily Downscaled Projections (NEX-GDDP) (Thrasher and Nemani 2015) for the globe and Localized Constructed Analogs (LOCA) CMIP5 projections for the continental United States (Pierce, Cayan, and Thrasher 2014; Maurer et al. 2007). NEX-GDDP and LOCA contain daily minimum and maximum temperature and precipitation values from several dozen models for two climate scenarios over the period of 1950-2100. 

The data are derived following four steps:

 1. For each model, scenario, and year: compute annual indicators
 2. For each indicator, model, and scenario: compute moving averages (typically 30yrs)
 3. For each indicator, model, and scenario: compute difference and/or change layers
 4. For each indicator and scenario: compute statistics across the ensemble of models (e.g. median)

### Filenames

Filenames are composed of the following parts, joined by underscores:
```
                        {formula}_{variable}_{scenario}_{model}_{years}_{dataset}.tif
# or expanding {formula} into subcomponents:
{ensemble}-{timefunc}-{indicator}_{variable}_{scenario}_{model}_{years}_{dataset}.tif
```

e.g. `q50-abs-annual_pr_rcp85_ens_2035-2065_nexgddp.tif` - Median absolute average annual (`q50-abs-annual`) precipitation (`pr`) for the high emissions scenario (`rcp85`) across the ensemble (`ens`) for the 31yr period centered on 2050 (`2035-2065`) derived from NEX-GDDP (`nexgddp`).

**Formula**

The formula defines what computations to run, the remainder of the keyname defines what data to run the computationts against. The `{formula}` can be decomposed further as follows:

```
# {formula} can be decompose into:
                      {indicator}_...
           {timefunc}-{indicator}_...
# or
{ensemble}-{timefunc}-{indicator}_...
```

Each of these components is a reducing function that depends on the previous. For example, computing ensemble median average annual (`q50-abs-annual`) precipitation from individual models' average annual (`abs-annual`) precipitation:

key | is derived from
--- | ---
`q50-abs-annual_..._ens_...` | `abs-annual_..._ACCESS1-0_...` `abs-annual_..._BNU-ESM_...` `abs-annual_..._CCSM4_...` `abs-annual_..._CESM1-BGC_...` `abs-annual_..._CNRM-CM5_...` ...

Or computing the 31yr average (`abs-annual`) precipitation from annual (`annual`) precipitation:

key | is derived from
--- | ---
`abs-annual_..._2035-2065_...` | `annual_..._2035_...` `annual_..._2036_...` `annual_..._2037_...` ... `annual_..._2065_...`

#### Valid values for keynames

 - **ensemble**
 
 value | description
 --- | ---
 `q25` | 25th percentile
 `q50` | median
 `q75` | 75th percentile
 `iqr` | interquartile range
 `mean` | mean

 - **timefunc**
 
 value | description
 --- | ---
 `abs` | absolute i.e. the average value over the range of years
 `diff` | linear change from baseline (1960-1990) (i.e. `abs_..._{startyear}-{endyear}_...` minus `abs_..._1960-1990_...`)
 `ch` | multiplicative change from baseline (1960-1990) (i.e. `abs_..._{startyear}-{endyear}_...` divided by `abs_..._1960-1990_...`)

 - **indicator**
 
 value | description
 --- | ---
 `annual` | annual average
 `q98` | 98th percentile for the year
 `q99` | 99th percentile for the year
 `gt-q98` | number of days/yr exceeding the 98th percentile for the baseline period (1960-1990)
 `gt-q99` | number of days/yr exceeding the 99th percentile for the baseline period (1960-1990)
 `gt50mm` | number of days/yr exceeding 0.0005787037037 (kg/m2/s) (50mm/day)
 `gt95f` | number of days/yr exceeding 308.15 (Kelvin)
 `gt90f` | number of days/yr exceeding 305.37 (Kelvin)
 `gt85f` | number of days/yr exceeding 302.59 (Kelvin)
 `gt32f` | number of days/yr exceeding 273.15 (Kelvin)
 `frostfree` | length of longest run of days in a year exceeding 273.15 (Kelvin)
 `drydays` | length of longest run of days in a year less than 0.000011574 (kg/m2/s) (1mm/day)
 `dryspells` | number of runs of at least 5 days in a year with less than 0.000011574 (kg/m2/s) (1mm/day). Each day over 5 days counts as 0.2 of a run.
 `tavg-tasmin` | Use with `tasmax`. Daily average temperature. (Specifically, daily value plus `tasmin` divided by 2.)
 `hdd65f-tasmin` | Use with `tasmax`. Heating degree days. The sum of `291.48 - value` (Kelvin) (65ºF), where positive, for each day in a year.
 `cdd65f-tasmin` | Use with `tasmax`. Cooling degree days. The sum of `value - 291.48` (Kelvin) (65ºF), where positive, for each day in a year.

 - **variables**

 value | description
 --- | ---
 `pr` | precipitation (kg/ms/s)
 `tasmin` | daily minimum temperature
 `tasmax` | daily maximum temperature

 - **scenario**
 
 value | description
 --- | ---
 `rcp45` | Low emissions scenario
 `rcp85` | High emissions scenario
 `historical` | Retrospective data (1990-2005 only)

 - **model**
 
 value | description
 --- | ---
 `ACCESS1-0` `BNU-ESM` `CCSM4` `CESM1-BGC` `CNRM-CM5` `CSIRO-Mk3-6-0` `CanESM2` `GFDL-CM3` `GFDL-ESM2G` `GFDL-ESM2M` `IPSL-CM5A-LR` `IPSL-CM5A-MR` `MIROC-ESM-CHEM` `MIROC-ESM` `MIROC5` `MPI-ESM-LR` `MPI-ESM-MR` `MRI-CGCM3` `NorESM1-M` `bcc-csm1-1` `inmcm4` | Models included in NEX-GDDP
 `ACCESS1-0` `ACCESS1-3` `CCSM4` `CESM1-BGC` `CESM1-CAM5` `CMCC-CM` `CMCC-CMS` `CNRM-CM5` `CSIRO-Mk3-6-0` `CanESM2` `EC-EARTH` `FGOALS-g2` `GFDL-CM3` `GFDL-ESM2G` `GFDL-ESM2M` `GISS-E2-H` `GISS-E2-R` `HadGEM2-AO` `HadGEM2-CC` `HadGEM2-ES` `IPSL-CM5A-LR` `IPSL-CM5A-MR` `MIROC-ESM` `MIROC-ESM-CHEM` `MIROC5` `MPI-ESM-LR` `MPI-ESM-MR` `MRI-CGCM3` `NorESM1-M` `bcc-csm1-1` `bcc-csm1-1-m` `inmcm4` | Models included in LOCA
 `ens` | For ensemble functions

 - **years**
 
 value | description
 --- | ---
 A single `{year}` between `1950` and `2100` | Year of data (functions without a `{timefunc}`)
 A range of years `{startyear}-{endyear}` | For multi-year averages (functions with a `{timefunc}`)


 - **dataset**

 value | description
 --- | ---
 `nexgddp` | Derive from NEX GDDP
 `loca` | Derive from LOCA

## References

Thrasher, Bridget, and Rama Nemani. 2015. “NASA Earth Exchange Global Daily Downscaled Projections (NEX-GDDP).” https://nex.nasa.gov/nex/projects/1356/.

Pierce, David W., Daniel R. Cayan, and Bridget L. Thrasher. 2014. “Statistical Downscaling Using Localized Constructed Analogs (LOCA).” Journal of Hydrometeorology 15 (6): 2558–2585. doi:10.1175/JHM-D-14-0082.1.

Maurer, Edwin P., Levi Brekke, Tom Pruitt, and Philip B. Duffy. 2007. “Fine-Resolution Climate Projections Enhance Regional Climate Change Impact Studies.” Eos, Transactions American Geophysical Union 88 (47). John Wiley & Sons, Ltd: 504–504. doi:10.1029/2007EO470006.

We acknowledge the World Climate Research Programme's Working Group on Coupled Modelling, which is responsible for CMIP, and we thank the climate modeling groups for producing and making available their model output. For CMIP the U.S. Department of Energy's Program for Climate Model Diagnosis and Intercomparison provides coordinating support and led development of software infrastructure in partnership with the Global Organization for Earth System Science Portals.
