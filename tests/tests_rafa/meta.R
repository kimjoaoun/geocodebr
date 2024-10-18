library(arrow)
# install_arrow()
library(sf)
library(dplyr)
library(geobr)



# install extension

con <- duckdb::dbConnect(duckdb::duckdb(), dbdir=":memory:")  # run on memory
DBI::dbExecute(con, "FORCE INSTALL httpfs")
dbExecute(con, "LOAD httpfs")


DBI::dbGetQuery(con, "FROM duckdb_extensions()")




muni <- geobr::lookup_muni(name_muni = 'Guarulhos')
muni <- geobr::read_municipality(code_muni = muni$code_muni)

bb <- sf::st_bbox(muni)

# https://cran.r-project.org/web/packages/overturemapsr/index.html

# https://walker-data.com/posts/overture-buildings/
# https://data.humdata.org/organization/meta

buildings <- open_dataset('s3://overturemaps-us-west-2/release/2024-05-16-beta.0/theme=buildings?region=us-west-2')

nrow(buildings)


df <- arrow::open_dataset('s3://overturemaps-us-west-2/release/2024-09-18.0')
names(df)


library(overturemapsr)

t <- overturemapsr::get_all_overture_types()

d <- overturemapsr::dataset_path(overture_type = 'locality')

df <- overturemapsr::record_batch_reader(overture_type = 'locality', bbox = bb)




library(duckdb)
library(DBI)




q <- "COPY(
  SELECT
    *
  FROM read_parquet('s3://overturemaps-us-west-2/release/2024-08-20.0/theme=places/type=place/*', filename=true, hive_partitioning=1)
  LIMIT 100000
) TO 'places.parquet';"


DBI::dbExecute(con, statement = q)

