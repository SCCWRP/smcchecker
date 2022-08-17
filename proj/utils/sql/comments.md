# Explanation of why we have the SQL Files here
There are the SQL Files here because it tells us how to query the access database that container PHAB data. There are 3 queries - Field, Habitat, and Geometry

The Field query should return close to 16 records, and the habitat query should return close to 1800. The geometry query just matches the stationcode with its recorded lat longs from the Geometry_Entry table in the access database

The results of the Field and Habitat queries get stacked on top of each other, and then the geometry query result gets 'left joined' on stationcode, since it is possible that the lat longs werent recorded

