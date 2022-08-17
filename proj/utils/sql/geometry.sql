SELECT
	Sample_Entry.StationCode AS stationcode,
	Geometry_Entry.Latitude AS actual_latitude,
	Geometry_Entry.Longitude AS actual_longitude
FROM
	Sample_Entry
	INNER JOIN ( Geometry_Entry INNER JOIN Location_Entry ON Geometry_Entry.LocationRowID = Location_Entry.LocationRowID ) ON Sample_Entry.SampleRowID = Location_Entry.SampleRowID;