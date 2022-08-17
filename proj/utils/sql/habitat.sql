SELECT
	Sample_Entry.StationCode AS stationcode,
	Sample_Entry.SampleDate AS sampledate,
	Sample_Entry.AgencyCode AS sampleagencycode,
	Sample_Entry.EventCode AS eventcode,
	Sample_Entry.ProtocolCode AS protocolcode,
	Sample_Entry.ProjectCode AS projectcode,
	ParentProjectLookUp.ParentProjectCode AS parentprojectcode,
	Location_Entry.LocationCode AS locationcode,
	HabitatCollection_Entry.CollectionTime AS collectiontime,
	HabitatCollection_Entry.CollectionMethodCode AS collectionmethodcode,
	- 88 AS collectiondepth,
	'' AS unitcollectiondepth,
	HabitatCollection_Entry.Replicate AS replicate,
	AnalyteLookUp.AnalyteName AS analytename,
	FractionLookUp.FractionName AS fractionname,
	MethodLookUp.MethodName AS methodname,
	HabitatResult_Entry.VariableResult AS variableresult,
	HabitatResult_Entry.RESULT AS RESULT,
	HabitatResult_Entry.ResQualCode AS resqualcode,
	HabitatResult_Entry.QACode AS qacode,
	HabitatResult_Entry.ComplianceCode AS compliancecode,
	HabitatResult_Entry.BatchVerificationCode AS batchverificationcode,
	HabitatResult_Entry.HabitatResultComments AS resultcomments,
	HabitatCollection_Entry.HabitatCollectionComments AS collectioncomments
FROM
	(
		MethodLookUp
		INNER JOIN (
			(
				AnalyteLookUp
				INNER JOIN (
					(
						(
							(
								( ( Sample_Entry INNER JOIN Location_Entry ON Sample_Entry.SampleRowID = Location_Entry.SampleRowID ) INNER JOIN ProjectLookUp ON Sample_Entry.ProjectCode = ProjectLookUp.ProjectCode )
								INNER JOIN ParentProjectLookUp ON ProjectLookUp.ParentProjectCode = ParentProjectLookUp.ParentProjectCode 
							)
							INNER JOIN HabitatCollection_Entry ON Location_Entry.LocationRowID = HabitatCollection_Entry.LocationRowID 
						)
						INNER JOIN HabitatResult_Entry ON HabitatCollection_Entry.HabitatCollectionRowID = HabitatResult_Entry.HabitatCollectionRowID 
					)
					INNER JOIN ConstituentLookUp ON HabitatResult_Entry.ConstituentRowID = ConstituentLookUp.ConstituentRowID 
				) ON AnalyteLookUp.AnalyteCode = ConstituentLookUp.AnalyteCode 
			)
			INNER JOIN FractionLookUp ON ConstituentLookUp.FractionCode = FractionLookUp.FractionCode 
		) ON MethodLookUp.MethodCode = ConstituentLookUp.MethodCode 
	)
	INNER JOIN FractionLookUp AS FractionLookUp_1 ON ConstituentLookUp.FractionCode = FractionLookUp_1.FractionCode;