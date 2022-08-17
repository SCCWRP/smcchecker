SELECT
	Sample_Entry.StationCode AS stationcode,
	Sample_Entry.SampleDate AS sampledate,
	Sample_Entry.AgencyCode AS sampleagencycode,
	Sample_Entry.EventCode AS eventcode,
	Sample_Entry.ProtocolCode AS protocolcode,
	Sample_Entry.ProjectCode AS projectcode,
	ParentProjectLookUp.ParentProjectCode AS parentprojectcode,
	Location_Entry.LocationCode AS locationcode,
	FieldCollection_Entry.CollectionTime AS collectiontime,
	FieldCollection_Entry.CollectionMethodCode AS collectionmethodcode,
	FieldCollection_Entry.CollectionDepth AS collectiondepth,
	FieldCollection_Entry.UnitCollectionDepth AS unitcollectiondepth,
	FieldCollection_Entry.Replicate AS replicate,
	AnalyteLookUp.AnalyteName AS analytename,
	FractionLookUp.FractionName AS fractionname,
	MethodLookUp.MethodName AS methodname,
	'' AS variableresult,
	FieldResult_Entry.RESULT AS RESULT,
	FieldResult_Entry.ResQualCode AS resqualcode,
	FieldResult_Entry.QACode AS qacode,
	FieldResult_Entry.ComplianceCode AS compliancecode,
	FieldResult_Entry.BatchVerificationCode AS batchverificationcode,
	FieldResult_Entry.FieldResultComments AS resultcomments,
	FieldCollection_Entry.FieldCollectionComments AS collectioncomments
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
							INNER JOIN FieldCollection_Entry ON Location_Entry.LocationRowID = FieldCollection_Entry.LocationRowID 
						)
						INNER JOIN FieldResult_Entry ON FieldCollection_Entry.FieldCollectionRowID = FieldResult_Entry.FieldCollectionRowID 
					)
					INNER JOIN ConstituentLookUp ON FieldResult_Entry.ConstituentRowID = ConstituentLookUp.ConstituentRowID 
				) ON AnalyteLookUp.AnalyteCode = ConstituentLookUp.AnalyteCode 
			)
			INNER JOIN FractionLookUp ON ConstituentLookUp.FractionCode = FractionLookUp.FractionCode 
		) ON MethodLookUp.MethodCode = ConstituentLookUp.MethodCode 
	)
	INNER JOIN FractionLookUp AS FractionLookUp_1 ON ConstituentLookUp.FractionCode = FractionLookUp_1.FractionCode;