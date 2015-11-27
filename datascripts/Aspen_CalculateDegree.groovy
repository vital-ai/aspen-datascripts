package commons.scripts

import java.util.Map

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import ai.vital.vitalsigns.model.AggregationResult;
import ai.vital.prime.groovy.VitalPrimeGroovyScript
import ai.vital.prime.groovy.VitalPrimeScriptInterface
import ai.vital.vitalsigns.model.property.URIProperty
import ai.vital.query.querybuilder.VitalBuilder
import ai.vital.vitalservice.VitalStatus
import ai.vital.vitalservice.VitalStatus.Status;
import ai.vital.vitalservice.query.AggregationType;
import ai.vital.vitalservice.query.ResultElement;
import ai.vital.vitalservice.query.ResultList
import ai.vital.vitalservice.query.Connector;
import ai.vital.vitalservice.query.GraphElement;
import ai.vital.vitalservice.query.QueryContainerType;
import ai.vital.vitalservice.query.Source;
import ai.vital.vitalservice.query.Destination;
import ai.vital.vitalservice.query.VitalGraphArcContainer;
import ai.vital.vitalservice.query.VitalGraphArcElement
import ai.vital.vitalservice.query.VitalGraphCriteriaContainer;
import ai.vital.vitalservice.query.VitalGraphQueryPropertyCriterion
import ai.vital.vitalservice.query.VitalGraphQueryPropertyCriterion.Comparator
import ai.vital.vitalservice.query.VitalSelectAggregationQuery;
import ai.vital.vitalservice.query.VitalSelectQuery;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.VITAL_Edge_PropertiesHelper;
import ai.vital.vitalsigns.model.VITAL_Node
import ai.vital.vitalsigns.model.VITAL_Node_PropertiesHelper;
import ai.vital.vitalsigns.model.VitalSegment
import ai.vital.vitalsigns.ontology.VitalCoreOntology;

class Aspen_CalculateDegree implements VitalPrimeGroovyScript {

	private final static Logger log = LoggerFactory.getLogger(Aspen_CalculateDegree.class)

	static def builder = new VitalBuilder()
	
	@Override
	public ResultList executeScript(
	VitalPrimeScriptInterface scriptInterface,
	Map<String, Object> parameters) {

		ResultList rl = new ResultList();

		try {

			String segmentsP = parameters.get("segments")

			//optionally limit the processing 
			Integer max = parameters.get("max");
			
			if(!segmentsP) throw new Exception("No 'segments' parameter")

			List<VitalSegment> segments = []
			
			Set<String> currentSegments = new HashSet<String>();
			
			for(VitalSegment s : scriptInterface.listSegments()) {
				currentSegments.add(s.segmentID.toString())
			}

			for(String s : segmentsP.split("\\s+")) {
				if(s) {
					if(!currentSegments.contains(s)) {
						throw new Exception("Segment not found: ${s}")
					}
					segments.add(s)
				}
			}

			if(segments.size() < 1) throw new Exception("Empty segments list")


			int updated = 0
			int unchanged = 0
			
			int seg = 0
			
			int processed = 0
			
			VITAL_Edge_PropertiesHelper edgeHelper = new VITAL_Edge_PropertiesHelper()
			
			for(VitalSegment segment : segments) {

				seg ++

				int limit = 1000
				int offset = 0

				
				VitalSelectQuery sq = builder.query {
					
					
					SELECT {
						
						value segments: [segment]
						value offset: offset
						value limit: limit

						node_constraint { new VITAL_Node_PropertiesHelper().active.notEqualTo(false) }
												
					}
					
					
				}.toQuery()
				
				while(offset >= 0) {

					sq.offset = offset

					long t = System.currentTimeMillis()
							
					ResultList res = scriptInterface.query(sq)


					Integer total = res.totalResults

					log.info("Query segment ${seg} of ${segments.size()} - ${offset} - ${offset + limit}, total: ${total}, ${System.currentTimeMillis() - t}ms")

					if((offset + limit) < total.intValue()) {
						offset += limit
					} else {
						offset = -1
					}

					List<VITAL_Node> toUpdate = []

					for(ResultElement re : res.results) {

						GraphObject g = re.graphObject;

						if(! ( g instanceof VITAL_Node )) continue

						VITAL_Node n = (VITAL_Node)g;

						if(max != null) {
							if(processed >= max.intValue()) {
								offset = -1;
								break;
							}
						}
						
						processed ++
						
						//count edges - aggregation function
						Integer oldValue = n.degree?.rawValue()

						//count all edges
						
						VitalSelectAggregationQuery edgesQuery = builder.query {
							
							SELECT {
								
								COUNT edgeHelper.edgeSource
								
								value offset: 0
								
								value limit: 100000
								
								value segments: segments
								
								OR {
									
									edge_constraint { edgeHelper.edgeSource.equalTo(URIProperty.withString(n.URI)) }
									
									edge_constraint { edgeHelper.edgeDestination.equalTo(URIProperty.withString(n.URI)) }
										
								}
								
								
								
							}
							
						}.toQuery()
						
						ResultList erl = scriptInterface.query(edgesQuery);

						if(erl.status.status != Status.ok) continue

						AggregationResult ar = erl.results[0].graphObject
						int newValue = ar.value.intValue()
						
//						int newValue = erl.totalResults.intValue()
						

						if(oldValue == null || oldValue.intValue() != newValue) {

							//update node value
							n.degree = newValue

							toUpdate.add(n)

						} else {

							unchanged++

						}

					}

					if(toUpdate.size() > 0) {
						scriptInterface.save(segment, toUpdate)
						updated += toUpdate.size()
					}

				}
				
				if(max != null) {
					if(processed >= max.intValue()) {
						break;
					}
				}
				
			}

			rl.totalResults = updated

			rl.setStatus(VitalStatus.withOKMessage((String) "Updated: ${updated}, unchanged: ${unchanged} ${max != null ? (' (max ' + max ) : ''}"))


		} catch(Exception e) {
			rl.status = VitalStatus.withError(e.localizedMessage)
		}

		return rl;
	}

}
