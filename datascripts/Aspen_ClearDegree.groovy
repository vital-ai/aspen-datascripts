package commons.scripts

import java.util.Map

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import ai.vital.vitalsigns.model.AggregationResult;
import ai.vital.domain.properties.Property_hasDegree;
import ai.vital.prime.groovy.v2.VitalPrimeGroovyScriptV2
import ai.vital.prime.groovy.v2.VitalPrimeScriptInterfaceV2
import ai.vital.query.querybuilder.VitalBuilder
import ai.vital.vitalservice.VitalStatus
import ai.vital.vitalservice.VitalStatus.Status;
import ai.vital.vitalservice.query.AggregationType;
import ai.vital.vitalservice.query.ResultElement;
import ai.vital.vitalservice.query.ResultList
import ai.vital.vitalservice.query.VitalSelectAggregationQuery;
import ai.vital.vitalservice.query.VitalSelectQuery;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.VITAL_Node
import ai.vital.vitalsigns.model.VitalSegment
import ai.vital.vitalsigns.ontology.VitalCoreOntology;
import static ai.vital.query.Utils.*

class Aspen_ClearDegree implements VitalPrimeGroovyScriptV2 {

	private final static Logger log = LoggerFactory.getLogger(Aspen_ClearDegree.class)

	static def builder = new VitalBuilder()
	
	@Override
	public ResultList executeScript(
	VitalPrimeScriptInterfaceV2 scriptInterface,
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
			
			for(VitalSegment segment : segments) {

				seg ++

				int limit = 1000
				int offset = 0

				
				
				VitalSelectQuery sq = builder.query {
					
					
					SELECT {
						
						value segments: [segment]
						value offset: offset
						value limit: limit

						node_constraint { PropertyConstraint(Property_hasDegree.class).exists() }
												
					}
					
					
				}.toQuery()
				

				while(offset >= 0) {

					sq.offset = offset

					long t = System.currentTimeMillis()
							
					ResultList res = scriptInterface.query(sq)

					Integer total = res.totalResults

					log.info("Query segment ${seg} of ${segments.size()} - ${offset} - ${offset + limit}, total: ${total}, ${System.currentTimeMillis() - t}ms")

					if(offset < total.intValue()) {
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

						if(oldValue != null) {

							//update node value
							n.degree = null

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
			}

			rl.totalResults = updated

			rl.setStatus(VitalStatus.withOKMessage((String) "Updated: ${updated}, unchanged: ${unchanged} ${max != null ? (' (max ' + max ) : ''}"))


		} catch(Exception e) {
			rl.status = VitalStatus.withError(e.localizedMessage)
		}

		return rl;
	}

}
