package commons.scripts

import java.util.Map.Entry

import ai.vital.domain.properties.Property_hasPageRank
import ai.vital.prime.groovy.VitalPrimeGroovyScript
import ai.vital.prime.groovy.VitalPrimeScriptInterface
import ai.vital.query.querybuilder.VitalBuilder
import ai.vital.vitalservice.VitalService
import ai.vital.vitalservice.VitalStatus
import ai.vital.vitalservice.config.VitalServiceConfig
import ai.vital.vitalservice.factory.VitalServiceFactory
import ai.vital.vitalservice.query.ResultElement
import ai.vital.vitalservice.query.ResultList
import ai.vital.vitalservice.query.VitalGraphQuery
import ai.vital.vitalservice.query.VitalSelectQuery
import ai.vital.vitalservice.query.VitalSortProperty
import ai.vital.vitalsigns.block.CompactStringSerializer
import ai.vital.vitalsigns.model.GraphMatch
import ai.vital.vitalsigns.model.GraphObject
import ai.vital.vitalsigns.model.VitalSegment
import ai.vital.vitalsigns.model.VitalServiceKey
import ai.vital.vitalsigns.model.property.IProperty
import ai.vital.vitalsigns.model.property.StringProperty


/**
 * a datascript that queries local or external service to get top page-ranked nodes and expand them
 *
 */
class Aspen_GetTopPageRankNodes implements VitalPrimeGroovyScript {

	@Override
	public ResultList executeScript(VitalPrimeScriptInterface scriptInterface, Map<String, Object> parameters) {

		String profCfg = parameters.get('profileConfig')
		
		String segmentID = parameters.get('segmentID')
		
		String topNParam = parameters.get("topN")
		
		Integer topN = 10
		
		ResultList rl = new ResultList()
		
		VitalService service = null;
		
		boolean closeService = false;
		
		try {
			
			if(!profCfg) throw new Exception("No profile-config param")
			
			if(!segmentID) throw new Exception("No segmentID param")
			
			if(topNParam) {
				topN = Integer.parseInt(topNParam)
				
				if(topN < 1) throw new Exception("topN param must be > 0")
 				
			}
			
			VitalSegment segment = null
			
			if(profCfg.equals("this")) {
				
				//run it over this instance

				for(VitalSegment s : scriptInterface.listSegments() ) {
					
					if(s.segmentID.toString().equals(segmentID)) {
						segment = s
						break 
					}
					
				}
				
			} else if(profCfg.startsWith("name:")) {
			
				String name = profCfg.substring("name:".length());			
			
				service = scriptInterface.getVitalService(name)
				
				if(service == null) throw new Exception("Service with name not found: " + service)
				
				segment = service.getSegment(segmentID)
				
			} else {
			
			
				VitalServiceKey key = new VitalServiceKey()
				key.key = "serv-serv-serv"
						
				VitalServiceConfig cfg = VitalServiceFactory.parseConfigString(profCfg)
						
				service = VitalServiceFactory.openService(key, profCfg)
			
				segment = service.getSegment(segmentID)
				
				closeService = true
				
			}
			
			if(segment == null) throw new Exception("Segment not found: " + segmentID)
			
			VitalSelectQuery sq = new VitalBuilder().query {
				
				SELECT {
					
					value segments: [segment]
					
					value offset: 0
					
					value limit: topN
					
					value sortProperties: [ VitalSortProperty.get(Property_hasPageRank.class, true) ]
					
					node_constraint { ai.vital.query.Utils.PropertyConstraint(Property_hasPageRank.class).exists() }
					
				}
				
			}.toQuery()

			
			if( service == null) {
				
				rl = scriptInterface.query(sq)
				
			} else {
			
				rl = service.query(sq)
				
			}			
			
			
			if(rl.status.status != VitalStatus.Status.ok || rl.results.size() == 0) {
				return rl
			}
			
			Set<String> uris = new HashSet<String>()
			
			//expand each node bi-directional
			for(GraphObject g : rl) {
				
				uris.add(g.URI)
				
			}
			
			
			for(String uri : new ArrayList<String>(uris)) {
			
				VitalGraphQuery gq = new VitalBuilder().query {
					
					GRAPH {
						
						value segments: [segment]
						
						value limit: 100
						
						value offset: 0
						
						value inlineObjects: true
						
						ARC {
							
							node_constraint { "URI eq $uri" }
						
							ARC_OR {
								
								ARC {
									
									value direction: 'forward'
									
	//								node_constraint { VITAL_Node.props().URIProp.exists() }
									
								}
								
								ARC {
									
									value direction: 'reverse'
									
	//								node_constraint { VITAL_Node.props().URIProp.exists() }
									
								}
								
							}
								
						}
						
					}
					
					
				}.toQuery()
				
				ResultList gRL = null
				
				if(service == null) {
					
					gRL = scriptInterface.query(gq)
					
				} else {
				
					gRL = service.query(gq)
				
				}
				
				if(gRL.status.status != VitalStatus.Status.ok) {
					return gRL
				}
					
				gRL = unpackGraphMatch(gRL)
				
				for(GraphObject g : gRL) {
					
					if(uris.add(g.URI)) {
						
						rl.results.add(new ResultElement(g, 2D))
						
					}
					
				}
				
			}
			
		} catch(Exception e) {
		
			rl.status = VitalStatus.withError(e.localizedMessage)
			
		} finally {
		
			if(closeService) {
				try { service.close() } catch(Exception e) {}
			}
		
		}
		
		
		return rl;
	}


	protected ResultList unpackGraphMatch(ResultList rl) throws Exception {
		
		ResultList r = new ResultList();
		
		for(GraphObject g : rl) {
			
			if(g instanceof GraphMatch) {
				
				for(Entry<String, IProperty> p : g.getPropertiesMap().entrySet()) {
					
					
					IProperty unwrapped = p.getValue().unwrapped();
					if(unwrapped instanceof StringProperty) {
						GraphObject x = CompactStringSerializer.fromString((String) unwrapped.rawValue());
						if(x != null) r.getResults().add(new ResultElement(x, 1D));
					}
					
				}
				
			} else {
				throw new Exception("Expected graph match objects only");
			}
			
		}
		
		return r;
	}
	
}
