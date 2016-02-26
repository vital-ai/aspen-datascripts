package commons.scripts

import java.nio.charset.StandardCharsets;
import java.util.Map

import org.apache.commons.io.IOUtils;

import ai.vital.aspen.groovy.modelmanager.AspenModel;
import ai.vital.aspen.groovy.modelmanager.ModelManager;
import ai.vital.prime.groovy.VitalPrimeGroovyScript;
import ai.vital.prime.groovy.VitalPrimeScriptInterface;
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.query.ResultElement
import ai.vital.vitalservice.query.ResultList
import ai.vital.vitalsigns.block.BlockCompactStringSerializer
import ai.vital.vitalsigns.block.CompactStringSerializer;
import ai.vital.vitalsigns.model.GraphObject
import ai.vital.vitalsigns.model.VITAL_GraphContainerObject;;

class Aspen_Predict implements VitalPrimeGroovyScript {

	public final static String modelManagerKey = 'aspen-model-manager'
	
	@Override
	public ResultList executeScript(
			VitalPrimeScriptInterface scriptInterface,
			Map<String, Object> parameters) {

		ResultList rl = new ResultList()
		
		try {
			
			String modelName = parameters.get('modelName');
			String modelURI = parameters.get('modelURI');
			
			
			if(( !modelName && !modelURI) || (modelName && modelURI) ) throw new Exception("Exactly one of 'modelName' or 'modelURI' param expected")
			
			List<GraphObject> inputBlock = parameters.get('inputBlock')
			String inputCompactBlock = parameters.get('inputCompactBlock');
			
			
			if(inputCompactBlock != null && inputBlock != null) {
				throw new Exception("inputBlock and inputCompactBlock params are mutually exclusive")
			} 
			
			if(!inputCompactBlock == null && inputBlock == null) throw new Exception("inputBlock or inputCompactBlock required")
			
			if(inputCompactBlock) {
			
				inputBlock = []
				
				List<String> lines = IOUtils.readLines(new ByteArrayInputStream(inputCompactBlock.getBytes(StandardCharsets.UTF_8)), 'UTF-8')
				
				int c = 0
				
				for(String l : lines) {
					
					c++
					
					l = l.trim();
					if(l.isEmpty()  || l.startsWith('#') || l.startsWith(BlockCompactStringSerializer.BLOCK_SEPARATOR) || l.startsWith(BlockCompactStringSerializer.DOMAIN_HEADER_PREFIX) ) {
						continue
					}
					
					GraphObject g = CompactStringSerializer.fromString(l)
					
					if(g == null) {
					
						throw new Exception("No graph object deserialized from line ${c}: ${l}")
							
					}
					
					inputBlock.add(g)
					
				}
				
				if(inputBlock.size() == 0) {
					throw new Exception("No graph objects found in input block string")
				}
					
			} else {
			
				if(inputBlock == null || inputBlock.size() == 0) throw new Exception("Null or empty 'inputBlock' param - graph objects list")
				
			}
			
			
			ModelManager manager = scriptInterface.getRegistry().get(modelManagerKey)
			
			if(manager == null) throw new Exception("Model manager not initialized, call any Aspen_ModelManager script action to initialize")
				
			AspenModel model = null;					
			
			for(AspenModel m : manager.getLoadedModels() ) {

				if(modelName && m.getName().equals(modelName)) {
					model = m;
				} else if(m.getURI().equals(modelURI)) {
					model = m;
				}
								
			}
			
			if(model == null) throw new Exception("Model not found ${modelName ? ('name: ' + modelName) : ('URI: ' + modelURI) }")
			
			List<GraphObject> output = model.predict(inputBlock)
			
			int i = 0
			
			for(GraphObject g : output) {
				
				
				if(inputCompactBlock != null) {
					
					i++
					VITAL_GraphContainerObject obj = new VITAL_GraphContainerObject();
					obj.URI = "urn:serialized:${i}"
					obj.object = g.toCompactString()
					
					rl.results.add(new ResultElement(obj, 1D))
					
				} else {
				
					rl.results.add(new ResultElement(g, 1D))
					
				}
				
			}
			
			
		} catch(Exception e) {
			rl.setStatus(VitalStatus.withError(e.localizedMessage))
		}
		
		return rl
			
	}

}
