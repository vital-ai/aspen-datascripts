package commons.scripts

import java.util.Map

import ai.vital.aspen.groovy.modelmanager.AspenModel;
import ai.vital.aspen.groovy.modelmanager.ModelManager;
import ai.vital.prime.groovy.VitalPrimeGroovyScript;
import ai.vital.prime.groovy.VitalPrimeScriptInterface;
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.query.ResultElement
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalsigns.model.GraphObject;

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
			if(inputBlock == null || inputBlock.size() == 0) throw new Exception("Null or empty 'inputBlock' param - graph objects list")
			
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
			
			for(GraphObject g : output) {
				rl.results.add(new ResultElement(g, 1D))
			}
			
			
		} catch(Exception e) {
			rl.setStatus(VitalStatus.withError(e.localizedMessage))
		}
		
		return rl
			
	}

}
