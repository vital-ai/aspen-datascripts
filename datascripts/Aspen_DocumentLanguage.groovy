package commons.scripts

import java.util.Map

import ai.vital.aspen.groovy.modelmanager.AspenModel;
import ai.vital.aspen.groovy.modelmanager.ModelManager;
import ai.vital.aspen.groovy.nlp.steps.LanguageDetectorStep;
import ai.vital.domain.Document
import ai.vital.prime.groovy.v2.VitalPrimeGroovyScriptV2;
import ai.vital.prime.groovy.v2.VitalPrimeScriptInterfaceV2;
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.query.ResultElement
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalsigns.model.GraphObject;

class Aspen_DocumentLanguage implements VitalPrimeGroovyScriptV2 {

	static LanguageDetectorStep languageDetectorStep
	
	static LanguageDetectorStep getDetector() {
		if(languageDetectorStep == null) {
			synchronized(Aspen_DocumentLanguage.class) {
				if(languageDetectorStep == null) {
					languageDetectorStep = new LanguageDetectorStep()
					languageDetectorStep.init()
				}
			}
		}
		
		return languageDetectorStep
	}
	
	@Override
	public ResultList executeScript(
			VitalPrimeScriptInterfaceV2 scriptInterface,
			Map<String, Object> parameters) {

		ResultList rl = new ResultList()
		
		try {
			
			
			List<GraphObject> inputBlock = parameters.get('inputBlock')
			if(inputBlock == null || inputBlock.size() == 0) throw new Exception("Null or empty 'inputBlock' param - graph objects list")
			
			List<GraphObject> outputBlock = []
			
			for(GraphObject g : inputBlock) {
				
				if(g instanceof Document) {
					List<GraphObject> output = getDetector().processDocument((Document)g)
					outputBlock.addAll(output)
				}
				
			}
			
			for(GraphObject g : outputBlock) {
				rl.getResults().add(new ResultElement(g, 1D))
			}
			
			
		} catch(Exception e) {
			rl.setStatus(VitalStatus.withError(e.localizedMessage))
		}
		
		return rl
			
	}

}
