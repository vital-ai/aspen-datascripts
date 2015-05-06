package commons.scripts

import java.util.Map

import ai.vital.aspen.groovy.AspenGroovyConfig;
import ai.vital.aspen.groovy.modelmanager.AspenModel
import ai.vital.aspen.groovy.modelmanager.ModelManager;
import ai.vital.aspen.model.DecisionTreePredictionModel;
import ai.vital.aspen.model.KMeansPredictionModel;
import ai.vital.aspen.model.NaiveBayesPredictionModel;
import ai.vital.aspen.model.RandomForestPredictionModel;
import ai.vital.domain.FlowPredictModel;
import ai.vital.prime.conf.VitalPrimeConfigurationException;
import ai.vital.prime.groovy.v2.S3Conf
import ai.vital.prime.groovy.v2.VitalPrimeGroovyScriptV2;
import ai.vital.prime.groovy.v2.VitalPrimeScriptInterfaceV2;
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.query.ResultElement
import ai.vital.vitalservice.query.ResultList;

class Aspen_ModelManager implements VitalPrimeGroovyScriptV2 {

	public final static String modelManagerKey = 'aspen-model-manager'
	
	//common prime initialization
	ModelManager initModelManager(VitalPrimeScriptInterfaceV2 scriptInterface) {
		
		ModelManager manager = scriptInterface.getRegistry().get(modelManagerKey);
		
		if(manager != null) return manager
		
		synchronized (ModelManager.class) {

			manager = scriptInterface.getRegistry().get(modelManagerKey);
			
			if(manager != null) return manager
			
			AspenGroovyConfig config = AspenGroovyConfig.get()
			
			try {
				
				S3Conf s3Conf = scriptInterface.getS3Config();
				config.setAWSAccessCredentials(s3Conf.awsAccessKeyId, s3Conf.awsSecretAccessKey)
				
			} catch(VitalPrimeConfigurationException e) {
				//ignore
			}
			
			try {
				
				List<String> xmls = scriptInterface.getHadoopConfig()
				config.setHadoopConfigXML(xmls)
				
			} catch(VitalPrimeConfigurationException e) {
				//ignore
			}
			
			config.modelType2Class.put(DecisionTreePredictionModel.spark_decision_tree_prediction(), DecisionTreePredictionModel.class.canonicalName)
			config.modelType2Class.put(KMeansPredictionModel.spark_kmeans_prediction(), KMeansPredictionModel.class.canonicalName)
			config.modelType2Class.put(NaiveBayesPredictionModel.spark_naive_bayes_prediction(), NaiveBayesPredictionModel.class.canonicalName)
			config.modelType2Class.put(RandomForestPredictionModel.spark_randomforest_prediction(), RandomForestPredictionModel.class.canonicalName)
			
			
			manager = new ModelManager()
			
			
			
			scriptInterface.getRegistry().put(modelManagerKey, manager)
			
			return manager
						
		}
		
	}

	
	@Override
	public ResultList executeScript(
			VitalPrimeScriptInterfaceV2 scriptInterface,
			Map<String, Object> parameters) {

		ResultList rl = new ResultList();
		
		try {
			
			String action = parameters.get("action");
			
			if(!action) throw new Exception("No 'action' param")
			
			if( ! ['listModels', 'loadModel', 'unloadModel'].contains(action) ) {
				throw new Exception("Unknown action: ${action}")
			}
			
			ModelManager manager = initModelManager(scriptInterface)
			
			if('listModels'.equals(action)) {

				for( AspenModel model : manager.getLoadedModels() ) {
					
					FlowPredictModel fpm = new FlowPredictModel()
					fpm.URI = model.getURI()
					fpm.modelPath = model.sourceURL
					fpm.modelType = model.type
					fpm.name = model.getName()
									
					rl.results.add(new ResultElement(fpm, 1D))
													
				}
				
				rl.setTotalResults(rl.results.size())
								
			} else if('loadModel'.equals(action)) {
			
				String modelURL = parameters.get("modelURL");
				
				if(!modelURL) throw new Exception("No 'modelURL' param");
				
				AspenModel model = manager.loadModel(modelURL)
							
				FlowPredictModel fpm = new FlowPredictModel()
				fpm.URI = model.getURI()
				fpm.modelPath = model.sourceURL
				fpm.modelType = model.type
				fpm.name = model.getName()
								
				rl.results.add(new ResultElement(fpm, 1D))
				
				rl.setStatus(VitalStatus.withOKMessage("Model loaded successfully, URI: ${model.URI}"))
					
			} else if('unloadModel'.equals(action)) {
			
				String modelName = parameters.get('modelName');
				String modelURI = parameters.get('modelURI');
				
				if(( !modelName && !modelURI) || (modelName && modelURI) ) throw new Exception("Exactly one of 'modelName' or 'modelURI' param expected")
				
				if(modelName) {
					if(!manager.unloadModelByName(modelName)) throw new Exception("Model with name ${modelName} not found")
					rl.setStatus(VitalStatus.withOKMessage("Model with name ${modelName} removed"))
				} else {
					if(!manager.unloadModelByURI(modelURI)) throw new Exception("Model with URI ${modelURI} not found")
					rl.setStatus(VitalStatus.withOKMessage("Model with URI ${modelURI} removed"))
				}
			
			} else {
				throw new Exception("Unknown action: ${action}")
			}
			
		} catch(Exception e) {
			rl.setStatus(VitalStatus.withError(e.getLocalizedMessage()))
		}
		
		return rl;
	}
			

}
