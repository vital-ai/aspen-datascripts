package ai.vital.aspen.datascripts

import com.vitalai.domain.nlp.Annotation;
import com.vitalai.domain.nlp.Document
import com.vitalai.domain.nlp.FlowPredictModel;
import com.vitalai.domain.nlp.TargetNode;
import ai.vital.vitalservice.VitalService;
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.factory.VitalServiceFactory;
import ai.vital.vitalservice.query.ResultList
import ai.vital.vitalsigns.meta.GraphContext;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.VitalApp
import ai.vital.vitalsigns.model.VitalServiceKey
import ai.vital.vitalsigns.model.property.IProperty;
import ai.vital.vitalsigns.model.property.URIProperty;

class AspenDatascriptsSampleClient {

	static Map cmd2CLI = [:]
	
	static String ADC = "aspen-datascript-client"
	
	static String CMD_LIST_MODELS = 'listmodels'
	
	static String CMD_DETECT_LANGUAGE = 'detect-language'
	
	static String CMD_LOAD_MODEL = 'loadmodel'
	
	static String CMD_UNLOAD_MODEL = 'unloadmodel'
	
	static {
		
		def initCLI = new CliBuilder(usage: "${ADC} ${CMD_LIST_MODELS} [options]")
		initCLI.with {
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			sk longOpt: 'service-key', 'service key, xxxx-xxxx-xxxx format', args: 1, required: true
		}
		cmd2CLI.put(CMD_LIST_MODELS, initCLI)
		
		
		def detectLanguageCLI = new CliBuilder(usage: "${ADC} ${CMD_DETECT_LANGUAGE} [options]")
		detectLanguageCLI.with {
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			b longOpt: 'body', 'Document.body property value', args:1, required: true
			sk longOpt: 'service-key', 'service key, xxxx-xxxx-xxxx format', args: 1, required: true
		}
		cmd2CLI.put(CMD_DETECT_LANGUAGE, detectLanguageCLI)
		
		
		def loadModelCLI = new CliBuilder(usage: "${ADC} ${CMD_LOAD_MODEL} [options]")
		loadModelCLI.with {
			u longOpt: 'model-url', 'prediction model URL', args:1, required: true
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			sk longOpt: 'service-key', 'service key, xxxx-xxxx-xxxx format', args: 1, required: true
		}
		cmd2CLI.put(CMD_LOAD_MODEL, loadModelCLI)
		
		
		def unloadModelCLI = new CliBuilder(usage: "${ADC} ${CMD_UNLOAD_MODEL} [options]")
		unloadModelCLI.with {
			n longOpt: 'model-name', 'prediction model name, mutually exclusive with model-uri', args:1, required:false
			u longOpt: 'model-uri', 'prediction model URI, mutually exclusive with model-name', args:1, required:false
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			sk longOpt: 'service-key', 'service key, xxxx-xxxx-xxxx format', args: 1, required: true
		}
		cmd2CLI.put(CMD_UNLOAD_MODEL, unloadModelCLI)
	}	
	
	def static main(args) {
	
		String cmd = args.length > 0 ? args[0] : null
		
		boolean printHelp = args.length == 0
			
		if(printHelp) {
			usage()
			return
		}
			
		String[] params = args.length > 1 ? Arrays.copyOfRange(args, 1, args.length) : new String[0]
			
		def cli = cmd2CLI.get(cmd)
			
		if(!cli) {
			System.err.println "unknown command: ${cmd}"
			usage()
			return
		}
		
		def options = cli.parse(params)
		if(!options) {
			return
		}
		
		VitalServiceKey serviceKey = new VitalServiceKey().generateURI((VitalApp) null)
		serviceKey.key = options.sk
		
		String profile = options.prof ? options.prof : null
		if(profile != null) {
			println "vitalservice profile: ${profile}"
		} else {
			println "using default vitalservice profile: ${VitalServiceFactory.DEFAULT_PROFILE}"
			profile = VitalServiceFactory.DEFAULT_PROFILE
		}
		
		def service = VitalServiceFactory.openService(serviceKey, profile)
		
		if(cmd == CMD_LIST_MODELS) {
			
			listModels(service)
			
			
		} else if(cmd == CMD_DETECT_LANGUAGE) {
		
			String body = options.b
			
			detectLanguage(service, body)
		
		} else if(cmd == CMD_LOAD_MODEL) {
		
			String modelURL = options.u
			
			loadModel(service, modelURL)	
		
		} else if(cmd == CMD_UNLOAD_MODEL) {
		
			String modelName = options.n ? options.n : null
			String modelURI = options.u ? options.u : null
			
			if((modelName && modelURI) || (!modelName && !modelURI)) {
				System.err.println "--model-name and --model-uri parameters are mutually exclusive, exactly 1 required"
				return
			}
		
			unloadModel(service, modelName, modelURI)
			
		} else {
			throw new RuntimeException("unhandled command: ${cmd}")
		}
		
	}

	static void usage() {
		
		println "usage: ${ADC} <command> [options] ..."
		
		for(def e : cmd2CLI.entrySet()) {
			e.value.usage()
		}

	}
	
	static def loadModel(VitalService service, String modelURL) {
		
		ResultList loadRL = service.callFunction('commons/scripts/Aspen_ModelManager', ['action': 'loadModel', 'modelURL': modelURL]);
		
		if(loadRL .status.status != VitalStatus.Status.ok) {
			System.err.println "Error when loading model via datascript: ${loadRL.status.message}"
			return
		}
		
		FlowPredictModel model = loadRL.first()
		
		println loadRL.status.message
		println "URI: ${model.URI}"
		println "name: ${model.name}"
		println "type: ${model.modelType}"
		println "path: ${model.modelPath}"
		
	}	
	
	
	static def unloadModel(VitalService service, String modelName, String modelURI) {
		
		ResultList unloadRL = service.callFunction('commons/scripts/Aspen_ModelManager', ['action': 'unloadModel', 'modelName': modelName, 'modelURI': modelURI ]);
		
		if(unloadRL .status.status != VitalStatus.Status.ok) {
			System.err.println "Error when unloading model via datascript: ${unloadRL.status.message}"
			return
		}

		
		println unloadRL.status.message
		
	}
	
	static def detectLanguage(VitalService service, String body) {
		
		Document doc = new Document()
		doc.URI = "urn:doc1"
//		doc.title = title
		doc.body = body
		List inputBlock = [doc]
		
		ResultList predictRL = service.callFunction("commons/scripts/Aspen_DocumentLanguage", ['inputBlock': inputBlock] )

		if(predictRL.status.status != VitalStatus.Status.ok) {
			System.err.println "Error when calling predict datascript: ${predictRL.status.message}"
			return
		}
		
		boolean found = false
				
		for(GraphObject g : predictRL) {
			
			if(g instanceof Annotation) {
				
				println "Language tag: ${g.annotationValue}"
				
				found = true
			}
			
		}
		
		if(!found) System.err.println("No language tag found")
	}
	
	
	static def listModels(VitalService service) {
		
		ResultList modelsRL = service.callFunction('commons/scripts/Aspen_ModelManager', ['action': 'listModels']);
		
		if(modelsRL.status.status != VitalStatus.Status.ok) {
			System.err.println "Error when listing models: ${modelsRL.status.message}"
			return
		}
		
		println "Models count: ${modelsRL.results.size()}"
		
		int c = 1
		for(FlowPredictModel model : modelsRL) {
			
			println ""
			
			println "Model #${c}"
			println "URI: ${model.URI}"
			println "name: ${model.name}"
			println "type: ${model.modelType}"
			println "path: ${model.modelPath}"
			
			c++
			
		}
		
	}

}
