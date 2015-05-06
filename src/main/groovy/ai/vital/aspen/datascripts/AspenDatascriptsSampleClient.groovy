package ai.vital.aspen.datascripts

import ai.vital.domain.Document
import ai.vital.domain.FlowPredictModel;
import ai.vital.domain.TargetNode;
import ai.vital.vitalservice.VitalService;
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.factory.VitalServiceFactory;
import ai.vital.vitalservice.query.ResultList
import ai.vital.vitalsigns.model.GraphObject;

class AspenDatascriptsSampleClient {

	static Map cmd2CLI = [:]
	
	static String ADC = "aspen-datascript-client"
	
	static String CMD_LIST_MODELS = 'listmodels'
	
	static String CMD_PREDICT = 'predict'
	
//	static String CMD_LOAD_MODEL = 'loadmodel'
	
//	static String CMD_UNLOAD_MODEL = 'unloadmodel'
	
	static {
		
		def initCLI = new CliBuilder(usage: "${ADC} ${CMD_LIST_MODELS} [options]")
		initCLI.with {
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
		}
		cmd2CLI.put(CMD_LIST_MODELS, initCLI)
		
		
		def listAppsCLI = new CliBuilder(usage: "${ADC} ${CMD_PREDICT} [options]")
		listAppsCLI.with {
			n longOpt: 'model-name', 'prediction model name, mutually exclusive with model-uri', args:1, required:false
			u longOpt: 'model-uri', 'prediction model URI, mutually exclusive with model-name', args:1, required:false
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			b longOpt: 'body', 'Document.body property', args:1, required: true
		}
		cmd2CLI.put(CMD_PREDICT, listAppsCLI)
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
		
		
		String profile = options.prof ? options.prof : null
		if(profile != null) {
			println "Setting vitalservice profile to: ${profile}"
			VitalServiceFactory.setServiceProfile(profile)
		} else {
			println "using default vitalservice profile: ${VitalServiceFactory.getServiceProfile()}"
		}
		
		def service = VitalServiceFactory.getVitalService()
		
		if(cmd == CMD_LIST_MODELS) {
			
			listModels(service)
			
			
		} else if(cmd == CMD_PREDICT) {
		
			String modelName = options.n ? options.n : null
			String modelURI = options.u ? options.u : null
			
			if((modelName && modelURI) || (!modelName && !modelURI)) {
				System.err.println "--model-name and --model-uri parameters are mutually exclusive, exactly 1 required"
				return
			}
		
			predict(service, modelName, modelURI, options.b)
		
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
	
	static def predict(VitalService service, String modelName, String modelURI, String body) {
		
		Document doc = new Document()
		doc.URI = "urn:doc1"
		doc.body = body
		List inputBlock = [doc]
		
		ResultList predictRL = service.callFunction("commons/scripts/Aspen_Predict", ['modelName': modelName, 'modelURI': modelURI, 'inputBlock': inputBlock] )

		if(predictRL.status.status != VitalStatus.Status.ok) {
			System.err.println "Error when calling predict datascript: ${predictRL.status.message}"
			return
		}
		
		
		println "predictions:"
		
		int c = 0
		
		for(GraphObject g : predictRL) {
			
			if(g instanceof TargetNode) {
				
				println "${++c}:  ${g.targetStringValue} , score: ${g.targetScore}"
				
			}
			
		}
		
		if(c == 0) println "(none)"

		
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
