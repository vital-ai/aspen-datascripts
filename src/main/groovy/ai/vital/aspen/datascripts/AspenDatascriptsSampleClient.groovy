package ai.vital.aspen.datascripts

import org.movielens.domain.Movie;
import org.movielens.domain.User;

import ai.vital.domain.Annotation;
import ai.vital.domain.Document
import ai.vital.domain.FlowPredictModel;
import ai.vital.domain.TargetNode;
import ai.vital.vitalservice.VitalService;
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.factory.VitalServiceFactory;
import ai.vital.vitalservice.query.ResultList
import ai.vital.vitalsigns.meta.GraphContext;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.property.IProperty;
import ai.vital.vitalsigns.model.property.URIProperty;

class AspenDatascriptsSampleClient {

	static Map cmd2CLI = [:]
	
	static String ADC = "aspen-datascript-client"
	
	static String CMD_LIST_MODELS = 'listmodels'
	
	static String CMD_PREDICT = 'predict'
	
	static String CMD_RECOMMENDATIONS = 'recommendations'
	
	static String CMD_DETECT_LANGUAGE = 'detect-language'
	
	static String CMD_LOAD_MODEL = 'loadmodel'
	
	static String CMD_UNLOAD_MODEL = 'unloadmodel'
	
	static {
		
		def initCLI = new CliBuilder(usage: "${ADC} ${CMD_LIST_MODELS} [options]")
		initCLI.with {
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
		}
		cmd2CLI.put(CMD_LIST_MODELS, initCLI)
		
		
		def predictCLI = new CliBuilder(usage: "${ADC} ${CMD_PREDICT} [options]")
		predictCLI.with {
			n longOpt: 'model-name', 'prediction model name, mutually exclusive with model-uri', args:1, required:false
			u longOpt: 'model-uri', 'prediction model URI, mutually exclusive with model-name', args:1, required:false
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			b longOpt: 'body', 'Document.body property value', args:1, required: true
			t longOpt: 'title', "Document.title property value (optional)", args: 1, required: false
		}
		cmd2CLI.put(CMD_PREDICT, predictCLI)
		
		
		def recommCLI = new CliBuilder(usage: "$ADC $CMD_RECOMMENDATIONS [options]")
		recommCLI.with {
			n longOpt: 'model-name', 'prediction model name, mutually exclusive with model-uri', args:1, required:false
			u longOpt: 'model-uri', 'prediction model URI, mutually exclusive with model-name', args:1, required:false
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			uri longOpt: 'user-uri', 'user uri', args: 1
			max  longOpt: 'max-results', 'max results count, default 10', args: 1, required: false 
		}
		cmd2CLI.put(CMD_RECOMMENDATIONS, recommCLI) 
		
		def detectLanguageCLI = new CliBuilder(usage: "${ADC} ${CMD_DETECT_LANGUAGE} [options]")
		detectLanguageCLI.with {
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			b longOpt: 'body', 'Document.body property value', args:1, required: true
		}
		cmd2CLI.put(CMD_DETECT_LANGUAGE, detectLanguageCLI)
		
		
		def loadModelCLI = new CliBuilder(usage: "${ADC} ${CMD_LOAD_MODEL} [options]")
		loadModelCLI.with {
			u longOpt: 'model-url', 'prediction model URL', args:1, required: true
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
		}
		cmd2CLI.put(CMD_LOAD_MODEL, loadModelCLI)
		
		
		def unloadModelCLI = new CliBuilder(usage: "${ADC} ${CMD_UNLOAD_MODEL} [options]")
		unloadModelCLI.with {
			n longOpt: 'model-name', 'prediction model name, mutually exclusive with model-uri', args:1, required:false
			u longOpt: 'model-uri', 'prediction model URI, mutually exclusive with model-name', args:1, required:false
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
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
		
			String title = options.t ? options.t : null
			
			predict(service, modelName, modelURI, title, options.b)
			
		} else if(cmd == CMD_RECOMMENDATIONS) {
		
			String modelName = options.n ? options.n : null
			String modelURI = options.u ? options.u : null
		
			if((modelName && modelURI) || (!modelName && !modelURI)) {
				System.err.println "--model-name and --model-uri parameters are mutually exclusive, exactly 1 required"
				return
			}
			
			String userURI = options.uri
			
			Integer max = 10
			
			if( options.max ) max = Integer.parseInt( options.max )
			if(max < 1) {
				System.err.println "-max must not be < 1"
				return
			}
			
			recommendations(service, modelName, modelURI, userURI, max)
			
			
		
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
	
	static def recommendations(VitalService service, String modelName, String modelURI, String userURI, Integer max) {
	
		GraphObject x = service.get(GraphContext.ServiceWide, URIProperty.withString(userURI)).first()
		if(x == null) {
			System.err.println("Object with URI: " + userURI + " not found")
			return
		}
		if(!(x instanceof User) ) {
			System.err.println("Object with URI " + userURI + " is not a user")
			return
		}
		
		User u = x
		def selfmovies = u.getProperty("ratedMovieURIs")
		Collection uris = null
		
		double resultsValueCount = max.doubleValue() 
		if(selfmovies != null) {
			uris = (Collection) ((IProperty)selfmovies).rawValue()
			resultsValueCount = (double)( max + uris.size())
		}
		
		if(uris == null) uris = []
		
		TargetNode resultsCount = new TargetNode()
		resultsCount.setURI("urn:resultsCount")
		resultsCount.targetScore = resultsValueCount
		List inputBlock = [x, resultsCount]
		
		ResultList predictRL = service.callFunction("commons/scripts/Aspen_Predict", ['modelName': modelName, 'modelURI': modelURI, 'inputBlock': inputBlock] )
		
		if(predictRL.status.status != VitalStatus.Status.ok) {
			System.err.println "Error when calling predict datascript: ${predictRL.status.message}"
			return
		}
		
		List<URIProperty> movies = []
		Map<String, Double> scores = [:] 
		
		for(GraphObject g : predictRL) {
			
			if(g instanceof TargetNode) {
				
				TargetNode t = g
				movies.add(URIProperty.withString( t.targetStringValue.toString()) )
				scores.put(t.targetStringValue.toString(), t.targetScore.doubleValue())
				
			}
			
		}
		
		if(movies.size() > 0) {
			
			ResultList rl = service.get(GraphContext.ServiceWide, movies)
			
			if(rl.status.status != VitalStatus.Status.ok) {
				System.err.println "Error when getting movies list: ${rl.status.message}"
				return
			}
			
			int c = 0
			
			for(URIProperty m : movies) {
				
				if( uris.contains(m.get()) ) continue
				
				c++
				
				Movie movie = rl.get(m.get())
				
				String title = movie != null ? movie.name : "-- movie not found --"
				
				double score = scores.get(m.get())
				
				println "$c: ${m.get()} $title $score ${uris.contains(m.get()) ? ('[self-rated]') : ('')}"
				
				if(c >= max.intValue()) break
				
			}
			
		} else {
		
			println "(no recommendations found)"
		
		}
		
		
		
		
		
				
			
	}
	
	
	static def predict(VitalService service, String modelName, String modelURI, String title, String body) {
		
		Document doc = new Document()
		doc.URI = "urn:doc1"
		doc.title = title
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
