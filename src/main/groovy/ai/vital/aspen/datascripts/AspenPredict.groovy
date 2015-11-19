package ai.vital.aspen.datascripts

import ai.vital.query.GRAPH;
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.factory.VitalServiceFactory;
import ai.vital.vitalservice.query.ResultList
import ai.vital.vitalsigns.VitalSigns;
import ai.vital.vitalsigns.block.BlockCompactStringSerializer;
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.BlockIterator;
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.VitalBlock;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.VitalApp
import ai.vital.vitalsigns.model.VitalServiceKey
import ai.vital.vitalsigns.model.property.URIProperty;


class AspenPredict {

	def static AP = 'aspen-predict'
	
	def static error(String msg) {
		System.err.println(msg)
		System.exit(1)
	}
	
	def static main(args) {
		
		boolean printHelp = args.length == 0
			
		String[] params = args
			
		def cli = new CliBuilder(usage: "${AP} [options]")
		cli.with {
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			sk longOpt: 'service-key', 'service key, xxxx-xxxx-xxxx format', args: 1, required: true
			i longOpt: 'input', 'input block file', args: 1, required: true
			o longOpt: 'output', 'output block file', args: 1, required: true
			ow longOpt: 'overwrite', 'overwrite output file', args: 0, required: false
			n longOpt: 'model-name', 'model name, mutually exclusive with --model-uri', args: 1, required: false
			u longOpt: 'model-uri', 'model uRI, mutually exclusive with --model-name', args: 1, required: false
			t longOpt: 'type', 'process single object of type at a time', args: 1, required: false
			sub longOpt: 'include-subclasses', 'include subclasses, only applicable with --type option', args: 0, required: false
		}
		
		if(printHelp) {
			cli.usage()
			return
		}
			
		def options = cli.parse(params)
		if(!options) {
			return
		}
		
		String modelName = options.n ? options.n : null
		String modelURI = options.u ? options.u : null
		
		File inputFile = new File(options.i)
		File outputFile = new File(options.o)
		
		boolean overwrite = options.ow ? true : false
		
		println "Input  file: ${inputFile.absolutePath}"
		println "Output file: ${outputFile.absolutePath}"
		println "Overwrite ? ${overwrite}"
		
		if(!inputFile.exists()) error("File not found: ${inputFile.absolutePath}")
		
		String type = options.t ? options.t : null
		boolean includeSubclasses = options.sub ? true : false
		
		Class<? extends GraphObject> clazz = null
		
		if(type) {
			
			println "Single typed object mode, type: ${type}, include subclasses: ${includeSubclasses}"
			
			if(type.contains(':')) {
				println "Getting class by URI"
				clazz = VitalSigns.get().getClass(URIProperty.withString(type))
				if(clazz == null) {
					error("Class with URI not found: ${type}")
					return
				}
			} else if(type.contains('.')) {
				println "Getting class by canonical name"
				clazz = VitalSigns.get().getClassesRegistry().getGraphObjectClass(type)
				if(clazz == null) {
					error("Class with canonical name not found: ${type}")
					return
				}
			} else {
				println "Getting class by short name"
				clazz = VitalSigns.get().getClass(type)
				if(clazz == null) {
					error("Class with short name not found or ambiguous: ${type}")
					return
				}
			}
			
			
		} else {
		
			if(includeSubclasses) println "WARN: --include-subclasses only applicable wth --type"
			
		}
		
		if(outputFile.exists()) {
			if(!overwrite) error("Output file already exists: ${outputFile.absolutePath}")
			println "File will be overwritten: ${outputFile.absolutePath}"
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

		int i = 0
		
		int processed = 0

		BlockCompactStringSerializer writer = new BlockCompactStringSerializer(outputFile)
				
		for( BlockIterator iterator = BlockCompactStringSerializer.getBlocksIterator(inputFile); iterator.hasNext(); ) {

			i++
			
			VitalBlock block = iterator.next()			
			
			List l = block.toList()
			
			if(l.size() < 1) {
				println "Skipping an empty block"
				continue
			}
			
			List rl = []
			
			if(clazz != null) {
				
				int c = 0
				
				for(GraphObject g : l) {
					
					if( (includeSubclasses && clazz.isInstance(g)) || clazz.equals(g.getClass()) ) {
						
						c++
						
						ResultList predictRL = service.callFunction("commons/scripts/Aspen_Predict", [
							inputBlock: [g],
							modelName: modelName,
							modelURI: modelURI
						])
						
						processed++
						
						if(predictRL.status.status != VitalStatus.Status.ok) {
							
							println "Block ${i} object ${c} error: ${predictRL.status.message}"
							
						} else {
						
							for(GraphObject x : predictRL) {
								
								if(x.getURI().equals(g.getURI())) {
									//refresh object only
									g = x
								}
								
							}
						}
						
					}						
					
					rl.add(g)
					
				}
				
			} else {
			
				processed++
				
				ResultList predictRL = service.callFunction("commons/scripts/Aspen_Predict", [
					inputBlock: l,
					modelName: modelName, 
					modelURI: modelURI
				])
				
				if(predictRL.status.status != VitalStatus.Status.ok) {
					println "Block ${i} error: ${predictRL.status.message}"
					continue
				}
	

				for(GraphObject g : predictRL) {
					rl.add(g)
				}			
				
			}
			
			if(rl.size() == 0) {
				println "Block ${i}: empty output output"
				continue
			}
			
			writer.startBlock()
			
			for(GraphObject g : rl) {
				writer.writeGraphObject(g)
			}
			
			writer.endBlock()
		}
		
		writer.close()
		
		println "DONE, blocks iterated: ${i}, predict script called ${processed} time${processed != 1 ? 's' : ''}"
			
	}
	
}
