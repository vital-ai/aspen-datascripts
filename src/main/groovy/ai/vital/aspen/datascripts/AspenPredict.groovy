package ai.vital.aspen.datascripts

import ai.vital.query.GRAPH;
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.factory.VitalServiceFactory;
import ai.vital.vitalservice.query.ResultList
import ai.vital.vitalsigns.block.BlockCompactStringSerializer;
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.BlockIterator;
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.VitalBlock;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.VitalApp
import ai.vital.vitalsigns.model.VitalServiceKey


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

		BlockCompactStringSerializer writer = new BlockCompactStringSerializer(outputFile)
				
		for( BlockIterator iterator = BlockCompactStringSerializer.getBlocksIterator(inputFile); iterator.hasNext(); ) {

			i++
			
			VitalBlock block = iterator.next()			
			
			List l = block.toList()
			
			if(l.size() < 1) {
				println "Skipping an empty block"
				continue
			}
			
			ResultList predictRL = service.callFunction("commons/scripts/Aspen_Predict", [
				inputBlock: l,
				modelName: modelName, 
				modelURI: modelURI
			])
			
			if(predictRL.status.status != VitalStatus.Status.ok) {
				println "Block ${i} error: ${predictRL.status.message}"
				continue
			}

			List rl = []
			for(GraphObject g : predictRL) {
				rl.add(g)
			}			
			
			if(rl.size() == 0) {
				println "Block ${i}: empty output"
				continue
			}
			
			writer.startBlock()
			
			for(GraphObject g : rl) {
				writer.writeGraphObject(g)	
			}
			
			writer.endBlock()
			
		}
		
		writer.close()
		
		println "DONE, blocks iterated: ${i}"
			
	}
	
}
