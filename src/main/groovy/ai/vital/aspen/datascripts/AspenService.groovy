package ai.vital.aspen.datascripts

import java.util.Map;

import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.factory.VitalServiceFactory;
import ai.vital.vitalservice.query.ResultList


class AspenService {

	static Map<String, CliBuilder> cmd2CLI = new LinkedHashMap<String, CliBuilder>()

	static String AS = "aspenservice"

	static String CMD_LIST_RDDS= 'listrdds'
	static String CMD_REMOVE_RDD= 'removerdd'

	static String CMD_LIST_APPS = 'listapps'

	static String CMD_LIST_CONTEXTS = 'listcontexts'
	static String CMD_POST_CONTEXT = 'postcontext'
	static String CMD_DELETE_CONTEXT = 'deletecontext'


	static String CMD_LIST_JOBS = 'listjobs'
	static String CMD_GET_JOB = 'getjob'
	static String CMD_DELETE_JOB = 'deletejob'
	static String CMD_POST_JOB = 'postjob'


	static {

		def listRddsCLI = new CliBuilder(usage: "$AS $CMD_LIST_RDDS [options]")
		listRddsCLI.with {
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			app longOpt: 'appName', 'app name', args: 1, required: true
			ctx longOpt: 'context', 'spark context', args: 1, required: true
		}
		cmd2CLI.put(CMD_LIST_RDDS, listRddsCLI)


		def removeRddCLI = new CliBuilder(usage: "$AS $CMD_REMOVE_RDD [options]")
		removeRddCLI.with {
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			app longOpt: 'appName', 'app name', args: 1, required: true
			ctx longOpt: 'context', 'spark context', args: 1, required: true
			name longOpt: 'rddName', 'RDD name', args: 1, required: true
		}
		cmd2CLI.put(CMD_REMOVE_RDD, removeRddCLI)


		def listAppsCLI = new CliBuilder(usage: "$AS $CMD_LIST_APPS [options]")
		listAppsCLI.with {
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
		}
		cmd2CLI.put(CMD_LIST_APPS, listAppsCLI)


		def listContextsCLI = new CliBuilder(usage: "$AS $CMD_LIST_CONTEXTS [options]")
		listContextsCLI.with {
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
		}
		cmd2CLI.put(CMD_LIST_CONTEXTS, listContextsCLI)

		def postContextCLI = new CliBuilder(usage: "$AS $CMD_POST_CONTEXT [options]")
		postContextCLI.with {
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			ctx longOpt: 'context', 'spark context', args: 1, required: true
			par longOpt: 'contextParamsURLEncoded', 'URL-encoded context params list', args: 1, required: false
		}
		cmd2CLI.put(CMD_POST_CONTEXT, postContextCLI)

		def deleteContextCLI = new CliBuilder(usage: "$AS $CMD_DELETE_CONTEXT [options]")
		deleteContextCLI.with {
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			ctx longOpt: 'context', 'spark context', args: 1, required: true
		}
		cmd2CLI.put(CMD_DELETE_CONTEXT, deleteContextCLI)


		def listJobsCLI = new CliBuilder(usage: "$AS $CMD_LIST_JOBS [options]")
		listJobsCLI.with {
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
		}
		cmd2CLI.put(CMD_LIST_JOBS, listJobsCLI)

		def getJobCLI = new CliBuilder(usage: "$AS $CMD_GET_JOB [options]")
		getJobCLI.with {
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			jid longOpt: 'jobId', 'job ID', args: 1, required: true
		}
		cmd2CLI.put(CMD_GET_JOB, getJobCLI)
		
		def deleteJobCLI = new CliBuilder(usage: "$AS $CMD_DELETE_JOB [options]")
		deleteJobCLI.with {
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			jid longOpt: 'jobId', 'job ID', args: 1, required: true
		}
		cmd2CLI.put(CMD_DELETE_JOB, deleteJobCLI)
		
		def postJobCLI = new CliBuilder(usage: "$AS $CMD_POST_JOB [options]")
		postJobCLI.with {
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			app longOpt: 'appName', 'app name', args: 1, required: true
			cp longOpt: 'classPath', 'job class', args: 1, required: true
			ctx longOpt: 'context', 'spark context', args: 1, required: false
			sync longOpt: 'sync', 'run synchronously', args: 0, required: false
			ps longOpt: 'paramsString', 'hocon job params string', args: 1, required: true
		}
		cmd2CLI.put(CMD_POST_JOB, postJobCLI)
		
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
		
		println "command: $cmd"
		
		
		String profile = options.prof ? options.prof : null
		if(profile != null) {
			println "Setting vitalservice profile to: ${profile}"
			VitalServiceFactory.setServiceProfile(profile)
		} else {
			println "using default vitalservice profile: ${VitalServiceFactory.getServiceProfile()}"
		}
		
		def service = VitalServiceFactory.getVitalService()
		
		Map fParams = ['action': cmd]
		
		
		def innerOpts = options.getInner()
		
		for(def opt : innerOpts.getOptions()) {
			
			def optName = opt.getLongOpt()
        
			if(optName == 'profile') continue
			
			def optValue = null
			
			if(opt.hasArg()) {
          
				optValue = innerOpts.getOptionValue(optName)
          
			} else {

				if( innerOpts.hasOption(optName) ) {
					optValue = true 
				} else {
					optValue = false 
				}
          
			}
			
			fParams.put(optName, optValue)
			
		}
		
		println "datascript params: $fParams"
		
		ResultList jobserverRL = service.callFunction('commons/scripts/Aspen_JobServer', fParams);
		
		if(jobserverRL .status.status != VitalStatus.Status.ok) {
			System.err.println "Error when executing jobserver datascript: ${jobserverRL.status.message}"
			return
		}
		
		println "datascript status - OK"
		
		println jobserverRL.getStatus().message
		
	}
	
	static void usage() {
		
		println "usage: ${AS} <command> [options] ..."
		
		for(def e : cmd2CLI.entrySet()) {
			e.value.usage()
		}

	}
}

